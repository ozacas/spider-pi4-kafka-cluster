# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
import json
import hashlib
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from urllib.parse import urljoin, urlparse
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DontCloseSpider
from utils.AustraliaGeoLocator import AustraliaGeoLocator

class KafkaSpiderMixin(object):

    """
    Mixin class to implement reading urls from a kafka queue.
    :type kafka_topic: str
    """

    def process_kafka_message(self, message):
        """"
        Tell this spider how to extract urls from a kafka message
        :param message: A Kafka message object
        :type message: kafka.common.OffsetAndMessage
        :rtype: str or None
        """
        if not message:
            return None

        return message.value.get('url')

    def next_request(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        message = next(self.consumer)
        url = self.process_kafka_message(message)
        if url is None:
            return None
        self.logger.info("Obtained kafka url: {}".format(url))
        return self.make_requests_from_url(url)

    def schedule_next_request(self):
        """Schedules a request if available"""
        req = self.next_request()
        if req:
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """Avoids waiting for the spider to  idle before scheduling the next request"""
        self.schedule_next_request()


class OneurlSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'oneurl'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)
       self.consumer = KafkaConsumer('4thug', bootstrap_servers='kafka1', auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
       self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
       self.au_locator = AustraliaGeoLocator(db_location='/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(OneurlSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        spider.logger.info("Closed spider") 

    def queue_followup_links(self, producer, topic, url_list):
        """
           URLs which are on an australian IP are sent to kafka
        """
        urls = frozenset(url_list) # de-dupe
        self.logger.info("Considering {} url's for followup.".format(len(urls)))
        sent = 0
        for u in urls:
           up = urlparse(u)
           if (up.path.count('/') > 3) or len(up.query) > 10:
               producer.send('rejected-urls', { 'url': u, 'reason': 'too long path/query' })
           elif len(u) > 200:
               producer.send('rejected-urls', { 'url': u, 'reason': 'url too long (over 200)' })
           elif self.au_locator.is_au(up.hostname):
               producer.send(topic, { 'url': u })  # send to the 4thug queue
               sent = sent + 1
           else:
               producer.send('rejected-urls', { 'url': u, 'reason': 'not an AU IP address' })
        self.logger.info("Sent {} url's to kafka".format(sent))

    def parse(self, response):
        content_type = response.headers.get('Content-Type', b'').decode('utf-8')
        ret = []
        if content_type.startswith('text/html'):
           src_urls = response.xpath('//script/@src').extract()
           hrefs = response.xpath('//a/@href').extract()
           # TODO FIXME... extract inline script fragments...

           # ensure relative script src's are absolute... for the spider to follow now
           abs_src_urls = [urljoin(response.url, src) for src in src_urls]
           abs_hrefs = [urljoin(response.url, href) for href in hrefs]
           # but we also want suitable follow-up links for pushing into the 4thug topic
           self.queue_followup_links(self.producer, '4thug', abs_hrefs)
        
           # spider over the JS content... 
           ret = [self.make_requests_from_url(u) for u in abs_src_urls]
           # FALLTHRU
        
        self.producer.send('visited', { 'url': response.url, 'content_size_bytes': len(response.body), 
                                           'content-type': content_type, 'when': str(datetime.utcnow()), 
					   'sha256': hashlib.sha256(response.body).hexdigest(), 'md5': hashlib.md5(response.body).hexdigest() })
        return ret
