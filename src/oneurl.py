# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
import json
import hashlib
import pymongo
import argparse
import pylru
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from urllib.parse import urljoin, urlparse, urlunparse
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

    def is_blacklisted(self, domain):
        return False # overriden in subclass

    def is_recently_crawled(self, url):
        return False # overridden in subclass

    def next_request(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        valid = False
        while not valid:
           message = next(self.consumer)
           url = self.process_kafka_message(message)
           if url is None:
               return None
           up = urlparse(url)
           # we implement the blacklist on the dequeue side so that we can blacklist as the crawl proceeds. Not enough to just blacklist on the enqueue side
           if not (self.is_blacklisted(up.hostname.lower()) or self.is_recently_crawled(url)):
               valid = True  # no more iterations
           else:
               self.logger.info("Skipping undesirable URL: {}".format(url))

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
    custom_settings = {
        'ONEURL_MONGO_HOST': 'pi1', 
        'ONEURL_MONGO_PORT': 27017,
        'ONEURL_MAXMIND_DB': '/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb',
        'ONEURL_MONGO_DB': 'au_js',
        'ONEURL_KAFKA_BOOTSTRAP': 'kafka1',
        'ONEURL_KAFKA_CONSUMER_GROUP': 'scrapy-thug2',
        'ONEURL_KAFKA_URL_TOPIC': '4thug.gen3'
    }

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)
       settings = self.custom_settings
       topic = settings.get('ONEURL_KAFKA_URL_TOPIC')
       bs = settings.get('ONEURL_KAFKA_BOOTSTRAP')
       grp_id = settings.get('ONEURL_KAFKA_CONSUMER_GROUP')
       self.consumer = KafkaConsumer(topic, bootstrap_servers=bs, group_id=grp_id, auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout 
       self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=settings.get('ONEURL_KAFKA_BOOTSTRAP'))
       self.au_locator = AustraliaGeoLocator(db_location=settings.get('ONEURL_MAXMIND_DB'))
       self.mongo = pymongo.MongoClient(settings.get('ONEURL_MONGO_HOST', settings.get('ONEURL_MONGO_PORT')))
       self.db = self.mongo[settings.get('ONEURL_MONGO_DB')]
       self.cache = pylru.lrucache(10 * 1024) # dont insert into kafka url topic if url seen recently
       self.recent_cache = pylru.lrucache(10 * 1024) # size just a guess (roughly a few hours of spidering)
       self.ensure_db_constraints()

    def ensure_db_constraints(self):
       # ensure url collection has a unique key index on url field
       url_collection = self.db['urls']
       url_collection.create_index("url", unique=True)

       # DONE
       pass

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(OneurlSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        spider.logger.info("Closed spider") 

    def find_url(self, url):
        return self.db.url.find_one({ 'url': url })

    def is_recently_crawled(self, url):
        return url in self.recent_cache

    def queue_followup_links(self, producer, url_list):
        """
           URLs which are on an australian IP are sent to kafka
        """
        urls = frozenset(url_list) # de-dupe
        self.logger.info("Considering {} url's for followup.".format(len(urls)))
        sent = 0
        last_month = datetime.utcnow() - timedelta(weeks=4)
        topic = self.custom_settings.get('ONEURL_KAFKA_URL_TOPIC')
        self.logger.info("Looking for URLs not visited since {}".format(str(last_month)))
        for u in urls:
           up = urlparse(u)
           if (up.path.count('/') > 3) or len(up.query) > 10:
               producer.send('rejected-urls', { 'url': u, 'reason': 'too long path/query' })
           elif len(u) > 200:
               producer.send('rejected-urls', { 'url': u, 'reason': 'url too long (over 200)' })
           elif self.au_locator.is_au(up.hostname):
               up = up._replace(fragment='') # remove all fragments from spidering
               url = urlunparse(up)
               if not url in self.cache:
                   result = self.find_url(url)
                   if (result is None or result.get(u'last_visited') < last_month):
                       producer.send(topic, { 'url': url })  # send to the 4thug queue
                       self.cache[url] = 1  # add to cache
                       sent = sent + 1
           else:
               producer.send('rejected-urls', { 'url': u, 'reason': 'not an AU IP address' })
        self.logger.info("Sent {} url's to kafka".format(sent))

    def save_url(self, url):
         last_visited = datetime.utcnow()
         last_month = last_visited - timedelta(weeks=4)
         # ensure last_visited is kept accurate so that we can ignore url's which we've recently seen
         result = self.db.urls.update_one({ 'url': url }, { "$set": { 'url': url, 'last_visited': last_visited } }, upsert=True)

    def save_snippet(self, origin_url_id, script, sha256, md5):
         existing = self.db.snippets.find_one({ 'sha256': sha256, 'md5': md5, 'size_bytes': len(script) })
         if existing:
             self.db.snippet_url.insert_one({ 'url_id': origin_url_id, 'snippet': ObjectId(existing.get('_id')) })
         else:
             id = self.db.snippets.insert_one({'sha256': sha256, 'md5': md5, 'size_bytes': len(script), 'code': script }).inserted_id
             self.db.snippet_url.insert_one({ 'url_id': origin_url_id, 'snippet': id }) 

    def save_script(self, url, script, inline_script=False, content_type=None):
        # compute hashes to search for
        sha256 = hashlib.sha256(script).hexdigest()
        md5 = hashlib.md5(script).hexdigest()
        # check to see if in mongo already
        self.save_url(url)
        url_record = self.db.urls.find_one( { 'url': url } ) # must not fail given it has just been saved...
        url_id = url_record.get(u'_id')
        #self.logger.info("Got oid {} for {}".format(url_id, url))
        if inline_script:
             self.save_snippet(url_id, script, sha256, md5)
        else:
             s = self.db.scripts.find_one({ 'sha256': sha256, 'md5': md5, 'size_bytes': len(script) })
             if s:
                 self.db.script_url.insert({ 'url_id': url_id, 'script': s.get(u'_id') })
             else:
                 id = self.db.scripts.insert_one({ 'sha256': sha256, 'md5': md5, 'size_bytes': len(script), 'code': script }).inserted_id
                 self.db.script_url.insert( { 'url_id': url_id, 'script': id })

        # finally update the kafka visited queue
        self.producer.send('visited', { 'url': url, 'size_bytes': len(script), 'inline': inline_script,
                                           'content-type': content_type, 'when': str(datetime.utcnow()), 
					   'sha256': sha256, 'md5': md5 })

    def save_inline_script(self, url, inline_script, content_type=None):
        return self.save_script(url, inline_script.encode(), inline_script=True, content_type=content_type)

    def is_blacklisted(self, domain):
        return self.db.blacklisted_domains.find_one({ 'domain': domain }) is not None

    def parse(self, response):
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()
        url = response.url
        # dont visit the response url again for a long time
        self.cache[url] = 1        # no repeats from kafka
        if url in self.recent_cache:
             return []
        self.recent_cache[url] = 1 # no repeats from crawler

        self.logger.info("Processing page {} {}".format(content_type, response.url))
        if 'html' in content_type:
           src_urls = response.xpath('//script/@src').extract()
           hrefs = response.xpath('//a/@href').extract()
           # TODO FIXME... extract inline script fragments...

           # ensure relative script src's are absolute... for the spider to follow now
           abs_src_urls = [urljoin(url, src) for src in src_urls]
           abs_hrefs = [urljoin(url, href) for href in hrefs]
           # but we also want suitable follow-up links for pushing into the url topic
           self.queue_followup_links(self.producer, abs_hrefs)
       
           # extract inline javascript and store in mongo...
           inline_scripts = response.xpath('//script/text()').getall()
           for script in inline_scripts:
                self.save_inline_script(url, script, content_type=content_type)
           self.logger.info("Saved {} inline scripts from {}".format(len(inline_scripts), url))
 
           # spider over the JS content... 
           return [self.make_requests_from_url(u) for u in abs_src_urls if not u in self.recent_cache]
           # FALLTHRU
        elif 'javascript' in content_type:
           self.save_script(url, response.body, inline_script=False, content_type=content_type)
           self.logger.info("Saved javascript {}".format(url))
           return []
        else:
           return []
