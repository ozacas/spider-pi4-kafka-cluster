# -*- coding: utf-8 -*-
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
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
from utils.fileitem import FileItem
from utils.AustraliaGeoLocator import AustraliaGeoLocator
from utils.url import as_priority

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

    def is_recently_crawled(self, url, up):
        return False # overridden in subclass

    def is_suitable(self, url, kafka_message=None, check_priority=True):
        up = urlparse(url)
        if self.is_recently_crawled(url, up):
           return False
        if up.hostname and self.is_blacklisted(up.hostname.lower()):
           return False
        # check priority when crawling as we dont need everything crawled...
        if check_priority and as_priority(url, up) > 4:
           return False
        return True

    def errback(self, failure):
        url = failure.request.url
        up = urlparse(url)
        self.penalise(up.hostname, penalty=2) 
        self.logger.info("Penalise {} due to download failure".format(up.hostname))

    def kafka_next(self):
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
           # we implement the blacklist on the dequeue side so that we can blacklist as the crawl proceeds. Not enough to just blacklist on the enqueue side
           if self.is_suitable(url, kafka_message=message): # pass message so that is_suitable() can look at content_type or whatever from kafka topic chosen by spider
               valid = True  # no more iterations
           else:
               self.logger.info("Skipping undesirable URL: {}".format(url))

        self.logger.info("Obtained kafka url: {}".format(url))
        return url

    def schedule_next_request(self, batch_size=200):
        """Schedules a request if available"""
        found = 0
        batch = set()
        counts_by_host = { }
        while found < batch_size:
            url = self.kafka_next()
            if url: 
                if not url in batch: # else ignore it, since it is a dupe from kafka
                    up = urlparse(url)
                    if not up.hostname in counts_by_host:
                        counts_by_host[up.hostname] = 1
                    else:
                        counts_by_host[up.hostname] += 1
                        if counts_by_host[up.hostname] <= 10: 
                            req = scrapy.Request(url, callback=self.parse, errback=self.errback, dont_filter=True)
                            self.crawler.engine.crawl(req, spider=self)
                            batch.add(url)
                            found += 1
                        else: 
                            self.logger.info("Ignoring due to non-random ingest for {}".format(url))
                            # TODO FIXME... push back onto the queue? nah... we have no shortage of urls!
            else: # no request?
                break
        self.logger.info("Got batch of {} URLs to crawl".format(found))

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def spider_closed(self, spider):
        spider.logger.info("Closed spider") 

    def item_scraped(self, *args, **kwargs):
        pass

class KafkaSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'kafkaspider'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)
    custom_settings = {
        'ONEURL_MONGO_HOST': 'pi1', 
        'ONEURL_MONGO_PORT': 27017,
        'ONEURL_MAXMIND_DB': '/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb',
        'ONEURL_MONGO_DB': 'au_js',
        'ONEURL_KAFKA_BOOTSTRAP': 'kafka1',
        'ONEURL_KAFKA_CONSUMER_GROUP': 'scrapy-thug2',
        'ONEURL_KAFKA_URL_TOPIC': 'thug.gen5',
        'LOG_LEVEL': 'INFO',
        'SAVE_SNIPPETS': False,        # save inline javascript in html pages to mongo?
        'LRU_MAX_PAGES_PER_SITE': 20   # only 20 pages per recent_sites cache entry ie. 20 pages per at least 500 sites spidered
    }

    def recent_site_eviction(self, key, value):
       self.logger.info("Evicting site cache entry: {} = {} (cache size {})".format(key, value, len(self.recent_sites)))

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
       self.recent_cache = pylru.lrucache(10 * 1024) # size just a guess (roughly a few hours of spidering, non-JS script URLs only)
       self.recent_sites = pylru.lrucache(500, self.recent_site_eviction) # last 100 sites (value is page count fetched since cache entry created for host)
       self.js_cache = pylru.lrucache(500) # dont re-fetch JS which we've recently seen
       self.last_blacklist_query = datetime.utcnow() - timedelta(minutes=20) # force is_blacklisted() to query at startup

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KafkaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def penalise(self, host, penalty=1):
        if not host in self.recent_sites:
             self.recent_sites[host] = 0
        self.recent_sites[host] += penalty
        return self.recent_sites[host]

    def is_recently_crawled(self, url, up):
        # recently fetched the URL?
        if url in self.recent_cache:
             return True
        # LRU bar: if one of the last 100 sites and we've fetched 100 pages, we say no to future urls from the same host until it is evicted from the LRU cache
        host = up.hostname
        self.penalise(host)
        if self.recent_sites[host] > self.custom_settings.get('LRU_MAX_PAGES_PER_SITE'):
             return True # only eviction from the LRU cache will permit host again
        return False

    def followup_pages(self, producer, url_list, url_category, max=100):
        """
           URLs which are on an australian IP are sent to kafka
        """
        urls = list(frozenset(url_list)) # de-dupe
        self.logger.debug("Considering {} url's for followup.".format(len(urls)))
        sent = 0
        rejected = 0
        not_au = 0
        # the more pages are in the LRU cache for the site
        topic = self.custom_settings.get('ONEURL_KAFKA_URL_TOPIC')
        self.logger.info("{} URLs to followup: given {}, max {}".format(url_category, len(urls), max))
        for u in urls:
           up = urlparse(u)
           priority = as_priority(u, up)
           if priority > 5:
               producer.send('rejected-urls', { 'url': u, 'reason': 'low priority' })
               rejected = rejected + 1
           elif self.au_locator.is_au(up.hostname):
               up = up._replace(fragment='')   # remove all fragments from spidering
               url = urlunparse(up)            # search for fragment-free URL
               producer.send(topic, { 'url': url }, key=up.hostname.encode('utf-8'))  # send to the pending queue but send one host to exactly one partition only via key
               if sent > max: 
                   break
               sent = sent + 1
           else:
               producer.send('rejected-urls', { 'url': u, 'reason': 'not an AU IP address' })
               not_au = not_au + 1
        self.logger.info("Sent {} {} URLs to kafka (rejected {}, not au {})".format(sent, url_category, rejected, not_au))
        return sent

    def is_blacklisted(self, domain):
        # only run the query every 5 mins to avoid excessive db queries...
        now = datetime.utcnow()
        five_mins_ago = now - timedelta(minutes=5)
        if self.last_blacklist_query < five_mins_ago:
             self.blacklisted_domains = self.db.blacklisted_domains.distinct('domain')
             self.last_blacklist_query = now

        return domain in self.blacklisted_domains

    def parse(self, response):
        status = response.status
        url = response.url
        if status < 200 or status >=400:
           self.recent_cache[url] = 1
           up = urlparse(url)
           self.logger.info("Penalising {} due to http status {}".format(up.hostname, status))
           self.penalise(up.hostname, penalty=2) # penalty for slowing down the crawl is quite severe, but wont impact current batch already being crawled
           return []

        content_type = response.headers.get('Content-Type', b'').decode().lower()
        ret = []
        self.logger.info("Processing page {} {}".format(content_type, url))
        if 'html' in content_type:
           self.recent_cache[url] = 1     # no repeats from crawler, RACE-CONDITION: multiple url's maybe fetched in parallel with same url...
           hrefs = response.xpath('//a/@href').extract()

           # but we also want suitable follow-up links for pushing into the url topic 
           up = urlparse(url)
           # NB: by not considering every link on a page we reduce the maxmind cost and other spider slowdowns at limited loss of data 
           n_seen = self.penalise(up.hostname, penalty=0)
           if n_seen > 20:
               n_seen = 20
           max    = 100 - 4 * n_seen 
           abs_hrefs = [urljoin(url, href) for href in hrefs]
           external_hrefs = [] # prioritise external links unless max is large enough to cater to both categories on the page (ie. broad crawl rather than deep)
           internal_hrefs = []
           for u in abs_hrefs:
                if not up.hostname in u:
                     external_hrefs.append(u)
                else:
                     internal_hrefs.append(u)
           n = self.followup_pages(self.producer, external_hrefs, 'External', max=max)
           left = max - n
           if left < 0:
               left = 0
           self.followup_pages(self.producer, internal_hrefs, 'Internal', max=left)
       
           # spider over the JS content... 
           src_urls = response.xpath('//script/@src').extract()
           # ensure relative script src's are absolute... for the spider to follow now
           abs_src_urls = [urljoin(url, src) for src in src_urls]
           cnt = 0
           for u in abs_src_urls:
               if self.is_suitable(u, check_priority=False) and not u in self.js_cache:
                   item = FileItem(origin=url, file_urls=[u])
                   ret.append(item) # will trigger FilesPipeline to fetch it with priority and persist to local storage
                   self.js_cache[u] = 1 # dont fetch this JS again for at least 500 JS url's....
                   cnt += 1

           self.logger.info("Following {} suitable JS URLs (total {})".format(cnt, len(abs_src_urls)))
           # FALLTHRU
        else:
           self.logger.info("Received undesired content type: {} for {}".format(content_type, url))
           up = urlparse(url)
           self.penalise(up.hostname, penalty=2) # dont want to slow down the spider on sites with undesirable site design
          

        return ret  # url's come only from kafka, not the parse() invocation

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(KafkaSpider)
    process.start() # the script will block here until the crawling is finished
