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

    def is_suitable(self, url, check_priority=True):
        up = urlparse(url)
        if self.is_recently_crawled(url, up):
           return False
        if self.is_blacklisted(up.netloc.lower()):
           return False
        # check priority when crawling as we dont need everything crawled...
        if check_priority and as_priority(url, up) > 4:
           return False
        return True

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
           # we implement the blacklist on the dequeue side so that we can blacklist as the crawl proceeds. Not enough to just blacklist on the enqueue side
           if self.is_suitable(url):
               valid = True  # no more iterations
           else:
               self.logger.info("Skipping undesirable URL: {}".format(url))

        self.logger.info("Obtained kafka url: {}".format(url))
        return self.make_requests_from_url(url)

    def schedule_next_request(self, batch_size=128):
        """Schedules a request if available"""
        found = 0
        while found < batch_size:
            req = self.next_request()
            if req:
                self.crawler.engine.crawl(req, spider=self)
                found += 1
            else: # no request?
                break
        self.logger.info("Got batch of {} URLs to crawl".format(found))

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

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
        'LRU_MAX_PAGES_PER_SITE': 20  # only 20 pages per recent_sites cache entry ie. 20 pages per at least 500 sites spidered
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
       self.file_urls = [] # used by FilesPipeline to persist to local storage
       self.files = [] # to return results

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KafkaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        spider.logger.info("Closed spider") 

    def find_url(self, url):
        # cache lookups as excessive mongo calls slow the spider down
        if url in self.cache:
            return self.cache[url] 
        ret = self.db.urls.find_one({ 'url': url })
        self.cache[url] = ret
        return ret

    def penalise(self, host, penalty=1):
        if not host in self.recent_sites:
             self.recent_sites[host] = 0
        self.recent_sites[host] += penalty

    def is_recently_crawled(self, url, up):
        # recently fetched the URL?
        if url in self.recent_cache:
             return True
        # LRU bar: if one of the last 100 sites and we've fetched 100 pages, we say no to future urls from the same host until it is evicted from the LRU cache
        host = up.netloc
        self.penalise(host)
        if self.recent_sites[host] > self.custom_settings.get('LRU_MAX_PAGES_PER_SITE'):
             return True # only eviction from the LRU cache will permit host again
        return False

    def followup_pages(self, producer, url_list):
        """
           URLs which are on an australian IP are sent to kafka
        """
        urls = list(frozenset(url_list)) # de-dupe
        self.logger.info("Considering {} url's for followup.".format(len(urls)))
        sent = 0
        rejected = 0
        not_au = 0
        topic = self.custom_settings.get('ONEURL_KAFKA_URL_TOPIC')
        self.logger.info("Looking for URLs to followup (max 100, given {})".format(len(urls)))
        for u in urls[:100]: # ignore pages with ridiculous number of urls, just consider first 100
           up = urlparse(u)
           priority = as_priority(u, up)
           if priority > 5:
               producer.send('rejected-urls', { 'url': u, 'reason': 'low priority' })
               rejected = rejected + 1
           elif self.au_locator.is_au(up.hostname):
               up = up._replace(fragment='')   # remove all fragments from spidering
               url = urlunparse(up)            # search for fragment-free URL
               # NB: avoid doing a find_url() as it is just too expensive with pages with large numbers of links
               producer.send(topic, { 'url': url }, key=up.netloc.encode('utf-8'))  # send to the pending queue but send one host to one partition
               sent = sent + 1
           else:
               producer.send('rejected-urls', { 'url': u, 'reason': 'not an AU IP address' })
               not_au = not_au + 1
        self.logger.info("Sent {} url's to kafka (rejected {}, not au {})".format(sent, rejected, not_au))

    def save_url(self, url, now):
         # ensure last_visited is kept accurate so that we can ignore url's which we've recently seen
         result = self.db.urls.find_one_and_update({ 'url': url }, { "$set": { 'url': url, 'last_visited': now } }, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
         return result.get(u'_id')

    def save_snippet(self, origin_url_id, script, script_len, sha256, md5):
         j = { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len }
         ret = self.db.snippets.find_one_and_update(j, 
                     { "$set": { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len, 'code': script }}, 
                     upsert=True, return_document=pymongo.ReturnDocument.AFTER)
         id = ret.get(u'_id')
         self.db.snippet_url.insert_one({ 'url_id': origin_url_id, 'snippet': id }) 

    def save_script(self, url, script, inline_script=False, content_type=None):
        # NB: we work hard here to avoid mongo calls which will slow down the spider

        # compute hashes to search for
        sha256 = hashlib.sha256(script).hexdigest()
        md5 = hashlib.md5(script).hexdigest()

        # check to see if in mongo already
        now = datetime.utcnow()
        url_id = self.save_url(url, now)
        #self.logger.info("Got oid {} for {}".format(url_id, url))

        script_len = len(script)
        if inline_script:
             self.save_snippet(url_id, script, script_len, sha256, md5)
        else:
             s = self.db.scripts.find_one_and_update({ 'sha256': sha256, 'md5': md5, 'size_bytes': script_len }, 
                           { '$set': { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len, 'code': script }}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
             self.db.script_url.insert_one( { 'url_id': url_id, 'script': s.get(u'_id') })

        # finally update the kafka visited queue
        self.producer.send('visited', { 'url': url, 'size_bytes': script_len, 'inline': inline_script,
                                           'content-type': content_type, 'when': str(now), 
					   'sha256': sha256, 'md5': md5 })


    def is_blacklisted(self, domain):
        # only run the query every 5 mins to avoid excessive db queries...
        five_mins_ago = datetime.utcnow() - timedelta(minutes=5)
        if self.last_blacklist_query < five_mins_ago:
             self.blacklisted_domains = self.db.blacklisted_domains.distinct('domain')

        return domain in self.blacklisted_domains

    def parse(self, response):
        status = response.status
        url = response.url
        if status < 200 or status >=400:
           self.recent_cache[url] = 1
           up = urlparse(url)
           self.logger.info("Penalising {} due to http status {}".format(up.netloc, status))
           self.penalise(up.netloc, penalty=2) # penalty for slowing down the crawl is quite severe
           return []

        content_type = response.headers.get('Content-Type', b'').decode().lower()
        ret = []
        self.logger.info("Processing page {} {}".format(content_type, url))
        if 'html' in content_type:
           hrefs = response.xpath('//a/@href').extract()
           self.recent_cache[url] = 1     # no repeats from crawler

           # but we also want suitable follow-up links for pushing into the url topic
           abs_hrefs = [urljoin(url, href) for href in hrefs]
           self.followup_pages(self.producer, abs_hrefs)
       
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

        return ret  # url's come only from kafka, not the parse() invocation

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(KafkaSpider)
    process.start() # the script will block here until the crawling is finished