# -*- coding: utf-8 -*-
import scrapy
from twisted.internet import task
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DontCloseSpider

from dataclasses import asdict
import json
import hashlib
import pymongo
import argparse
import pylru
from collections import namedtuple
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from urllib.parse import urljoin, urlparse, urlunparse
from utils.fileitem import FileItem
from utils.AustraliaGeoLocator import AustraliaGeoLocator
from utils.url import as_priority
from utils.models import PageStats

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
        if up.hostname:
            self.penalise(up.hostname, penalty=2) 
            self.logger.info("Penalise {} due to download failure".format(up.hostname))

    def kafka_next(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        valid = False
        skipped = 0
        while not valid:
           message = next(self.consumer)
           url = self.process_kafka_message(message)
           if url is None:
               return None
           # we implement the blacklist on the dequeue side so that we can blacklist as the crawl proceeds. Not enough to just blacklist on the enqueue side
           if self.is_suitable(url, kafka_message=message): # pass message so that is_suitable() can look at content_type or whatever from kafka topic chosen by spider
               valid = True  # no more iterations
           else:
               #self.logger.info("Skipping undesirable URL: {}".format(url))
               skipped += 1

        #self.logger.info("Obtained kafka url: {}".format(url))
        return (url, skipped)

    def is_suitable_host(self, host, counts_by_host):
        # we must provide this as a method so that the snippetspider can override to ignore this logic, which is not relevant to it
        if not host in counts_by_host:
             counts_by_host[host] = 1
        else:
             counts_by_host[host] += 1
             if counts_by_host[host] <= 10: 
                  pass # FALLTHRU
             else:
                  return False
        return True

    def schedule_next_request(self, batch_size=200):
        """Schedules a request if available"""
        found = 0
        non_random = 0
        skipped_total = 0
        batch = set()
        counts_by_host = { }
        while found < batch_size:
            url, skipped = self.kafka_next()
            skipped_total += skipped
            if url: 
                if not url in batch: # else ignore it, since it is a dupe from kafka
                    up = urlparse(url)
                    host = up.hostname
                    if self.is_suitable_host(host, counts_by_host):
                        req = scrapy.Request(url, callback=self.parse, errback=self.errback, dont_filter=True)
                        self.crawler.engine.crawl(req, spider=self)
                        batch.add(url)
                        found += 1
                    else: 
                        #self.logger.info("Ignoring due to non-random ingest for {}".format(url))
                        # TODO FIXME... push back onto the queue? nah... we have no shortage of urls!
                        #self.producer.send(self.settings.get('ONEURL_KAFKA_URL_TOPIC'), { 'url': url })
                        non_random += 1
                        pass
            else: # no request?
                break

        self.logger.info("Skipped {} URLs as undesirable, {} URLs as too many in a single batch to the same site".format(skipped_total, non_random))
        self.logger.info("Got batch of {} URLs to crawl".format(found))

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def spider_closed(self, spider):
        spider.logger.info("Closed spider") 

class KafkaSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'kafkaspider'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)

    def recent_site_eviction(self, key, value):
       self.logger.info("Evicting site cache entry: {} = {} (cache size {})".format(key, value, len(self.recent_sites)))

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)
       settings = get_project_settings()
       topic = settings.get('ONEURL_KAFKA_URL_TOPIC')
       bs = settings.get('ONEURL_KAFKA_BOOTSTRAP')
       grp_id = settings.get('ONEURL_KAFKA_CONSUMER_GROUP')
       self.consumer = KafkaConsumer(topic, bootstrap_servers=bs, group_id=grp_id, auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout 
       self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=bs)
       self.au_locator = AustraliaGeoLocator(db_location=settings.get('ONEURL_MAXMIND_DB'))
       self.mongo = pymongo.MongoClient(settings.get('MONGO_HOST', settings.get('MONGO_PORT')))
       self.db = self.mongo[settings.get('MONGO_DB')]
       self.cache = pylru.lrucache(10 * 1024) # dont insert into kafka url topic if url seen recently
       self.recent_cache = pylru.lrucache(10 * 1024) # size just a guess (roughly a few hours of spidering, non-JS script URLs only)
       self.recent_sites = pylru.lrucache(500, self.recent_site_eviction) # last 100 sites (value is page count fetched since cache entry created for host)
       self.js_cache = pylru.lrucache(500) # dont re-fetch JS which we've recently seen
       self.update_blacklist() # to ensure self.blacklist_domains is populated

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KafkaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        t = task.LoopingCall(spider.update_blacklist)
        t.start(300) # every 5 minutes
        #reactor.run()  # spider will do this for us, so no need...
        return spider

    # NB: any call to this function will ensure that host is no longer least recently used, regardless of penalty, which is the desired behaviour?
    def penalise(self, host, penalty=1):
        if not host in self.recent_sites:
            self.recent_sites[host] = 0
        if penalty != 0:     # NB: dont reset the LRU timestamp for the host unless penalty has actually been specified....
            self.recent_sites[host] += penalty
        return self.recent_sites[host]

    def is_recently_crawled(self, url, up):
        # recently fetched the URL?
        if url in self.recent_cache:
             return True
        # LRU bar: if one of the last 100 sites and we've fetched 100 pages, we say no to future urls from the same host until it is evicted from the LRU cache
        host = up.hostname
        n_seen = self.penalise(host, penalty=0) # NB: penalty=0 so that we dont side-effect self.recent_sites[host]
        if n_seen > self.settings.get('LRU_MAX_PAGES_PER_SITE'):
             return True # only eviction from the LRU cache will permit host again
        return False

    def followup_pages(self, producer, url_iterable, max=100):
        """
           URLs which are on an australian IP are sent to kafka
        """
        sent = 0
        rejected = 0
        not_au = 0
        # the more pages are in the LRU cache for the site
        topic = self.settings.get('ONEURL_KAFKA_URL_TOPIC')
        for u in url_iterable:
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
        self.logger.info("Rejected {} low priority URLs. {} not AU.".format(rejected, not_au))
        return sent

    def is_blacklisted(self, domain):
        return domain in self.blacklisted_domains

    def update_blacklist(self):
        self.blacklisted_domains = self.db.blacklisted_domains.distinct('domain')

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
           ps = PageStats(url, str(datetime.utcnow()))

           # but we also want suitable follow-up links for pushing into the url topic 
           up = urlparse(url)
           # NB: by not considering every link on a page we reduce the maxmind cost and other spider slowdowns at limited loss of data 
           n_seen = self.penalise(up.hostname)
           follow_internals = n_seen < self.settings.get('SITE_INTERNAL_LINK_LIMIT')
           if n_seen > 20:
               n_seen = 20
           max = 100 - 4 * n_seen 
           abs_hrefs = [urljoin(url, href) for href in hrefs]
           ps.n_hrefs = len(abs_hrefs)
           ps.n_hrefs_max_permitted = max
           external_hrefs = set() # prioritise external links unless max is large enough to cater to both categories on the page (ie. broad crawl rather than deep)
           internal_hrefs = set()
           for u in abs_hrefs:
                if not up.hostname in u:
                     external_hrefs.add(u)
                else:
                     internal_hrefs.add(u)
           ps.n_external = len(external_hrefs)
           n = self.followup_pages(self.producer, external_hrefs, max=max) 
           ps.n_external_accepted = n
           left = max - n
           if left < 0:
               left = 0
           ps.n_internal = len(internal_hrefs)
           # dont follow internal links if we've seen a lot of pages in the LRU cache....
           if follow_internals:
               n = self.followup_pages(self.producer, internal_hrefs, max=left)
               ps.n_internal_accepted = n
           else:
               self.logger.info("Not following internal links for {} as spidered lots already".format(up.hostname))
               ps.n_internal_accepted = 0
       
           # spider over the JS content... 
           src_urls = response.xpath('//script/@src').extract()
           # ensure relative script src's are absolute... for the spider to follow now
           abs_src_urls = [urljoin(url, src) for src in src_urls]
           n = 0
           concat = ''
           for u in abs_src_urls:
               if not u in self.js_cache and self.is_suitable(u, check_priority=False):
                   item = FileItem(origin=url, file_urls=[u])
                   ret.append(item) # will trigger FilesPipeline to fetch it with priority and persist to local storage
                   self.js_cache[u] = 1 # dont fetch this JS again for at least 500 JS url's....
                   n += 1
               concat += '{} '.format(u) # all script urls are put into kafka, not just acceptable ones (for evidentiary purposes)
           ps.n_scripts = len(abs_src_urls)
           ps.n_scripts_accepted = n
           ps.scripts = concat
           self.producer.send(self.settings.get('PAGESTATS_TOPIC'), asdict(ps), key=up.hostname.encode()) # specifying the key means the same site in one partition of the topic
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
