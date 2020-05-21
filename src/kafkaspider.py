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
import random
import socket
import os
from collections import namedtuple
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from urllib.parse import urljoin, urlparse, urlunparse
from utils.fileitem import FileItem
from utils.geo import AustraliaGeoLocator
from utils.misc import as_priority, rm_pidfile, save_pidfile
from utils.models import PageStats, Password

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
        for message in self.consumer: 
           url = self.process_kafka_message(message)
           yield url 

    def is_congested_batch(self, up):
        """
        Returns True if the parsed url instance reflects a URL which we have too many of in the current batch. False otherwise.
        KafkaSpider fetches a maximum of 10 pages per host in a given batch (which lasts a few minutes typically)
        """
        assert up is not None
        host = up.hostname
        if host is None: # may happen for mailto:, tel: and other URL schemes
            return True  # falsely say that the host is congested, want to get rid of bad urls...
        lru = self.congested_batch
        assert lru is not None
        if not host in lru:
             lru[host] = 1
             return False
        else:
             lru[host] += 1
             return lru[host] > 10 # no more than 10 pages per batch to one hostname

    def is_recently_crawled(self, up):
        """
        LRU bar: if one of the last 100 sites and we've fetched 100 pages, we say 
        no to future urls from the same host until it is evicted from the LRU cache. Returns True if so, False otherwise.
        """
        host = up.hostname
        n_seen = self.penalise(host, penalty=0) # NB: penalty=0 so that we dont side-effect self.recent_sites[host]
        if n_seen > self.settings.get('LRU_MAX_PAGES_PER_SITE'):
             return True # only eviction from the LRU cache will permit host again
        return False

    def is_long_term_banned(self, up):
        """
        Parsed host present in the long-term disinterest topic? If so, we've hit it enough for the next 3 months. Returns True if so, False otherwise.
        """
        assert up is not None
        return up.hostname in self.overrepresented_hosts

    def in_recent_sites(self, up):
        """
        Return True (ie. do not crawl) if LRU says we've seen more than twenty pages (incl. penalties) already...
        """
        assert up is not None
        host = up.hostname
        return host in self.recent_sites and self.recent_sites[host] > 20

    def penalise(self, host, penalty=1):
        assert isinstance(host, str) # check that people dont accidentally pass ParseResult() for this fn

        if penalty != 0: # do not side-effect self.recent_sites if penalty is 0 to avoid premature eviction during highly duplicated kafka ingest
            if not host in self.recent_sites:
                 self.recent_sites[host] = 0
            self.recent_sites[host] += penalty
            return self.recent_sites[host]
        else:
            if host in self.recent_sites:
                return self.recent_sites[host]
            else:
                return 0

    def is_blacklisted(self, up):
        """
        Return True if specified host appears to be blacklisted ie. not to be crawled under any circumstances, ever. False otherwise.
        """
        assert up.hostname is not None
        ret = up.hostname in self.blacklisted_domains
        return ret

    def unsuitable_priority(self, up):
        assert up is not None
        return as_priority(up) > 4

    def is_outside_au(self, up):
        """
        Return False if parsed hostname ends with '.au' or geolocated to Australian network. Otherwise True
        """
        assert self.au_locator is not None
        host = up.hostname
        assert host is not None
        # avoid expensive geo lookup if we can, by assuming '.au' implies inside AU. Seems ok for the most part.
        ok = host.endswith('.au') or self.au_locator.is_au(host)
        return not ok

    def is_suitable(self, url, stats=None, rejection_criteria=None):
        if url is None or not (url.startswith("http://") or url.startswith("https://")) or url in self.recent_cache:
            if stats:
               stats['url_rejected'] += 1
            return False
        up = urlparse(url)
        if up.hostname is None:
            print("WARNING: got bad URL {}".format(url))
            return False
        if rejection_criteria is None:
            rejection_criteria = self.host_rejection_criteria
      
        for t in rejection_criteria:
            assert isinstance(t, tuple) and len(t) == 2
            k, cb = t
            if cb(up):
               if stats:
                  stats[k] += 1
               return False
        return True 

    def schedule_next_request(self, batch_size=400):
        """Schedules a request if available"""
        self.congested_batch = pylru.lrucache(500) # new instance every batch ie. very short-term memory for now

        stats = { t[0]: 0 for t in self.host_rejection_criteria } 
        stats.update({ 'url_rejected': 0, 'found': 0 })

        while stats['found'] < batch_size:
            url = next(self.kafka_next())
            ok = self.is_suitable(url, stats=stats)
            if ok:
                batch.add(url)
                req = scrapy.Request(url, callback=self.parse, errback=self.errback) # NB: let scrapy filter dupes should they happen (eg. across batches) 
                self.crawler.engine.crawl(req, spider=self)
                stats['found'] += 1

        self.logger.info("URL batch statistics: {}".format(stats))

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def spider_closed(self, spider):
        # NB: to be overridden by subclass eg. to persist state
        spider.logger.info("Closed spider") 

class KafkaSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'kafkaspider'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)

    def recent_site_eviction(self, key, value):
       self.logger.info("Evicting site cache entry: {} = {} (cache size {})".format(key, value, len(self.recent_sites)))
       # the average value seen anecdotally at eviction time is around 10 pages before it is evicted. So if we see more than 100 then its consistently over-represented in the URL list. So we dont want to come back for a while. Hence we add the site to a long term "medium" blacklist which will hurt it for about a month of spidering and allow us to
       # focus on domains which we've not seen yet. The long-term blacklist is loaded at spider init, since the number of unique domains is 
       # likely to be fairly small. It is evaluated when reading each batch of URLs to ignore stuff which is overrepresented in the past month (topic retention time)
       if value > 100 and self.init_completed: # NB: load of persistent cache will cause mass evictions, which we dont want to trigger (yet)
           
           self.producer.send(self.disinterest_topic, { 'hostname': key, 'n_pages': value, 'date': datetime.utcnow().strftime("%m/%d/%Y") })

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)
       settings = get_project_settings()
       self.init_completed = False
       topic = settings.get('ONEURL_KAFKA_URL_TOPIC')
       bs = settings.get('ONEURL_KAFKA_BOOTSTRAP')
       grp_id = settings.get('ONEURL_KAFKA_CONSUMER_GROUP')
       site_cache_max = settings.get('KAFKASPIDER_MAX_SITE_CACHE', 1000)
       recent_cache_max = settings.get('KAFKASPIDER_MAX_RECENT_CACHE', 5000)
       self.disinterest_topic = settings.get('OVERREPRESENTED_HOSTS_TOPIC')
       self.consumer = KafkaConsumer(topic, bootstrap_servers=bs, group_id=grp_id, auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=60000000) # crank max poll to ensure no kafkapython timeout, consumer wont die as the code is perfect ;-)
       self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, separators=(',', ':')).encode('utf-8'), bootstrap_servers=bs, batch_size=4096)
       self.au_locator = AustraliaGeoLocator(db_location=settings.get('ONEURL_MAXMIND_DB'))
       host = settings.get('MONGO_HOST')
       port = settings.get('MONGO_PORT')
       user = settings.get('KAFKASPIDER_MONGO_USER')    # to access blacklist
       password = str(Password(Password.DEFAULT, 'Password for {}@{}: '.format(user, host)))
       mongo = pymongo.MongoClient(host, port, username=user, password=password)
       self.db = mongo[settings.get('MONGO_DB')]
       self.recent_cache = pylru.lrucache(recent_cache_max) # size just a guess (roughly a few hours of spidering, non-JS script URLs only)
       self.recent_sites = pylru.lrucache(site_cache_max, self.recent_site_eviction) # last X sites (value is page count fetched since cache entry created for host)
       self.js_cache = pylru.lrucache(500) # dont re-fetch JS which we've recently seen
       self.internal_cache = pylru.lrucache(1000) # only for internal hrefs, stop continually adding the same link
       self.update_blacklist() # to ensure self.blacklist_domains is populated
       # populate recent_sites from previous run on this host
       self.init_recent_sites(self.recent_sites, bs)
       self.init_completed = True
       self.host_rejection_criteria = ( ('host_blacklisted', self.is_blacklisted), 
                                        ('host_recently_crawled', self.is_recently_crawled), 
                                        ('host_recent_sites', self.in_recent_sites), 
                                        ('host_congested_batch', self.is_congested_batch), 
                                        ('host_long_term_ban', self.is_long_term_banned),
                                        ('host_bad_priority', self.unsuitable_priority),
                                        ('host_not_au', self.is_outside_au) )
       # populate the over-represented sites (~1 month visitation blacklist)
       self.overrepresented_hosts = self.init_overrepresented_hosts(self.disinterest_topic, bs)
       # write a PID file so that ansible wont start duplicates on this host
       save_pidfile("pid.kafkaspider")

    def spider_closed(self, spider):
       # persist recent_sites to kafka topic so that we have long-term memory of caching
       d = { }
       d['host'] = socket.gethostname()
       long_term = { }
       # NB: update recent sites cache for next run, whenever that may be
       for k,v in spider.recent_sites.items():
           d[k] = v
           if v > 100:  # should be entered into long-term memory?
              long_term[k] = v
       spider.producer.send('recent-sites-cache', d)
       self.logger.info("Saved recent sites to cache")
       # NB: push long-term disinterested data into topic
       for k in long_term.keys():
            spider.logger.info("Adding {} to long-term memory (visited {} pages, incl. penalties)".format(k, long_term[k]))
            spider.producer.send(spider.disinterest_topic, { 'hostname': k, 'n_pages': long_term[k], 'date': datetime.utcnow().strftime("%m/%d/%Y") }) 

       # ensure main kafka consumer is cleaned up cleanly so that other spiders can re-balance as cleanly as possible
       spider.producer.flush()
       spider.producer.close()
       spider.consumer.close()
       rm_pidfile("pid.kafkaspider")

    def init_overrepresented_hosts(self, topic, bootstrap_servers):
       # since the kakfa topic retention time is set to a month, we can identify sites which are being visited every day and penalise them
       hosts = set()
       calc = { }
       consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=None, auto_offset_reset='earliest',
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')), consumer_timeout_ms=1000) 
       # 1.process large sites in topic
       for message in consumer:
           d = message.value
           site = d['hostname']
           ymd = d['date']
           n = d['n_pages']
           if site not in calc:
               s = set()
               s.add(ymd)
               calc[site] =  { 'n': n, 'dates': s }
           else:
               existing = calc[site]
               s = existing['dates']
               s.add(ymd)
               existing['n'] = max([n, existing['n']])
       # 2. add highly fetch sites to internal data structure for the crawl. No more fetching until removed from the topic
       for site in calc.keys():
           total_n = calc[site].get('n') 
           total_days = len(calc[site].get('dates'))
           if total_n >= 200:
              hosts.add(site) 
           elif total_n >= 100 and total_days >= 3:
              hosts.add(site)

       self.logger.info("Long-term blacklist has {} sites.".format(len(hosts)))
       return hosts

    def init_recent_sites(self, cache, bootstrap_servers):
       # populate recent_sites from most-recent message in kafka topic
       # BUG: we dont know which partitions (ie. hostnames) the spider will be assigned to. So we read every message and populate the LRU
       #      cache in the hope that at least 1/2 of the cache is accurate ie. assigned to this spider (for the two spider operational case)
       consumer = KafkaConsumer('recent-sites-cache', bootstrap_servers=bootstrap_servers, group_id=None, auto_offset_reset='earliest',
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')), consumer_timeout_ms=1000)
       # load only the most recent state message from this host, although this will be inaccurate if we change spider hosts, since consumption partitions will change
       for message in consumer:
           d = message.value
           for k,v in d.items():
              if k != 'host':
                 if k in cache:
                     cache[k] = max([v, cache[k]])  # frequently visited sites will be MRU relative to rarely seen sites 
                 else:
                     cache[k] = v

       self.logger.info("Populated recent sites with {} cache entries".format(len(cache)))
       consumer.close() # wont need this consumer anymore
       pass

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KafkaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        t = task.LoopingCall(spider.update_blacklist)
        t.start(300) # every 5 minutes
        #reactor.run()  # spider will do this for us, so no need...
        return spider

    def followup_pages(self, producer, url_iterable, max=100):
        """
           URLs which are on an australian IP are sent to kafka
        """
        sent = 0
        rejected = 0
        not_au = 0
        # the more pages are in the LRU cache for the site
        topic = self.settings.get('ONEURL_KAFKA_URL_TOPIC')
        stats = { t[0]: 0 for t in self.host_rejection_criteria }
        stats.update({ 'found': 0, 'url_rejected': 0 })

        for u in url_iterable:
           if self.is_suitable(u, stats=stats):
               up = urlparse(u)
               up = up._replace(fragment='')   # remove all fragments from spidering
               url = urlunparse(up)            # search for fragment-free URL
               producer.send(topic, { 'url': url }, key=up.hostname.encode('utf-8'))
               sent += 1
               if sent > max: 
                   break
        self.logger.debug("Sent {} URLs to kafka. Rejections: {}".format(sent, stats))
        return sent


    def update_blacklist(self):
        self.blacklisted_domains = set(self.db.blacklisted_domains.distinct('domain'))

    def parse(self, response):
        status = response.status
        url = response.url
        if status != 200:
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
           self.logger.debug("Visited {} pages for {}".format(n_seen, up.hostname))
           internals_limit = self.settings.get('SITE_INTERNAL_LINK_LIMIT')
           follow_internals = n_seen < internals_limit
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
           # NB: we dont reject based on current crawling state since it will be a long-time before these URLs get visited (months)
           limited = filter(lambda t: t[0] in set(['host_recently_crawled', 'host_recent_sites', 'host_congested_batch']), self.host_rejection_criteria)
           n = self.followup_pages(self.producer, filter(lambda u: self.is_suitable(u, rejection_criteria=limited), external_hrefs), max=max) 
           ps.n_external_accepted = n
           left = max - n
           if left < 0:
               left = 0
           elif left > internals_limit: # NB: dont pollute url topic with far too many links and preventing spider from crawling broadly
               left = internals_limit
           ps.n_internal = len(internal_hrefs)
           # dont follow internal links if we've seen a lot of pages in the LRU cache....
           if follow_internals:
               # since we are sampling, try to make sure its a random sample to avoid navigation bias
               irefs = list(internal_hrefs)
               random.shuffle(irefs) 
               n = self.followup_pages(self.producer, filter(lambda u: not u in self.internal_cache and 
                                            self.is_suitable(u, rejection_criteria=limited), irefs), max=left)
               ps.n_internal_accepted = n
               for i in range(n):
                   self.internal_cache[irefs[i]] = 1 # dont follow this internal link again in the short-term
           else:
               ps.n_internal_accepted = 0
      
           self.logger.info("Followed {} external and {} internal links on {}".format(ps.n_external_accepted, ps.n_internal_accepted, url))

           # spider over the JS content... 
           src_urls = response.xpath('//script/@src').extract()
           # ensure relative script src's are absolute... for the spider to follow now
           abs_src_urls = [urljoin(url, src) for src in src_urls]
           n = 0
           concat = ''
           js_rejection_criteria = ( )  # NB: much smaller rejection list for JS, note no AU rejection

           for u in abs_src_urls:
               if not u in self.js_cache and self.is_suitable(u, rejection_criteria=js_rejection_criteria):
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
          
        self.logger.info("Adding {} items to FilesPipeline".format(len(ret)))

        return ret  # url's come only from kafka, not the parse() invocation. But JS artefacts will be returned from here for the FilesPipeline to ingest 

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(KafkaSpider)
    process.start() # the script will block here until the crawling is finished
