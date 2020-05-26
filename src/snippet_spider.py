# -*- coding: utf-8 -*-
import scrapy
from twisted.internet import task
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import hashlib
import pymongo
import pylru
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafkaspider import KafkaSpiderMixin
from utils.models import Password
from utils.misc import save_pidfile, rm_pidfile

class SnippetSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'snippetspider'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)

    def __init__(self, *args, **kwargs):
       super().__init__(*args, **kwargs)
       settings = get_project_settings()
       topic = settings.get('SNIPPETSPIDER_URL_TOPIC')
       bs = settings.get('ONEURL_KAFKA_BOOTSTRAP')
       grp_id = settings.get('SNIPPETSPIDER_CONSUMER_GROUP')
       self.logger.info("Reading URLs from {}".format(topic))
       self.logger.info("Consumer group for URLs {}".format(grp_id))
       self.logger.info("Bootstrapping via {}".format(bs))
       self.consumer = KafkaConsumer(topic, bootstrap_servers=bs, group_id=grp_id, 
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout 
       self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=bs)
       host = settings.get('MONGO_HOST') 
       port = settings.get('MONGO_PORT')
       user = settings.get('SNIPPETSPIDER_MONGO_USER')
       password = str(Password(Password.DEFAULT)) # NB: read from environment variable iff specified
       mongo = pymongo.MongoClient(host, port, username=user, password=password)
       self.db = mongo[settings.get('MONGO_DB')]
       self.recent_cache = pylru.lrucache(10 * 1024)
       self.cache = pylru.lrucache(500)
       self.update_blacklist()
       self.disinterest_topic = settings.get('OVERREPRESENTED_HOSTS_TOPIC')
       self.overrepresented_hosts = self.init_overrepresented_hosts(self.disinterest_topic, bs)

       save_pidfile('pid.snippetspider')
       self.host_rejection_criteria = ( ('host_blacklisted', self.is_blacklisted),         # ignore hosts outside scope or which are manually deemed irrelevant
                                        ('host_long_term_ban', self.is_long_term_banned),  # we obey long-term bans to avoid contacting the site in concert across spiders
                                      )

    def spider_closed(self, spider):
        # NB: to be overridden by subclass eg. to persist state
        spider.logger.info("Closed spider")
        rm_pidfile('pid.snippetspider')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SnippetSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        t = task.LoopingCall(spider.update_blacklist)
        t.start(300) # update blacklist every 5 minutes to avoid hitting mongo too much
        #reactor.run()  # spider will do this for us, so no need...
        return spider

    def penalise(self, *args, **kwargs):
        # MUST implement this, but it does nothing for this spider
        pass

    def find_url(self, url):
        # cache lookups as excessive mongo calls slow the spider down
        if url in self.cache:
            return self.cache[url] 
        ret = self.db.urls.find_one({ 'url': url })
        self.cache[url] = ret
        return ret

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

    def save_inline_script(self, url, script, content_type=None):
        # NB: we work hard here to avoid mongo calls which will slow down the spider

        # compute hashes to search for
        sha256 = hashlib.sha256(script).hexdigest()
        md5 = hashlib.md5(script).hexdigest()

        # check to see if in mongo already
        now = datetime.utcnow()
        url_id = self.save_url(url, now)
        #self.logger.info("Got oid {} for {}".format(url_id, url))

        script_len = len(script)
        self.save_snippet(url_id, script, script_len, sha256, md5)

    def update_blacklist(self):
         self.blacklisted_domains = self.db.blacklisted_domains.distinct('domain')

    def parse(self, response):
        status = response.status
        url = response.url
        if status < 200 or status >=400:
           # if we fail to get something already visited dont worry about it - tis the life of a spider...
           return []

        content_type = response.headers.get('Content-Type', b'').decode().lower()
        ret = []
        self.logger.info("Processing page {} {}".format(content_type, url))
        if 'html' in content_type:
           inline_scripts = response.xpath('//script/text()').getall() 
           for script in inline_scripts:
               self.save_inline_script(url, script.encode('utf-8'), content_type=content_type)
        else:
           self.logger.info("Received undesired content type: {} for {}".format(content_type, url))

        return ret  # url's come only from kafka, not the parse() invocation

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(SnippetSpider)
    process.start() # the script will block here until the crawling is finished
