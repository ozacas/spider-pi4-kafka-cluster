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
from utils.url import as_priority
from kafkaspider import KafkaSpiderMixin

class SnippetSpider(KafkaSpiderMixin, scrapy.Spider):
    name = 'snippetspider'
    allowed_domains = ['*']  # permitted to crawl anywhere (except unless blacklisted)
    custom_settings = {
        'ONEURL_MONGO_HOST': 'pi1', 
        'ONEURL_MONGO_PORT': 27017,
        'ONEURL_MONGO_DB': 'au_js',
        'ONEURL_KAFKA_BOOTSTRAP': 'kafka1',
        'ONEURL_KAFKA_CONSUMER_GROUP': 'snippetspider',
        'ONEURL_KAFKA_URL_TOPIC': 'visited',
        'LOG_LEVEL': 'INFO',
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
       self.mongo = pymongo.MongoClient(settings.get('ONEURL_MONGO_HOST', settings.get('ONEURL_MONGO_PORT')))
       self.db = self.mongo[settings.get('ONEURL_MONGO_DB')]
       self.last_blacklist_query = datetime.utcnow() - timedelta(minutes=20) # force is_blacklisted() to query at startup
       self.recent_cache = pylru.lrucache(10 * 1024)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SnippetSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def is_suitable(self, url, kafka_message=None, check_priority=True):
        if url in self.recent_cache:
            return False
        ret = True
        if kafka_message:  # check to see if kafka message has content type and we only want to visit html pages
            content_type = kafka_message.value.get('content-type', '')
            ret = 'html' in content_type
            # FALLTHRU
        self.recent_cache[url] = 1 
        return ret

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

    def is_blacklisted(self, domain):
        # only run the query every 5 mins to avoid excessive db queries...
        five_mins_ago = datetime.utcnow() - timedelta(minutes=5)
        if self.last_blacklist_query < five_mins_ago:
             self.blacklisted_domains = self.db.blacklisted_domains.distinct('domain')
             self.last_blacklist_query = datetime.utcnow()

        return domain in self.blacklisted_domains

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
               self.save_script(url, script.encode('utf-8'), inline_script=True, content_type=content_type)
        else:
           self.logger.info("Received undesired content type: {} for {}".format(content_type, url))

        return ret  # url's come only from kafka, not the parse() invocation

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(SnippetSpider)
    process.start() # the script will block here until the crawling is finished
