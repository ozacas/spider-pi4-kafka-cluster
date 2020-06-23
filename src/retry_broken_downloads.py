#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
import os
import json
import argparse
import pylru
import pymongo
import requests
import hashlib
from time import sleep
from urllib.parse import urlparse
from datetime import datetime
from utils.misc import *
from utils.models import JavascriptArtefact
from utils.io import next_artefact, save_artefact

a = argparse.ArgumentParser(description="Attempt to download broken URLs despite scrapy failures. Tries multiple strategies")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='javascript-download-failure',
                    default_group='retry-broken-downloads',
                    default_to='visited')
add_debug_arguments(a)
add_mongo_arguments(a, default_access="read-write", default_user='rw')

def is_suitable(t):
   # for now a download is only considered suitable for 403 since we think its caused by unsuitable headers eg. user agent
   assert isinstance(t, tuple) and len(t) == 3
   return t[2] == 403 or t[2] >= 500

def iterate(consumer, max, verbose=False, cache=None):
   for value in next_artefact(consumer, max, None, verbose=verbose):
       # message will be in one of two formats:
       # 1. { "url": "https://www.XXXXXXXXXXX/js/jquery/jquery.js?ver=1.12.4-wp",
       #      "origin": "https://www.XXXXXXXXXXXXXX/ories", "reason": "download-error",
       #      "http-status": 403, "when": "2020-06-01 00:28:54.932249", "host": "pi2" }
       # or 2. {'origin': 'http://XXX.gov.au/', 'file_urls': ['http://XXXX.au/code.jquery.com/jquery.js'], 
       #        'files': [], 'when': '2020-05-07 00:16:55.474067', 'host': 'pi2'} 
       if 'file_urls' in value:
           artefact_url = value.get('file_urls')[0]
           cited_on = value.get('origin') 
           http_status = 403  # fake 403... to ensure retry happens
       elif 'http-status' in value:
           artefact_url = value.get('url')
           cited_on = value.get('origin')
           http_status = value.get('http-status')

       if artefact_url in cache: # ignore dupes should multiple pages cite the same JS artefact
           continue

       cache[artefact_url] = 1
       # so we standardise (as best we can) and yield only tuples to determine whether an attempt will be made to retry the download
       t = (artefact_url, cited_on, http_status)
       if is_suitable(t):
           if verbose:
               print(artefact_url)
           yield t

def strategy_1_pyrequests(db, producer, artefact_url, cited_on, **kwargs):
    assert db is not None
    assert producer is not None
    assert len(artefact_url) > 0
    assert len(cited_on) > 0

    ua = kwargs.get('ua', None)
    referrer = kwargs.get('referrer', kwargs.get('referer', None))  # one r or two.. your choice ;-)
    headers = {}
    if ua is not None:
        assert len(ua) > 0
        headers.update({ 'User-Agent': ua })
    if referrer is not None:
        assert len(referrer) > 0
        headers.update({ 'Referer': referrer })
    try:
        resp = requests.get(artefact_url, headers=headers, verify=False)
        if resp.status_code == 200:
           content = resp.content
           sha256 = hashlib.sha256(content).hexdigest()
           artefact = JavascriptArtefact(url=artefact_url, sha256=sha256,
                                         md5=hashlib.md5(content).hexdigest(), 
                                         size_bytes=len(content), origin=cited_on ) 
           # NB: ignore return result from save_artefact()
           ret, was_cached = save_artefact(db, artefact, None, content=content)  # will save to Mongo AND Kafka visited topic
           producer.send(kwargs.get('to'), ret, key=sha256.encode('utf-8'))
           return artefact
        else:
           return None   
    except Exception as e:
        print("Failed to download using python requests: {} (exception follows)".format(artefact_url))
        print(str(e))
        return None

def strategy_2_wget(db, producer, artefact_url, cited_on, **kwargs):
    return None # TODO FIXME...

def main(args, consumer=None, producer=None, db=None, cache=None):
   if args.v:
       print(args)
   if consumer is None:
       consumer = KafkaConsumer(args.consume_from, 
                            bootstrap_servers=args.bootstrap, 
                            group_id=args.group, 
                            auto_offset_reset=args.start,
                            consumer_timeout_ms= 10 * 1000, # 10s or we terminate consumption since we have caught up with all available messages
                            value_deserializer=json_value_deserializer())
   if producer is None:
       producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)
   if cache is None:
       cache = pylru.lrucache(500)
   if db is None:
       mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
       db = mongo[args.dbname]

   from requests.packages.urllib3.exceptions import InsecureRequestWarning
   requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
   random_ua = random_user_agent()
   print("Using random user agent for all requests: {}".format(random_ua))
   n_good = n_bad = 0
   for artefact_url, cited_on, http_status in iterate(consumer, args.n, verbose=args.v, cache=cache):
       artefact = None
       for strategy in [strategy_1_pyrequests, strategy_2_wget]:
           up = urlparse(cited_on)
           artefact = strategy(db, producer, artefact_url, cited_on, ua=random_ua, referrer=up.hostname, to=args.to)
           if artefact is not None:
               print("Successfully retrieved: {}".format(artefact))
               n_good += 1
               break 
       if artefact is None:
           n_bad += 1
       sleep(5)    # 5s delay between requests
   mongo.close()
   consumer.close()
   print("Processed {} artefacts successfully, {} could not be downloaded".format(n_good, n_bad))
   return 0

if __name__ == "__main__":
   save_pidfile('pid.fix.broken')
   status = main(a.parse_args())
   exit(status)
