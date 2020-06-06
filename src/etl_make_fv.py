#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from dataclasses import asdict
import os
import json
import pymongo
import hashlib
import argparse
import pylru
from datetime import datetime
from utils.features import analyse_script, get_script, safe_for_mongo
from datetime import datetime
from utils.io import save_analysis_content, next_artefact
from utils.models import JavascriptArtefact
from utils.misc import *

a = argparse.ArgumentParser(description="Extract features from each javascript in visited topic and dump into analysis-results topic")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='visited',
                    default_group='javascript-analysis',
                    default_to='analysis-results')
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--cache", help="Cache feature vectors to not re-calculate frequently seen JS (int specifies max cache entries, 0 disabled) [0]", type=int, default=0)
a.add_argument("--defensive", help="Enable extra checks to check data integrity during run", action="store_true")

def signal_handler(num, frame):
    global consumer
    global mongo
    print("Control-C pressed. Terminating... may take a while for current batch to complete")
    cleanup([mongo, consumer])
    exit(1)

def cleanup(items_to_close, pidfile='pid.make.fv'):
    for c in items_to_close:
        c.close()
    rm_pidfile(pidfile)

def report_failure(producer, artefact, reason):
    d = asdict(artefact)
    d['reason'] = reason if len(reason) < 300 else "{}...".format(reason[0:300])
    producer.send('feature-extraction-failures', d)

def save_to_kafka(producer, results, to='analysis-results', key=None):
   # and now kafka now that the DB has been populated
   assert 'js_id' in results and '_id' not in results
   producer.send(to, results, key=key)

def iterate(consumer, max, cache, verbose=False):
   for r in next_artefact(consumer, max, lambda v: 'javascript' in v.get('content-type', ''), verbose=verbose):
       jsr = JavascriptArtefact(**r)
       if not jsr.url in cache:
           yield jsr

def main(args, consumer=None, producer=None, db=None, cache=None):
   if args.v:
       print(args)
   if producer is None:
       producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)
   if cache is None:
       cache = pylru.lrucache(1000)

   if not os.path.exists(args.java):
      raise ValueError("Java executable does not exist: {}".format(args.java))
   if not os.path.exists(args.extractor):
      raise ValueError("JAR file to extract features does not exist: {}".format(args.extractor))
   # we want only artefacts which are not cached and are JS (subject to maximum record limits)
   fv_cache = pylru.lrucache(args.cache) if args.cache > 0 else None
   if fv_cache is None:
       print("WARNING: not using FV cache - are you sure you wanted to?")

   n_cached = n_analysed = n_failed = 0
   is_first = True
   for jsr in iterate(consumer, args.n, cache, verbose=args.v):
      # eg.  {'url': 'https://XXXX.asn.au/', 'size_bytes': 294, 'inline': True, 'content-type': 'text/html; charset=UTF-8', 
      #       'when': '2020-02-06 02:51:46.016314', 'sha256': 'c38bd5db9472fa920517c48dc9ca7c556204af4dee76951c79fec645f5a9283a', 
      #        'md5': '4714b9a46307758a7272ecc666bc88a7', 'origin': 'XXXX' }  NB: origin may be none for old records (sadly)
      assert isinstance(jsr, JavascriptArtefact)
      #assert len(jsr.js_id) > 0
      cache[jsr.url] = 1

      # 1. verbose?
      if args.v:
          print(jsr)

      # 2. got results cache hit ??? Saves computing it again and hitting the DB, which is slow...
      if fv_cache is not None and jsr.js_id in fv_cache:
          byte_content, js_id = fv_cache[jsr.js_id]
          n_cached += 1
          # falsely "update" the cache to ensure it is rewarded for being hit ie. becomes MRU
          fv_cache[js_id] = (byte_content, js_id)
          # FALLTHRU
      else:
          # 3. obtain and analyse the JS from MongoDB and add to list of analysed artefacts topic. On failure lodge to feature extraction failure topic
          js, js_id = get_script(db, jsr)
          if js is None:
              report_failure(producer, jsr, 'Could not locate in MongoDB')
              n_failed += 1
              continue
          if args.defensive:
              # validate that the data from mongo matches the expected hash or die trying...
              assert js_id == jsr.js_id
              assert hashlib.sha256(js).hexdigest() == jsr.sha256
              assert hashlib.md5(js).hexdigest() == jsr.md5
              assert len(js) == jsr.size_bytes

          byte_content, failed, stderr = analyse_script(js, jsr, java=args.java, feature_extractor=args.extractor)
          n_analysed += 1
          if failed:
              report_failure(producer, jsr, "Unable to analyse script: {}".format(stderr))
              n_failed += 1
              continue
          # put results into the cache and then FALLTHRU...
          if fv_cache is not None:
              fv_cache[js_id] = (byte_content, js_id)

      if len(jsr.js_id) == 0:
          jsr.js_id = js_id

      save_analysis_content(db, jsr, byte_content, ensure_indexes=is_first)
      is_first = False
      results = asdict(jsr)
      results.update({ 'js_id': js_id })  # this will be sufficient to load the vector from Mongo by the receiving application
      results.update({ 'byte_content_sha256': hashlib.sha256(byte_content).hexdigest() })
      save_to_kafka(producer, results, to=args.to)

   print("Analysed {} artefacts, {} failed, {} cached, now={}".format(n_analysed, n_failed, n_cached, str(datetime.now())))
   cleanup([mongo, consumer])
   return 0

if __name__ == "__main__":
   save_pidfile('pid.make.fv')
   setup_signals(signal_handler)
   global mongo
   global consumer
   args = a.parse_args()
   mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
   db = mongo[args.dbname]
 
   consumer = KafkaConsumer(args.consume_from, 
                            bootstrap_servers=args.bootstrap, 
                            group_id=args.group, 
                            auto_offset_reset=args.start,
                            value_deserializer=json_value_deserializer(),
                            max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
   status = main(args, consumer=consumer, db=db)
   exit(status)
