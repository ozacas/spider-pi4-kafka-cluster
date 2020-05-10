#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from dataclasses import asdict
import os
import json
import pymongo
import argparse
import pylru
from datetime import datetime
from utils.features import analyse_script, get_script
from datetime import datetime
from utils.io import save_call_vector, save_ast_vector, next_artefact, batch
from utils.models import JavascriptArtefact
from utils.misc import *

a = argparse.ArgumentParser(description="Extract features from each javascript in visited topic and dump into analysis-results topic")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='visited',
                    default_group='javascript-analysis',
                    default_to='analysis-results')
add_mongo_arguments(a, default_access="read-write")
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--cache", help="Cache feature vectors to not re-calculate frequently seen JS (int specifies max cache entries, 0 disabled) [0]", type=int, default=0)

terminate = False
def signal_handler(num, frame):
    global terminate
    print("Control-C pressed. Terminating... may take a while for current batch to complete")
    terminate = True

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

def iterate(consumer, max, verbose=False):
   for r in next_artefact(consumer, max, lambda v: 'javascript' in v.get('content-type', ''), verbose=verbose):
       yield JavascriptArtefact(**r)

def main(args, consumer=None, producer=None, db=None, cache=None):
   global terminate
   if args.v:
       print(args)
   if consumer is None:
       consumer = KafkaConsumer(args.consume_from, 
                            bootstrap_servers=args.bootstrap, 
                            group_id=args.group, 
                            auto_offset_reset=args.start,
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                            max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
   if producer is None:
       producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, separators=(',', ':')).encode('utf-8'), 
                            bootstrap_servers=args.bootstrap)
   if db is None:
       mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
       db = mongo[args.dbname]
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
   # NB: we batch process to observe how long each batch takes and cache performance at regular intervals
   for b in batch(filter(lambda a: not a.url in cache, 
                             iterate(consumer, args.n, verbose=args.v)), n=1000):

       for jsr in sorted(b, key=lambda jsr: jsr.sha256):
           # eg.  {'url': 'https://XXXX.asn.au/', 'size_bytes': 294, 'inline': True, 'content-type': 'text/html; charset=UTF-8', 
           #       'when': '2020-02-06 02:51:46.016314', 'sha256': 'c38bd5db9472fa920517c48dc9ca7c556204af4dee76951c79fec645f5a9283a', 
           #        'md5': '4714b9a46307758a7272ecc666bc88a7', 'origin': 'XXXX' }  NB: origin may be none for old records (sadly)
           assert isinstance(jsr, JavascriptArtefact)
           cache[jsr.url] = 1

           # 1. verbose?
           if args.v:
               print(jsr)

           # 2. got results cache hit ??? Saves computing it again and hitting the DB, which is slow...
           key = '-'.join([jsr.sha256, jsr.md5, str(jsr.size_bytes)])  # a cache hit has both hash matches AND byte size the same. Unlikely to be false positive!
           if fv_cache is not None and key in fv_cache:
               tmp, js_id = fv_cache[key]
               n_cached += 1
               # need to update some fields as the cached copy is not the same...
               results = tmp.copy()
               results.update({ 'url': jsr.url, 'origin': jsr.origin })
               # falsely "update" the cache to ensure it is rewarded for being hit ie. becomes MRU
               fv_cache[key] = (tmp, js_id)
               # FALLTHRU
           else:
               # 3. obtain and analyse the JS from MongoDB and add to list of analysed artefacts topic. On failure lodge to feature extraction failure topic
              js, js_id = get_script(db, jsr)
              if not js:
                  report_failure(producer, jsr, 'Could not locate in MongoDB')
                  n_failed += 1
                  continue

              results, failed, stderr = analyse_script(js, jsr, java=args.java, feature_extractor=args.extractor)
              n_analysed += 1
              if failed:
                  report_failure(producer, jsr, "Unable to analyse script: {}".format(stderr))
                  n_failed += 1
                  continue
              # put results into the cache and then FALLTHRU...
              if fv_cache is not None:
                  fv_cache[key] = (results, js_id)

           save_ast_vector(db, jsr, results.get('statements_by_count'), js_id=js_id)
           save_call_vector(db, jsr, results.get('calls_by_count'), js_id=js_id) 
           # NB: dont save literal vector to mongo atm, kafka only
           results.update({ 'js_id': js_id })
           save_to_kafka(producer, results, to=args.to)

       if terminate:
           break # NOT REACHED

   print("Analysed {} artefacts, {} failed, {} cached, now={}".format(n_analysed, n_failed, n_cached, str(datetime.now())))
   cleanup([mongo, consumer])
   return 0

if __name__ == "__main__":
   save_pidfile('pid.make.fv')
   setup_signals(signal_handler)
   status = main(a.parse_args())
   exit(status)
