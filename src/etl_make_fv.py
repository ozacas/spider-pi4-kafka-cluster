#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from dataclasses import asdict
import os
import json
import pymongo
import argparse
import pylru
import sys
from utils.features import analyse_script, get_script
from datetime import datetime
from utils.io import save_call_vector, save_ast_vector, next_artefact
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
args = a.parse_args()

consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, separators=(',', ':')).encode('utf-8'), bootstrap_servers=args.bootstrap)
mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Ctrl-C pressed. Cleaning up...")
    consumer.close()
    mongo.close()
    rm_pidfile('pid.make.fv')
    sys.exit(0)

cnt = 0    
cache = pylru.lrucache(1000)
setup_signals(cleanup)

if not os.path.exists(args.java):
    raise ValueError("Java executable does not exist: {}".format(args.java))
if not os.path.exists(args.extractor):
    raise ValueError("JAR file to extract features does not exist: {}".format(args.extractor))

def report_failure(producer, artefact, reason):
    d = asdict(artefact)
    d['reason'] = reason if len(reason) < 300 else "{}...".format(reason[0:300])
    producer.send('feature-extraction-failures', d)

def save_to_kafka(producer, results):
   # and now kafka now that the DB has been populated
   results.update({ "js_id": js_id })
   assert '_id' not in results.keys()
   producer.send('analysis-results', results)

# we want only artefacts which are not cached and are JS (subject to maximum record limits)
save_pidfile('pid.make.fv')
artefacts = [JavascriptArtefact(**r) for r in next_artefact(consumer, args.n, verbose=args.v, 
                   filter_cb=lambda m: 'javascript' in m['content_type'])]

for jsr in filter(lambda a: not a.url in cache, artefacts):
    # eg.  {'url': 'https://XXXX.asn.au/', 'size_bytes': 294, 'inline': True, 'content-type': 'text/html; charset=UTF-8', 
    #       'when': '2020-02-06 02:51:46.016314', 'sha256': 'c38bd5db9472fa920517c48dc9ca7c556204af4dee76951c79fec645f5a9283a', 
    #        'md5': '4714b9a46307758a7272ecc666bc88a7', 'origin': 'XXXX' }  NB: origin may be none for old records (sadly)
    cache[jsr.url] = 1

    # 1. verbose?
    if args.v:
        print(jsr)

    # 2. obtain and analyse the JS from MongoDB and add to list of analysed artefacts topic. On failure lodge to feature extraction failure topic
    (js, js_id) = get_script(db, jsr)
    if js:
         results, failed, stderr = analyse_script(js, jsr, java=args.java, feature_extractor=args.extractor)
         if not failed:
             save_ast_vector(db, jsr, results.get('statements_by_count'), js_id=js_id)
             save_call_vector(db, jsr, results.get('calls_by_count'), js_id=js_id) 
             # NB: dont save literal vector to mongo atm, kafka only
             save_to_kafka(producer, results)
         else:
             report_failure(producer, jsr, "Unable to analyse script: {}".format(stderr))
    else:
         report_failure(producer, jsr, 'Could not locate in MongoDB')

cleanup()
exit(0)
