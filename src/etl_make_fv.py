#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from dataclasses import dataclass, asdict
import os
import json
import pymongo
import argparse
import pylru
import sys
from utils.features import analyse_script, get_script, safe_for_mongo
from datetime import datetime
from utils.models import JavascriptArtefact, Password
from utils.misc import rm_pidfile, save_pidfile, setup_signals

a = argparse.ArgumentParser(description="Extract features from each javascript in visited topic and dump into analysis-results topic")
a.add_argument("--mongo-host", help="Hostname/IP with mongo instance [pi1]", type=str, default="pi1")
a.add_argument("--mongo-port", help="TCP/IP port for mongo instance [27017]", type=int, default=27017)
a.add_argument("--db", help="Mongo database to populate with JS data [au_js]", type=str, default="au_js")
a.add_argument("--user", help="Database user to read artefacts from (read-write access required)", type=str, required=True)
a.add_argument("--password", help="Password (prompted if not specified)", type=Password, default=Password.DEFAULT)
a.add_argument("--topic", help="Kafka topic to get visited JS summary [visited]", type=str, default='visited')
a.add_argument("--to", help="Send output to specified topic [analysis-results]", type=str, default='analysis-results')
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=int, default=1000000000)
a.add_argument("--group", help="Use specified kafka consumer group to find correct topic position [javascript-analysis]", type=str, default='javascript-analysis')
a.add_argument("--start", help="Start at earliest|latest available message [earliest]", type=str, default='earliest')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--java", help="Java client used to run the program", type=str, default="/usr/bin/java")
a.add_argument("--extractor", help="JAR file to perform the feature calculation for each JS artefact", type=str, default="/home/acas/src/extract-features.jar")
args = a.parse_args()

consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, group_id=args.group, auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, separators=(',', ':')).encode('utf-8'), bootstrap_servers=args.bootstrap)
mongo = pymongo.MongoClient(args.mongo_host, args.mongo_port, username=args.user, password=str(args.password))
db = mongo[args.db]

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

def next_artefact(consumer, max):
    cnt = 0
    for msg in consumer:
        d = msg.value
        d['content_type'] = d['content-type']
        if 'javascript' not in d['content_type']:
            continue

        del d['content-type']
        yield JavascriptArtefact(**d)
        # done enough per user request?
        cnt += 1
        if cnt >= max:
            break

def report_failure(producer, artefact, reason):
    d = asdict(artefact)
    d['reason'] = reason if len(reason) < 300 else "{}...".format(reason[0:300])
    producer.send('feature-extraction-failures', d)

def save_ast_vector(db, jsr, ast_vector):
   d = asdict(jsr)
   d.update(**ast_vector)  # ast_vector never needs to be made safe for Mongo, since its just mozilla rhino statement types for keys
   d.update({ "js_id": js_id })
   assert '_id' not in d.keys()
   db.statements_by_count.insert_one(d)

def save_call_vector(db, jsr, call_vector):
   d = asdict(jsr)
   calls = safe_for_mongo(results.get('calls_by_count'))
   d.update(calls)
   d.update({ "js_id": js_id })
   assert '_id' not in d.keys()
   db.count_by_function.insert_one(d)

def save_to_kafka(producer, results):
   # and now kafka now that the DB has been populated
   results.update({ "js_id": js_id })
   assert '_id' not in results.keys()
   producer.send('analysis-results', results)

# we want only artefacts which are not cached and are JS (subject to maximum record limits)
save_pidfile('pid.make.fv')
uncached_js_artefacts = filter(lambda a: not a.url in cache, next_artefact(consumer, args.n))

for jsr in uncached_js_artefacts:
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
             save_ast_vector(db, jsr, results.get('statements_by_count'))
             save_call_vector(db, jsr, results.get('calls_by_count')) 
             # NB: dont save literal vector to mongo atm, kafka only
             save_to_kafka(producer, results)
         else:
             report_failure(producer, jsr, "Unable to analyse script: {}".format(stderr))
    else:
         report_failure(producer, jsr, 'Could not locate in MongoDB')

cleanup()
exit(0)
