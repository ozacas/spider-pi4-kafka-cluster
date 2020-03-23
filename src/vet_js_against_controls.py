#!/usr/bin/python3
import pymongo
import json
import argparse
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control
from dataclasses import asdict

a = argparse.ArgumentParser(description="Read analysis results feature vector topic and closely related control vector (if any)")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=float('Inf'))
a.add_argument("--topic", help="Read analysis results from specified topic [analysis-results]", type=str, default="analysis-results") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [vet-features]", type=str, default='vet-features')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [earliest]", type=str, default='earliest')
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--to", help="Save results to named topic [javascript-artefact-control-results]", type=str, default="javascript-artefact-control-results")
args = a.parse_args()

group = args.group
if len(group) < 1:
    group = None
consumer = KafkaConsumer(args.topic, group_id=group, auto_offset_reset=args.start, consumer_timeout_ms=10000,
                         bootstrap_servers=args.bootstrap, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
mongo = pymongo.MongoClient(args.db, args.port)
db = mongo[args.dbname]

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Ctrl-C pressed. Cleaning up...")
    consumer.close()
    mongo.close()
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
# 0. read controls once only
controls = list(db.javascript_controls.find())

n = 0
for message in consumer:
    best_control = find_best_control(message.value, controls, db=db)
    if args.v:
        print(best_control)
    n += 1
    d = asdict(best_control)
    producer.send(args.to, d)
    db.vet_against_control.find_one_and_update({ 'origin_url': best_control.origin_url }, { "$set": d}, upsert=True)

    if n >= args.n:
       if args.v:
           print("Processed {} records.".format(n))
       break

cleanup()
