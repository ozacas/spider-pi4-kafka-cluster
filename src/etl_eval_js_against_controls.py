#!/usr/bin/python3
import os
import pymongo
import json
import argparse
import sys
import hashlib
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control, analyse_script
from utils.models import JavascriptArtefact, Password, JavascriptVectorSummary
from utils.misc import *
from dataclasses import asdict

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=float('Inf'))
a.add_argument("--topic", help="Read analysis results from specified topic [analysis-results]", type=str, default="analysis-results") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [vet-features]", type=str, default='vet-features')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [earliest]", type=str, default='earliest')
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--file", help="Debug specified file and exit []", type=str, default=None)
a.add_argument("--user", help="Specify MongoDB user with read/write access to dbname", type=str, required=True)
a.add_argument("--password", help="Specify password for user (prompted if required)", type=Password, default=Password.DEFAULT)
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--to", help="Save results to named topic [javascript-artefact-control-results]", type=str, default="javascript-artefact-control-results")
a.add_argument("--extractor", help="JAR file to extract features as JSON [extract-features.jar]", type=str, default="/home/acas/src/extract-features.jar")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Ctrl-C pressed. Cleaning up...")
    consumer.close()
    mongo.close()
    rm_pidfile('pid.eval.controls')
    sys.exit(0)


# 0. read controls once only (TODO FIXME: assumption is that the vectors fit entirely in memory)
controls = list(db.javascript_controls.find({}, { 'literals_by_count': False }))
print("Loaded {} AST control vectors from MongoDB".format(len(controls)))
# 0b. read magnitude of each vector so that we have the ability to reduce the number of comparisons performed to realistic controls
control_magnitudes = []
for d in list(db.javascript_controls_summary.find({}, { '_id': False })):
    control_magnitudes.append(JavascriptVectorSummary(**d)) 

if args.v:
   print("Reporting unique families with JS controls (please wait this may take some time):")
   print(db.javascript_controls.distinct('family'))

if args.file:
   with open(args.file, 'rb') as fp:
       content = fp.read()
       jsr = JavascriptArtefact(url=args.file, sha256=hashlib.sha256(content).hexdigest(), 
                                md5=hashlib.md5(content).hexdigest(), size_bytes=len(content))
       input_features, failed, stderr = analyse_script(content, jsr, feature_extractor=args.extractor)
       if failed:
           print("Unable to extract features... aborting.")
           print(stderr)
           cleanup()
       best_control, next_best_control = find_best_control(input_features, controls, db=db, debug=True, control_index=control_magnitudes) # index None so that all are searched
       print("*** WINNING CONTROL HIT")
       print(best_control)
       print("*** NEXT BEST CONTROL HIT (diff_functions and function dist not available)")
       print(next_best_control)
       cleanup()

group = args.group
if len(group) < 1:
    group = None
consumer = KafkaConsumer(args.topic, group_id=group, auto_offset_reset=args.start, 
                         bootstrap_servers=args.bootstrap, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
setup_signals(cleanup)

# 1. process the analysis results topic to get vectors for each javascript artefact which has been processed by 1) kafkaspider AND 2) etl_make_fv
n = 0
save_pidfile('pid.eval.controls')
for message in consumer:
    best_control, next_best_control = find_best_control(message.value, controls, db=db, control_index=control_magnitudes)
    n += 1

    if args.v:
        print(best_control)

    d = asdict(best_control) # NB: all fields of the model are sent to output kafka topic and Mongo

    # 2a. send results to kafka topic for streaming applications
    producer.send(args.to, d) 

    # 2b. also send results to MongoDB for batch-oriented applications and for long-term storage
    db.vet_against_control.find_one_and_update({ 'origin_url': best_control.origin_url }, { "$set": d}, upsert=True)

    if n >= args.n:
       if args.v:
           print("Processed {} records.".format(n))
       break

cleanup()
