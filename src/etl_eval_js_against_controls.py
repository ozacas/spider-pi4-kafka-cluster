#!/usr/bin/python3
import os
import pymongo
import json
import argparse
import sys
import hashlib
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control, analyse_script
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from utils.misc import *
from dataclasses import asdict

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
add_kafka_arguments(a, 
                    consumer=True,
                    producer=True, # and save to a topic
                    default_from='analysis-results',
                    default_group='vet-features', 
                    default_to="javascript-artefact-control-results")
add_mongo_arguments(a, default_access="read-write")
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--file", help="Debug specified file and exit []", type=str, default=None)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
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
consumer = KafkaConsumer(args.consume_from, group_id=group, auto_offset_reset=args.start, 
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
