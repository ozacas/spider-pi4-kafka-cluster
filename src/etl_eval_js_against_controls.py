#!/usr/bin/python3
import os
import pymongo
import json
import argparse
import sys
import hashlib
import pylru
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control, analyse_script, calculate_ast_vector
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from utils.io import next_artefact
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
    try:
        consumer.close()
        mongo.close()
    except NameError:
        pass # NameError occurs when using --file as consumer has not been setup since it is not required
    rm_pidfile('pid.eval.controls')
    sys.exit(0)


# 0. read controls once only (TODO FIXME: assumption is that the vectors fit entirely in memory)
all_controls = []
for control in db.javascript_controls.find({}, { 'literals_by_count': False }): # dont load literal vector to save considerable memory
    ast_vector, ast_sum = calculate_ast_vector(control['statements_by_count'])
    tuple = (control, ast_sum, ast_vector)
#    print(tuple)
    all_controls.append(tuple)

print("Loaded {} AST control vectors from MongoDB".format(len(all_controls)))

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
       best_control, next_best_control = find_best_control(input_features, all_controls, db=db, max_distance=1000.0, debug=True) 
       print("*** WINNING CONTROL HIT")
       print(best_control)
       print("*** NEXT BEST CONTROL HIT")
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
save_pidfile('pid.eval.controls')
for m in next_artefact(consumer, args.n, filter_cb=None, verbose=args.v):
    best_control, next_best_control = find_best_control(m, all_controls, db=db)

    if args.v:
        print(best_control)

    d = asdict(best_control) # NB: all fields of the model are sent to output kafka topic and Mongo

    # 2a. send results to kafka topic for streaming applications
    producer.send(args.to, d) 

    # 2b. also send results to MongoDB for batch-oriented applications and for long-term storage
    assert 'origin_url' in d and len(d['origin_url']) > 0
    assert isinstance(d['origin_js_id'], str) or d['origin_js_id'] is None
    db.vet_against_control.find_one_and_update({ 'origin_url': best_control.origin_url }, { "$set": d}, upsert=True)

    # 3. finally if the next_best_control looks just as good (or better than) the best control then we ALSO report it...
    if next_best_control is None:
        continue

    best_mult = best_control.dist_prod()
    next_best_mult = next_best_control.dist_prod()
    if next_best_mult <= best_mult and next_best_mult < 50.0: # only report good hits though... otherwise poor hits will generate lots of false positives
        print("NOTE: next best control looks as good as best control")
        print(next_best_control) 
        print(best_control)
        d = asdict(next_best_control)
        producer.send(args.to, d)

cleanup()
