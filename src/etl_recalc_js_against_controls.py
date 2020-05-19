#!/usr/bin/python3
import os
import pymongo
import json
import argparse
import sys
import hashlib
import pylru
from bson.objectid import ObjectId
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control, analyse_script, calculate_ast_vector
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from utils.io import next_artefact, load_controls
from utils.misc import *
from dataclasses import asdict

a = argparse.ArgumentParser(description="Recalculate existing hits against against all controls, updating results in MongoDB")
add_kafka_arguments(a, consumer=False, producer=True, default_to='javascript-artefact-control-results')
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_debug_arguments(a)
add_extractor_arguments(a)
a.add_argument('--control', help='Update all hits for specified control URL [None]', type=str, required=True)
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
    rm_pidfile('pid.recalc.controls')
    sys.exit(0)


all_controls = load_controls(db)
save_pidfile('pid.recalc.controls')
n_failed = n = 0
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)

for hit in db.vet_against_control.find({ "control_url": args.control }):
    ret = db.scripts.find_one({ '_id': ObjectId(hit.get('origin_js_id')) })
    content = ret.get('code') 
    jsr = JavascriptArtefact(url=hit.get('origin_url'), sha256=hashlib.sha256(content).hexdigest(), md5=hashlib.md5(content).hexdigest(), inline=False)
    m, failed, stderr = analyse_script(content, jsr, java=args.java, feature_extractor=args.extractor)
    if failed:
       n_failed += 1
       continue
    n += 1
    best_control, next_best_control = find_best_control(m, all_controls, db=db)
    d = asdict(best_control) # NB: all fields of the model are sent to output kafka topic and Mongo

    # 2a. also send results to MongoDB for batch-oriented applications and for long-term storage
    assert 'origin_url' in d and len(d['origin_url']) > 0
    assert isinstance(d['origin_js_id'], str) or d['origin_js_id'] is None
    ret = db.vet_against_control.find_one_and_update({ 'origin_url': best_control.origin_url }, 
                                                     { "$set": d}, 
                                                     upsert=True, 
                                                     return_document=pymongo.ReturnDocument.AFTER)

    # 2b. send results to kafka topic for streaming applications
    assert ret is not None and '_id' in ret
    xref = str(ret.get('_id'))
    assert xref is not None
    d['xref'] = xref
    best_control.xref = xref
    if next_best_control:
        next_best_control.xref = xref
    producer.send(args.to, d) 

    if args.v and len(best_control.control_url) > 0:  # only report hits in verbose mode, to make for easier investigation
        print(best_control)

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
        d['xref'] = xref
        producer.send(args.to, d)

print("Failed to analyse {} scripts, successfully processed {} hits".format(n_failed, n))
cleanup()
