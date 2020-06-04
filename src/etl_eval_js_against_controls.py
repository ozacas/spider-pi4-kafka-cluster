#!/usr/bin/python3
import os
import pymongo
from bson.objectid import ObjectId
import json
import argparse
import sys
import hashlib
import pylru
from kafka import KafkaConsumer, KafkaProducer
from utils.features import find_best_control, analyse_script, calculate_ast_vector, validate_feature_vectors
from utils.models import JavascriptArtefact, JavascriptVectorSummary, BestControl
from utils.io import next_artefact, load_controls
from utils.misc import *
from dataclasses import asdict

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
add_kafka_arguments(a, 
                    consumer=True,
                    producer=True, # and save to a topic
                    default_from='analysis-results',
                    default_group='etl-eval-js-against-controls', 
                    default_to="javascript-artefact-control-results")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--file", help="Debug specified file and exit []", type=str, default=None)
a.add_argument("--min-size", help="Skip all controls less than X bytes [1500]", type=int, default=1500)
a.add_argument("--defensive", help="Validate each message from kafka for corruption (slow)", action="store_true")
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
all_controls = load_controls(db, min_size=args.min_size, literals_by_count=False, verbose=args.v)

if args.v:
   print("Reporting unique families with JS controls (please wait this may take some time):")
   print(db.javascript_controls.distinct('family'))

if args.file:
   with open(args.file, 'rb') as fp:
       content = fp.read()
       jsr = JavascriptArtefact(url=args.file, sha256=hashlib.sha256(content).hexdigest(), 
                                md5=hashlib.md5(content).hexdigest(), size_bytes=len(content))
       byte_content, failed, stderr = analyse_script(content, jsr, feature_extractor=args.extractor)
       if failed:
           print("Unable to extract features... aborting.")
           print(stderr)
           cleanup()
       best_control, next_best_control = find_best_control(json.loads(byte_content.decode()), all_controls, db=db, max_distance=1000.0, debug=True) 
       print("*** WINNING CONTROL HIT")
       print(best_control)
       print("*** NEXT BEST CONTROL HIT")
       print(next_best_control)
       cleanup()

group = args.group
if len(group) < 1:
    group = None
consumer = KafkaConsumer(args.consume_from, group_id=group, auto_offset_reset=args.start, 
                         bootstrap_servers=args.bootstrap, value_deserializer=json_value_deserializer())
producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)
setup_signals(cleanup)

def save_vetting(db, hit: BestControl, origin_byte_content_sha256: str ):
    d = asdict(hit)
    assert 'cited_on' in d and len(d['cited_on']) > 0
    assert 'control_url' in d and len(d['control_url']) > 0
    assert 'origin_url' in d and len(d['origin_url']) > 0
    assert isinstance(d['origin_js_id'], str) or d['origin_js_id'] is None
    assert len(origin_byte_content_sha256) > 32
    d['origin_vectors_sha256'] = origin_byte_content_sha256

    control_url = hit.control_url
    origin_url = hit.origin_url
    ret = db.vet_against_control.find_one_and_update({ 'origin_url': origin_url, 'control_url': control_url }, 
                                                     { "$set": d}, 
                                                     upsert=True, 
                                                     return_document=pymongo.ReturnDocument.AFTER)
    assert ret is not None and '_id' in ret
    xref = str(ret.get('_id'))
    assert xref is not None
    d['xref'] = xref
    return d

# 1. process the analysis results topic to get vectors for each javascript artefact which has been processed by 1) kafkaspider AND 2) etl_make_fv
save_pidfile('pid.eval.controls')
print("Creating required index in vet_against_control collection... please wait")
db.vet_against_control.create_index([( 'origin_url', pymongo.ASCENDING), ('control_url', pymongo.ASCENDING )], unique=True)
print("Index creation complete.")

for m in next_artefact(consumer, args.n, filter_cb=lambda m: m.get('size_bytes') >= 1500, verbose=args.v):
    js_id = m.get('js_id')
    if not 'byte_content_sha256' in m:
        continue
    required_hash = m.get('byte_content_sha256')
    byte_content_doc = db.analysis_content.find_one({ 'js_id': js_id, 'byte_content_sha256': required_hash })
    if byte_content_doc is None:
        print("WARNING: analysis data could not be found: {}".format(m))
        continue
    assert 'analysis_bytes' in byte_content_doc
    byte_content = byte_content_doc.get('analysis_bytes')
    assert isinstance(byte_content, bytes)
    if args.defensive:
       actual_sha256 =  hashlib.sha256(byte_content).hexdigest() 
       #print(actual_sha256, " ", required_hash)
       assert actual_sha256 == required_hash
    m.update(json.loads(byte_content))

    best_control, next_best_control = find_best_control(m, all_controls, db=db, control_cache=pylru.lrucache(200))
    if best_control is None or len(best_control.control_url) == 0:
        continue

    d = save_vetting(db, best_control, required_hash)
    best_control.xref = d['xref']
    assert best_control.xref is not None

    producer.send(args.to, d) 
    if args.v and len(best_control.control_url) > 0:  # only report hits in verbose mode, to make for easier investigation
        print(best_control)

    if next_best_control is not None and len(next_best_control.control_url) > 0:
        d2 = save_vetting(db, next_best_control, required_hash)
        next_best_control.xref = d2['xref']
        assert len(next_best_control.xref) > 0

        best_mult = best_control.dist_prod()
        next_best_mult = next_best_control.dist_prod()
        if next_best_mult <= best_mult and next_best_mult < 50.0: # only report good hits though... otherwise poor hits will generate lots of false positives
            print("NOTE: next best control looks as good as best control")
            print(next_best_control) 
            producer.send(args.to, d2)
            assert d2['xref'] != d['xref']  # xref must not be the same document otherwise something has gone wrong 

cleanup()
