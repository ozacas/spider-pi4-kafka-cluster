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
from utils.features import find_best_control, analyse_script, calculate_ast_vector, calculate_literal_distance, fix_literals, find_feasible_controls
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
a.add_argument("--max-distance", help="Limit control hits to those with AST*Call distance < [150.0]", type=float, default=150.0)
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
for t in load_controls(db, all_vectors=True, min_size=args.min_size, verbose=args.v):
   assert isinstance(t, tuple)
   rec, ast_sum, ast_vector, call_vector, _ = t
   assert isinstance(rec, dict)
   all_controls.append((rec, ast_sum, ast_vector, call_vector ))

print("Loaded {} javascript controls.".format(len(all_controls)))

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
       best_control, next_best_control = find_best_control(json.loads(byte_content.decode()), all_controls,
                                                           db=db, max_distance=args.max_distance, debug=True) 
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
    assert 'control_url' in d  # control url may be empty for poor quality (or no) hits
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

def get_control_bytes(db, control_url: str):
    doc = db.javascript_control_code.find_one({ 'origin': control_url })
    if doc is None:  # should not happen during the normal course of events... but if it does we handle it gracefully
        return None
    assert doc is not None
    assert 'analysis_bytes' in doc
    assert 'analysis_vectors_sha256' in doc
    assert hashlib.sha256(doc['analysis_bytes']).hexdigest() == doc['analysis_vectors_sha256']
    return doc['analysis_bytes']

def update_literal_distance(db, hit: BestControl, ovec, fail_if_difference=False):
    assert hit is not None
    assert ovec is not None
    cvec = get_control_bytes(db, hit.control_url)
    if cvec is None:
         hit.literal_dist = -1.0
         hit.literals_not_in_origin = -1
         hit.literals_not_in_control = -1
         hit.n_diff_literals = -1
         hit.diff_literals = ''
         return
    t = calculate_literal_distance(json.loads(cvec).get('literals_by_count'), ovec, fail_if_difference=fail_if_difference)
    hit.literal_dist, hit.literals_not_in_origin, hit.literals_not_in_control, diff_literals = t
    hit.n_diff_literals = len(diff_literals)
    hit.diff_literals = fix_literals(diff_literals) 
    if hit.sha256_matched:
        # still broken - unknown cause
        #if hit.distance() > 0: 
        #    print(hit)
        #    assert hit.distance() < 0.1
        #    assert hit.n_diff_literals == 0
        #    assert hit.literals_not_in_origin == 0
        #    assert hit.literals_not_in_control == 0
        pass
    
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
       # 1. check byte vector hashes match
       actual_sha256 =  hashlib.sha256(byte_content).hexdigest() 
       if args.v:
           print("byte_vectors: ", actual_sha256, " ", required_hash, " origin js_id:", js_id)
       assert actual_sha256 == required_hash
    d = json.loads(byte_content)
    for t in ['statements_by_count', 'calls_by_count', 'literals_by_count']:
        m[t] = d[t]
    # example m: {'url': 'https://homes.mirvac.com/-/media/Project/Mirvac/Residential/Base-Residential-Site/Base-Residential-Site-Theme/scripts/navigationmin.js', 
    #             'sha256': 'd6941fcfdd12069e20f4bb880ecbab12d797d9696cae1b05ec9d59fb9bd90b51', 'md5': '2b10377115ab0747535acb1ad38b26bd', 
    #             'inline': False, 'content_type': 'text/javascript', 'when': '2020-06-04 03:28:28.483855', 'size_bytes': 14951, 
    #             'origin': 'https://homes.mirvac.com/homes-portfolio',  'js_id': '5e8ef7df582045cdd24ce8ae', 
    #             'byte_content_sha256': 'b2b45feee3497bee1e575eb56c50a84ec7651dbc160e8b1607b07153563d804c'}
    best_control, next_best_control = find_best_control(m, all_controls, db=db, control_cache=pylru.lrucache(200), max_distance=args.max_distance)
    ovec = m['literals_by_count']
    if best_control is None or len(best_control.control_url) == 0:
        pass 
    else:
        # 2. check that sha256's match each artefact iff find_best_control() said the hashes matched (since next_best_control had some distance it cannot be a hash match)
        if args.defensive and best_control.sha256_matched:
            cntl_doc = db.javascript_controls.find_one({ 'origin': best_control.control_url })
            assert cntl_doc is not None
            if args.v:
                print("artefact hash: ", m['sha256'], m['md5'], " control_hashes: ", cntl_doc.get('sha256'), cntl_doc.get('md5'))
            assert cntl_doc.get('sha256') == m['sha256']
        update_literal_distance(db, best_control, ovec, fail_if_difference=args.defensive and best_control.sha256_matched)
    d = save_vetting(db, best_control, required_hash)
    best_control.xref = d['xref']
    assert best_control.xref is not None

    producer.send(args.to, d) 
    if args.v and len(best_control.control_url) > 0:  # only report hits in verbose mode, to make for easier investigation
        print(best_control)

    if next_best_control is not None and len(next_best_control.control_url) > 0:
        assert not next_best_control.sha256_matched  # if must not be true since it would have been the best_control if it were
        update_literal_distance(db, next_best_control, ovec)
        d2 = save_vetting(db, next_best_control, required_hash)
        next_best_control.xref = d2['xref']
        assert len(next_best_control.xref) > 0
        if next_best_control.literal_dist < 0.0: # if the literal distance calculation fails (eg. missing control features) we ignore the hit
            continue

        best_mult = best_control.distance()
        next_best_mult = next_best_control.distance()
        if next_best_mult <= best_mult and next_best_mult < 50.0: # only report good hits though... otherwise poor hits will generate lots of false positives
            print("NOTE: next best control looks as good as best control")
            print(next_best_control) 
            producer.send(args.to, d2)
            assert d2['xref'] != d['xref']  # xref must not be the same document otherwise something has gone wrong 

cleanup()
