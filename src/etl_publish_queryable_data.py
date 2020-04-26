#!/usr/bin/python3
import os
import sys
import json
import argparse
import pymongo
from kafka import KafkaConsumer, KafkaProducer
from utils.io import get_function_call_vector, next_artefact
from utils.models import BestControl, Password
from utils.misc import *
from utils.features import as_url_fields
from dataclasses import asdict

a = argparse.ArgumentParser(description="Reconcile all data from control, origin and artefacts into a query-ready collection")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='javascript-artefact-control-results',
                    default_group='etl-hits',
                    default_to='etl-good-hits')
add_mongo_arguments(a, default_access="read-write")
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--threshold", help="Ignore hits with ast_distance greater than this [50.0]", type=float, default=50.0)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=10000,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Now cleaning up and terminating... please wait.")
    else:
        print("Finished analysis. Shutting down...")
    consumer.close()
    mongo.close()
    rm_pidfile('pid.etl.hits')
    sys.exit(0)

def load_controls(db, verbose):
    controls = {}
    for control in db.javascript_controls.find({}, { 'literals_by_count': False }):
        #print(control)
        url = control.get('origin')
        controls[url] = control
    if args.v:
        print("Loaded {} controls.".format(len(controls)))
    return controls

def iterate(consumer, max, verbose, threshold):
   for r in next_artefact(consumer, max, lambda v: v['ast_dist'] <= threshold, verbose=verbose):
       yield BestControl(**r)
 
setup_signals(cleanup)
origins = { }
n_unable = n_ok = 0
save_pidfile('pid.etl.hits')
controls = load_controls(db, args.v)
for best_control in iterate(consumer, args.n, args.v, args.threshold):
    dist = best_control.ast_dist

    origin_fields = as_url_fields(best_control.origin_url, prefix='origin')
    host = origin_fields.get('origin_host')
    if host is None:
       continue
    origins[host] = 1
    d = asdict(best_control)
    d.pop('diff_functions', None)
    fv_origin = get_function_call_vector(db, best_control.origin_url)
    if fv_origin is None:
        n_unable += 1 
        continue
    else:
        n_ok += 1
        # FALLTHRU

    u = best_control.control_url
    fv_control = controls[u].get('calls_by_count')
    d.update(origin_fields)

    # cited_on URL (aka. HTML page) iff specified
    d.update(as_url_fields(best_control.cited_on, prefix='cited_on'))
    d['control_family'] = controls[u].get('family')

    # good hits get sent to the suspicious analysis pipeline
    diff_fns_list = best_control.diff_functions.split(' ')
    if (dist < 10.0 or (dist < 20.0 and len(diff_fns_list) < 10)) and (dist > 0.0 and best_control.function_dist > 0.0):
        dc = d.copy()
        dc.pop('_id', None)
        dc['diff_functions'] = best_control.diff_functions
        producer.send('etl-good-hits', dc)

    # finally report each differentially called function as a separate record 
    for fn in diff_fns_list:
        if len(fn) > 0:
            d['diff_function'] = fn

            # other fields for ETL 
            d['expected_calls'] = fv_control.get(fn, None)
            d['actual_calls'] = fv_origin.get(fn, None)
            dc = d.copy()
            db.etl_hits.insert_one(dc)
             

if args.v:
    print("Unable to retrieve FV for {} URLs".format(n_unable))
    print("Found {} FV's without problem".format(n_ok))
cleanup()
