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
                    default_group='etl-publish-hits',
                    default_to='etl-good-hits')
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--threshold", help="Ignore hits with ast_distance greater than this [50.0]", type=float, default=50.0)
a.add_argument("--tail", help="Dont terminate if we've read all the messages. Wait for new ones", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]
timeout = float('Inf') if args.tail else 10000
consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=timeout,
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
       try:
          yield BestControl(**r)
       except TypeError:
          # BUGFIX coming from input topic data: assume r['origin_js_id'] was persisted (in error) as a tuple or list...
          r['origin_js_id'] = r['origin_js_id'][0]
          yield BestControl(**r)
 
setup_signals(cleanup)
origins = { }
n_unable = n_ok = 0
save_pidfile('pid.etl.hits')
controls = load_controls(db, args.v)
for hit in iterate(consumer, args.n, args.v, args.threshold):
    dist = hit.ast_dist
    assert dist >= 0.0
    assert hit.xref is not None and len(hit.xref) > 0

    origin_fields = as_url_fields(hit.origin_url, prefix='origin')
    host = origin_fields.get('origin_host')
    if host is None:
       continue
    origins[host] = 1
    d = asdict(hit)
    d.pop('diff_functions', None)
    fv_origin = get_function_call_vector(db, hit.origin_url)
    if fv_origin is None:
        n_unable += 1 
        continue
    else:
        n_ok += 1
        # FALLTHRU

    u = hit.control_url
    if not u in controls:
        continue   # control no longer in database? ok, skip further work

    fv_control = controls[u].get('calls_by_count')
    d.update(origin_fields)

    # cited_on URL (aka. HTML page) iff specified
    d.update(as_url_fields(hit.cited_on, prefix='cited_on'))
    d['control_family'] = controls[u].get('family')

    # good hits get sent to the suspicious analysis pipeline
    if hit.is_good_hit():
        dc = d.copy()
        dc.pop('_id', None)
        dc['diff_functions'] = hit.diff_functions_as_list()
        producer.send('etl-good-hits', dc)

        # finally report each differentially called function as a separate record 
        for fn in filter(lambda fn: len(fn) > 0, dc['diff_functions']):
            d['diff_function'] = fn
            d['expected_calls'] = fv_control.get(fn, None)
            d['actual_calls'] = fv_origin.get(fn, None)
            dc = d.copy()  # NB: dont give the same instance to pymongo each call
            assert 'xref' in dc.keys()
            db.etl_hits.insert_one(dc)
             

if args.v:
    print("Unable to retrieve FV for {} URLs".format(n_unable))
    print("Found {} FV's without problem".format(n_ok))
cleanup()