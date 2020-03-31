#!/usr/bin/python3
import sys
import json
import argparse
import signal
import pymongo
from kafka import KafkaConsumer
from utils.models import BestControl, Password
from dataclasses import asdict
from urllib.parse import urlparse

a = argparse.ArgumentParser(description="Peform ETL on JS control hits and dump non-normalised data into MongoDB ready for querying")
a.add_argument("--topic", help="Kafka topic to get visited JS summary [javascript-artefact-control-results]", type=str, default='javascript-artefact-control-results')
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [etl-controls2mongo]", type=str, default='etl-controls2mongo')
a.add_argument("--start", help="Consume from earliest|latest message available in control results topic [latest]", type=str, default='latest')
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=int, default=1000000000)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="MongoDB RBAC username to use (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="MongoDB password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
a.add_argument("--threshold", help="Ignore control hits with ast_distance greater than this", type=float, default=50.0)
a.add_argument("--v", help="Debug verbosely", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.user, password=str(args.password))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=1000,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def cleanup(*args):
    global consumer
    if len(args):
        print("Ctrl+C pressed. Now cleaning up and terminating...")
    else:
        print("Finished analysis. Shutting down...")
    consumer.close()
    sys.exit(0)

def get_function_call_vector(db, url):
    ret = db.count_by_function.find_one({ 'url': url })
    return ret

def next_artefact(consumer, max, verbose):
    global origins
    n = 0
    for message in consumer:
        rec = BestControl(**message.value)
        yield rec 
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records. Got data for {} origin hosts.".format(n, len(origins)))
        if n >= max:
            break

def load_controls(db, verbose):
    controls = {}
    for control in db.javascript_controls.find({}):
        #print(control)
        url = control.get('origin')
        controls[url] = control
    if args.v:
        print("Loaded {} controls.".format(len(controls)))
    return controls

signal.signal(signal.SIGINT, cleanup)
origins = { }
controls = load_controls(db, args.v)
for best_control in filter(lambda c: c.ast_dist < args.threshold, next_artefact(consumer, args.n, args.v)):
    dist = best_control.ast_dist

    up = urlparse(best_control.origin_url)
    host = up.hostname
    if host is None:
       continue
    origins[host] = 1
    d = asdict(best_control)
    d.pop('diff_functions', None)
    fv_origin = get_function_call_vector(db, best_control.origin_url)
    if fv_origin is None:
        if args.v:
            print("WARNING: unable to find function call vector for {}".format(best_control.origin_url))
        continue
    else:
        if args.v:
            print("Got function call vector for {}".format(best_control.origin_url))
        # FALLTHRU

    u = best_control.control_url
    fv_control = controls[u].get('calls_by_count')
    for fn in best_control.diff_functions.split(' '):
        if len(fn) > 0:
            d['diff_function'] = fn
            d['origin_host'] = host
            d['origin_has_query'] = len(up.query) > 0
            if up.port:
                 d['origin_port'] = up.port
            elif up.scheme == 'https':
                 d['origin_port'] = 443
            elif up.scheme == 'http':
                 d['origin_port'] = 80
            d['origin_scheme'] = up.scheme
            d['control_family'] = controls[u].get('family')
            d['expected_calls'] = fv_control.get(fn, None)
            d['actual_calls'] = fv_origin.get(fn, None)
            db.etl_javascript_controls.insert_one(d.copy())

cleanup()
