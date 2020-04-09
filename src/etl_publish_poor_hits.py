#!/usr/bin/python3
import os
import sys
import json
import argparse
import pymongo
from kafka import KafkaConsumer
from utils.models import BestControl, Password
from utils.misc import setup_signals, rm_pidfile, save_pidfile
from utils.features import as_url_fields, find_sha256_hash
from dataclasses import asdict

a = argparse.ArgumentParser(description="Reconcile poor hits into non-normalised, Mongo collection")
a.add_argument("--topic", help="Kafka topic to get visited JS summary [javascript-artefact-control-results]", type=str, default='javascript-artefact-control-results')
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [etl-bad-hits]", type=str, default='etl-bad-hits')
a.add_argument("--start", help="Consume from earliest|latest message available in control results topic [latest]", type=str, default='earliest')
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=float, default=float('Inf'))
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="MongoDB RBAC username to use (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="MongoDB password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
a.add_argument("--threshold", help="Only report hits with ast_distance greater than this [50.0]", type=float, default=50.0)
a.add_argument("--v", help="Debug verbosely", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.user, password=str(args.password))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=10000,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Now cleaning up and terminating... please wait.")
    else:
        print("Finished analysis. Shutting down...")
    consumer.close()
    mongo.close()
    rm_pidfile('pid.etl.badhits')
    sys.exit(0)

def get_function_call_vector(db, url):
    ret = db.count_by_function.find_one({ 'url': url })
    return ret

def next_artefact(consumer, max, verbose):
    n = 0
    for message in consumer:
        rec = BestControl(**message.value)
        yield rec 
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records. ".format(n))
        if n >= max:
            break

setup_signals(cleanup)
n_unable = n_ok = 0
save_pidfile('pid.etl.badhits')
for bad_hit in filter(lambda c: c.ast_dist > args.threshold, next_artefact(consumer, args.n, args.v)):
    # bad hits are still useful:
    # 1) they may indicate a javascript family which must be added to the controls in the database
    # 2) they might suggest other ways which have to be handled by the system
    # so here we want to provide enough data (even for the bad hit) to enable build-data-matrix-from-etl.py to 
    # be able to efficiently query for clusters of bad hits
    d = { 'ast_dist': bad_hit.ast_dist, 'function_dist': bad_hit.function_dist, 'origin_url': bad_hit.origin_url, 'cited_on': bad_hit.cited_on }
    d.update(as_url_fields(bad_hit.origin_url, 'origin'))
    d.update(as_url_fields(bad_hit.cited_on, 'cited_on'))
    # and finally compute hashes for the content for clustering 
    sha256, url_id = find_sha256_hash(db, bad_hit.origin_url)
    uid = None
    if url_id:
       uid = url_id.get('_id')
    d.update({ 'sha256': sha256, 'url_id': uid })   # fields will be None if not found in DB
    result = db.etl_bad_hits.find_one_and_update({ "origin_url": bad_hit.origin_url }, { "$set": d }, upsert = True)

cleanup()
