#!/usr/bin/python3
import os
import sys
import json
import argparse
import pymongo
from kafka import KafkaConsumer
from util.io import next_artefact
from utils.models import BestControl, Password
from utils.misc import *
from utils.features import as_url_fields, find_sha256_hash
from dataclasses import asdict

a = argparse.ArgumentParser(description="Reconcile poor hits into non-normalised, Mongo collection")
add_kafka_arguments(a, consumer=True, default_from='javascript-artefact-control-results', default_group='etl-bad-hits')
add_mongo_arguments(a, default_access="read-write")
add_debug_arguments(a)
a.add_argument("--threshold", help="Only report hits with ast_distance greater than this [50.0]", type=float, default=50.0)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, 
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


setup_signals(cleanup)
n_unable = n_ok = 0
save_pidfile('pid.etl.badhits')
for bad_hit in filter(lambda c: c.ast_dist > args.threshold, [BestControl(**r) for r in next_artefact(consumer, args.n, args.v)]):
    # bad hits are still useful:
    # 1) they may indicate a javascript family which must be added to the controls in the database
    # 2) they might suggest other ways which have to be handled by the system
    # so here we want to provide enough data (even for the bad hit) to enable build-data-matrix-from-etl.py to 
    # be able to efficiently query for clusters of bad hits
    d = { 'ast_dist': bad_hit.ast_dist, 'function_dist': bad_hit.function_dist, 'origin_url': bad_hit.origin_url, 'cited_on': bad_hit.cited_on }
    d.update(as_url_fields(bad_hit.origin_url, prefix='origin'))
    d.update(as_url_fields(bad_hit.cited_on, prefix='cited_on'))
    # and finally compute hashes for the content for clustering 
    sha256, url_id = find_sha256_hash(db, bad_hit.origin_url)
    uid = None
    if url_id:
       uid = url_id.get('_id')
    d.update({ 'sha256': sha256, 'url_id': uid })   # fields will be None if not found in DB
    result = db.etl_bad_hits.find_one_and_update({ "origin_url": bad_hit.origin_url }, { "$set": d }, upsert = True)

cleanup()
