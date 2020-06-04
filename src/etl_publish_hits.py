#!/usr/bin/python3
import os
import json
import argparse
import pymongo
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from utils.io import load_controls
from utils.models import BestControl, Password
from utils.misc import *
from utils.features import as_url_fields
from dataclasses import asdict


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
    exit(0)


def process_hit(db, all_controls, hit: BestControl, producer, stats=None):
    assert stats is not None
    dist = hit.ast_dist
    assert dist >= 0.0

    origin_fields = as_url_fields(hit.origin_url, prefix='origin')
    host = origin_fields.get('origin_host')
    if host is None:
        return False

    d = asdict(hit)
    d.pop('diff_functions', None)
    rec = db.analysis_content.find_one({ 'js_id': d['origin_js_id'], 'byte_content_sha256': d['origin_vectors_sha256'] })
    assert rec is not None and 'calls_by_count' in rec
    fv_origin = rec.get('calls_by_count')
    if fv_origin is None:
        return False

    u = hit.control_url
    assert u in all_controls # should have been checked before call
    fv_control = all_controls[u].get('calls_by_count')
    d.update(origin_fields)

    # cited_on URL (aka. HTML page) iff specified
    d.update(as_url_fields(hit.cited_on, prefix='cited_on'))
    d['control_family'] = all_controls[u].get('family')

    # good hits get sent to the suspicious analysis pipeline
    ok, reason = hit.good_hit_as_tuple()
    if ok:
        if not reason in stats:
            stats[reason] = 0
        stats[reason] += 1
        dc = d.copy()
        dc.pop('_id', None)
        dc['diff_functions'] = hit.diff_functions_as_list()
        assert isinstance(dc['diff_functions'], list)
        if dc['cited_on_host'] is None:   # some buggy records - rare
            print("Bad data - skipping... {}".format(dc))
            return False
        producer.send('etl-good-hits', dc)
        # sanity check: if different literals are found then the string must be greater than zero length
        assert (hit.n_diff_literals > 0 and len(dc.get('diff_literals')) > 0) or hit.n_diff_literals <= 0 
        db.etl_hits.insert_one(dc) # BREAKING CHANGE: dc['diff_functions'] is now a list not a comma separated string, but literals is still a string
        return True
    else:
        if args.bad:
            db.etl_bad_hits.insert_one(d)
        return False

if __name__ == "__main__":
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
    a.add_argument("--threshold", help="Ignore hits with AST * Function call distance greater than this [50.0]", type=float, default=50.0)
    a.add_argument("--tail", help="Dont terminate if we've read all the messages. Wait for new ones", action="store_true")
    a.add_argument("--bad", help="Save hits which fail threshold to db.etl_bad_hits", action='store_true')
    args = a.parse_args()

    mongo = pymongo.MongoClient(args.db, args.port, 
                    username=args.dbuser, password=str(args.dbpassword))
    db = mongo[args.dbname]
    timeout = float('Inf') if args.tail else 10000
    consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, 
                 auto_offset_reset=args.start, consumer_timeout_ms=timeout,
                 value_deserializer=json_value_deserializer())
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=json_value_serializer())
    setup_signals(cleanup)
    n_ok = n_bad = 0
    n_good = n_not_good = n = 0
    save_pidfile('pid.etl.hits')
    all_controls = {}
    for t in load_controls(db, literals_by_count=False, verbose=args.v):
        assert isinstance(t, tuple)
        assert len(t) == 3
        assert isinstance(t[0], dict)
        assert 'origin' in t[0]
        all_controls[t[0].get('origin')] = t[0]


    stats = {}
    for r in consumer: 
        n += 1
        if 'origin_vectors_sha256' not in r.value:
            continue

        hit = BestControl(**r.value)

        if args.v:
            if n % 10000 == 0:
                 print("Processed {} records ({} not good). {}".format(n, n_not_good, str(datetime.utcnow())))
                 print(stats)
        if n > args.n: # TODO FIXME: will lose last message unless we exit before kafka auto commit occurs (but only if --n specified)
            break

        # 1. if xref is missing/empty reject the entire record
        if hit.xref is None or len(hit.xref) == 0:
            n_bad += 1
            continue

        # 2. bad AST*function call product (over threshold)? Or not a hit? reject entire record
        if (len(hit.control_url) == 0 or hit.ast_dist * hit.function_dist >= args.threshold) and args.bad:
            db.etl_bad_hits.insert_one(asdict(hit))
            n_not_good += 1
            continue

        # 3. control for the hit no longer in the db? This typically happens as the control has been rejected as too small or some other criteria
        u = hit.control_url
        if not u in all_controls:
            print("Could not find {} in all controls".format(u))
            n_bad += 1
            continue   # control no longer in database? ok, skip further work

        ret = process_hit(db, all_controls, hit, producer, stats={})
        if ret:
            n_ok += 1
        else:
            n_bad += 1 

    print("Run completed: processed {} records with AST*function_call threshold <= {}".format(n, args.threshold))
    print("{} records had a problem which could not be ignored.".format(n_bad))
    print("Published {} hits, {} failed is_good_hit() test.".format(n_ok, n_not_good))
    if args.bad:
        print("{} records failed threshold, published to db.etl_bad_hits".format(n_not_good))
    cleanup()
