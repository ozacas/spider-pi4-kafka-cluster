#!/usr/bin/python3
import pymongo
import json
import argparse
import hashlib
from kafka import KafkaConsumer
from utils.models import JavascriptArtefact
from utils.features import find_script, safe_for_mongo
from utils.io import *
from utils.misc import *

a = argparse.ArgumentParser(description="Due to a bug in find_best_control() some sha256 hash matches were incorrect. Recalculate them to fix it.")
add_mongo_arguments(a, default_access="read-write")
add_debug_arguments(a)
a.add_argument("--n", help="Process no more than N records [Inf]", type=float, default=float('Inf'))
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

n = n_fixed = n_failed = 0
results = db.vet_against_control.find({ 'sha256_matched': False, "ast_dist": { "$lt": 0.000001 }, "function_dist": { "$lt": 0.000001 } })
for rec in results:
    if args.v:
       print(rec)
    control_url = rec.get('control_url')
    origin_url = rec.get('origin_url')

    n += 1
    if n % 1000 == 0:
        print("Processed {} records.".format(n))
    if n >= args.n:
        break

    # we need to obtain the features from the artefact. Several choices:
    # 1. look at the MongoDB to fetch the latest copy of origin_url
    # 2. re-fetch the origin URL
    # 3. Similar to 1) but choose the nearest timestamp prior to the timestamp embedded in the ObjectID associated with rec
    # we choose (1)
    # In any case, we update ALL hash matches present in db.etl_hits collection with the 
    # correct control_url and origin_url so long as the hit has a near-zero distance  

    # Eg. rec == {'_id': ObjectId('5e79424633bf432bbad243ff'), 'origin_url': 'https://www.sinswonline.pre.education.nsw.gov.au/Assets/js/bootstrap.min.js', 
    # 'ast_dist': 0.0, 'control_url': 'https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/3.3.7/js/bootstrap.min.js', 
    # 'diff_functions': '', 'function_dist': 0.0, 'sha256_matched': False, 'cited_on': 'https://www.sinswonline.pre.education.nsw.gov.au/IntranetSites/FutureSite'}
    most_recent_hit = list(sorted(db.urls.find({ 'url': origin_url }), key=lambda r: r.get('_id') ))[-1]
    script_rec = list(db.script_url.find({ 'url_id': most_recent_hit.get('_id') }))[-1]
    script = db.scripts.find_one({ '_id': script_rec.get('script') })
    if script is None:
        n_failed += 1
        continue
    actual_sha256 = hashlib.sha256(script.get('code')).hexdigest()
    expected_sha256 = db.javascript_controls.find_one({ 'origin': control_url })
    if expected_sha256 is not None and actual_sha256 == expected_sha256.get('sha256'):
        db.vet_against_control.update_one({ '_id': rec.get('_id') }, { "$set": { 'sha256_matched': True })
        result = db.etl_hits.update_many({ 'control_url': control_url, 'origin_url': origin_url, "ast_dist": { "$lt": 0.000001 } }, { "$set": { 'sha256_matched': True } })
        n_fixed += 1

print("Processed {} record, fixed {} and could not locate scripts for {}.".format(n, n_fixed, n_failed))
exit(0)
