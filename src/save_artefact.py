#!/usr/bin/python3
import pymongo
import argparse
import json
import hashlib
from bson.objectid import ObjectId
from utils.misc import add_mongo_arguments
from utils.models import JavascriptArtefact
from utils.features import get_script
from utils.io import find_or_update_analysis_content

a = argparse.ArgumentParser(description="Save the specified artefact to disk as the specified filename")
add_mongo_arguments(a, default_access="read-only", default_user='ro')
a.add_argument("--file", help="Save to specified file []", type=str, required=True)
g = a.add_mutually_exclusive_group(required=True)
g.add_argument('--artefact', help='Retrieve the specified JS script ID from the database', type=str)
g.add_argument('--control', help='Save the control JS code as specified by its URL', type=str)
a.add_argument('--literals', help="Dump string literals and usage count", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

if args.artefact:
   code, js_id = get_script(db, args.artefact)
   assert js_id == args.artefact
   if code is None:
       raise ValueError("Unable to retrieve artefact {}".format(args.artefact))
else: # args.control
   ret = db.javascript_control_code.find_one({ 'origin': args.control })
   if ret is None:
       raise ValueError("Cannot find control {}".format(args.control))
   code = ret.get('code')

with open(args.file, 'wb+') as fp:
   print("Saving artefact... {}".format(args.artefact))
   fp.write(code)

if args.artefact:
   m = { 'sha256': hashlib.sha256(code).hexdigest(), 'md5': hashlib.md5(code).hexdigest(),
         'size_bytes': len(code), 'js_id': args.artefact, 'byte_content_sha256': 'XXX', 'url': 'XXX' }
   vectors = find_or_update_analysis_content(db, m, fail_iff_not_found=True, defensive=True)
   assert isinstance(vectors, dict)
   print("Vectors computed from: {}".format(vectors.get('id')))
else:
   vectors = json.loads(ret.get('analysis_bytes'))

assert 'literals_by_count' in vectors
if vectors is not None and args.literals:
   lv = vectors.get('literals_by_count')
   print("literals seen (sorted): {}".format(', '.join(sorted(lv.keys()))))
   print("values seen (unsorted): {}".format(' '.join(lv.values())))
mongo.close()
exit(0)
