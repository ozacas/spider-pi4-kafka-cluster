#!/usr/bin/python3
import pymongo
import argparse
import json
from bson.objectid import ObjectId
from utils.misc import add_mongo_arguments
from utils.models import JavascriptArtefact
from utils.features import analyse_script

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
   ret = db.scripts.find_one({ '_id': ObjectId(args.artefact) })
   if ret is None:
       raise ValueError("Unable to retrieve artefact {}".format(args.artefact))
else: # args.control
   ret = db.javascript_control_code.find_one({ 'origin': args.control })
   if ret is None:
       raise ValueError("Cannot find control {}".format(args.control))

with open(args.file, 'wb+') as fp:
   print("Saving artefact... {}".format(args.artefact))
   fp.write(ret.get('code'))

if args.artefact:
   jsr = JavascriptArtefact(url='foo', sha256='XXX', md5='YYY', inline=False) # doesnt matter from the perspective of dumping the literals
   byte_content, failed, stderr = analyse_script(args.file, jsr)
   if failed:
      raise ValueError("Unable to analyse script: {}\n{}".format(args.file, stderr))
   ret = json.loads(byte_content)
else:
   doc = db.javascript_control_code.find_one({'origin': args.control })
   ret = json.loads(doc.get('analysis_bytes'))

assert 'literals_by_count' in ret
if ret is not None and args.literals:
   for k,v in ret.get('literals_by_count').items():
       print(v, " ", k)

mongo.close()
exit(0)
