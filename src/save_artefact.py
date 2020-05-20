#!/usr/bin/python3
import pymongo
import argparse
from bson.objectid import ObjectId
from utils.misc import add_mongo_arguments

a = argparse.ArgumentParser(description="Save the specified artefact to disk as the specified filename")
add_mongo_arguments(a, default_access="read-only", default_user='ro')
a.add_argument("--file", help="Save to specified file []", type=str, required=True)
a.add_argument('--artefact', help='Retrieve the specified JS script ID from the database', type=str, required=True)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

with open(args.file, 'wb+') as fp:
   ret = db.scripts.find_one({ '_id': ObjectId(args.artefact) })
   if ret is None:
       raise ValueError("Unable to retrieve artefact {}".format(args.artefact))
   print("Saving artefact... {}".format(args.artefact))
   fp.write(ret.get('code'))
mongo.close()
exit(0)
