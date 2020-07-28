#!/usr/bin/python3
import os
import json
import pymongo
import hashlib
import argparse
from bson import Binary
from utils.features import analyse_script
from utils.models import JavascriptArtefact
from utils.misc import *
from utils.io import update_control_summary
from datetime import datetime

a = argparse.ArgumentParser(description="Extract features from each javascript in visited topic and dump into analysis-results topic")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument('--defensive', help="Verify stored hash against actual", action="store_true")
a.add_argument('--recalc', help="Recalculate vector in addition to all summary fields", action="store_true")

if __name__ == "__main__":
   args = a.parse_args()
   mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
   db = mongo[args.dbname]
   cursor = db.javascript_control_code.find({}, no_cursor_timeout=True) # long-running find so we try to avoid it being killed prematurely...
   with cursor:
       for rec in cursor:
           assert 'code' in rec
           control_url = rec.get('origin')
           assert control_url.startswith("http")
           if args.v:
               print(control_url)

           # recalculate vector?
           if args.recalc:
               jsr = JavascriptArtefact(url=control_url, sha256='XXX', md5='YYY', inline=False)  # only url matters for analyse script
               vectors, failed, stderr = analyse_script(rec.get('code'), jsr, java=args.java, feature_extractor=args.extractor)
               assert not failed
               assert isinstance(vectors, bytes)
               print(jsr.url)
               required_hash = hashlib.sha256(vectors).hexdigest()
               db.javascript_control_code.update_one({ '_id': rec.get('_id') }, 
                                                 { "$set": { "analysis_bytes": Binary(vectors) } })
           else: 
               vectors = rec.get('analysis_bytes')
               assert vectors is not None
               assert isinstance(vectors, bytes)
           
           d = json.loads(vectors)
           update_control_summary(db, control_url, d['statements_by_count'], d['calls_by_count'], d['literals_by_count'])
           db.javascript_controls.update_one({ 'origin': control_url }, { '$set': { 
                'sha256': hashlib.sha256(rec['code']).hexdigest(),
                'md5': hashlib.md5(rec['code']).hexdigest(),
                'size_bytes': len(rec['code'])
           } })
   exit(0)
