#!/usr/bin/python3
import pymongo
from bson.binary import Binary
import argparse
import json
import hashlib
import requests
from dataclasses import asdict
from datetime import datetime
from utils.features import analyse_script, normalise_vector
from utils.models import JavascriptArtefact, Password, JavascriptVectorSummary
from utils.cdn import CDNJS, JSDelivr

a = argparse.ArgumentParser(description="Insert control artefact features into MongoDB using artefacts from CDN providers")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="User to authenticate to MongoDB RBAC (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="Password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
a.add_argument("--family", help="Name of JS family eg. jquery (ignored iff --update)", type=str, default='jquery')
a.add_argument("--release", help="Name of release eg. 1.10.3a", type=str, default=None)
a.add_argument("--variant", help="Only save artefacts which match variant designation eg. minimised [None]", type=str, default=None)
a.add_argument("--java", help="Path to JVM executable [/usr/bin/java]", type=str, default="/usr/bin/java")
a.add_argument("--extractor", help="Path to feature extractor JAR", type=str, default="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar")
a.add_argument("--list", help="List available assets, but do not save to DB", action="store_true")
a.add_argument("--i18n", help="Save internationalised versions of JS [False]", action="store_true", default=False)
a.add_argument("--provider", help="Specify CDN provider [cdnjs]", type=str, choices=['cdnjs', 'jsdelivr'])
a.add_argument("--update", help="Only update existing controls by re-fetching from CDN provider", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]

def save_control(url, family, version, variant, force=False, refuse_hashes=set(), provider=''):
   resp = requests.get(url)
   content = resp.content

   jsr = JavascriptArtefact(when=str(datetime.utcnow()), sha256=hashlib.sha256(content).hexdigest(),
                             md5 = hashlib.md5(content).hexdigest(), url=url,
                             inline=False, content_type='text/javascript', size_bytes=len(content))
   if jsr.sha256 in refuse_hashes and not force:
       print("Refusing to update existing control as dupe: {}".format(jsr))
       return jsr

   ret, failed, stderr = analyse_script(content, jsr, java=args.java, feature_extractor=args.extractor)
   if failed:
       raise ValueError('Could not analyse script {}'.format(jsr.url))
   ret.update({ 'family': family, 'release': version, 'variant': variant, 'origin': url, 'provider': provider })
   #print(ret)
   # NB: only one control per url/family pair (although in theory each CDN url is enough on its own)
   resp = db.javascript_controls.find_one_and_update({ 'origin': url, 'family': family }, 
                                                     { "$set": ret }, upsert=True)
   resp = db.javascript_control_code.find_one_and_update({ 'origin': url }, 
                                                     { "$set": { 'origin': url, 'code': Binary(content), 
                                                       "last_updated": jsr.when } }, upsert=True)
   # this code comes from etl_control_fix_magnitude, which means we dont need to run it separately
   vector, total_sum = normalise_vector(ret['statements_by_count'])
   sum_of_function_calls = sum(ret['calls_by_count'].values())
   vs = JavascriptVectorSummary(origin=url, sum_of_ast_features=total_sum,
                                 sum_of_functions=sum_of_function_calls, last_updated=jsr.when)
   db.javascript_controls_summary.find_one_and_update({ 'origin': url }, { "$set": asdict(vs) }, upsert = True)
   return jsr

existing_control_hashes = set(db.javascript_controls.distinct('sha256'))

if args.update:
    controls_to_save = [(ret['origin'], ret['family'], ret['variant'], ret['version'], ret['provider']) for ret in db.javascript_controls.find()]
else:
    fetcher = CDNJS() if args.provider == "cdnjs" else JSDelivr()
    controls_to_save = fetcher.fetch(args.family, args.variant, args.release, ignore_i18n=not args.i18n, provider=args.provider)
for url, family, variant, version, provider in controls_to_save:
    if args.v or args.list:
       print("Found artefact: {}".format(url))
    if not args.list:
       try: 
           artefact = save_control(url, family, variant, version, refuse_hashes=existing_control_hashes, provider=provider)
           existing_control_hashes.add(artefact.sha256)
           if args.v:
               print(artefact)
       except Exception as e:
           print(str(e))
    
