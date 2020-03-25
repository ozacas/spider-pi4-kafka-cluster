#!/usr/bin/python3
import pymongo
import argparse
import json
import hashlib
import requests
from datetime import datetime
from utils.features import analyse_script
from utils.models import JavascriptArtefact, Password
from utils.cdnjs import CDNJS
from tempfile import NamedTemporaryFile

a = argparse.ArgumentParser(description="Insert AST vectors and relate it to the JS release family into the specified MongoDB for later matching")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="User to authenticate to MongoDB RBAC (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="Password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
a.add_argument("--family", help="Name of JS family eg. jquery", type=str, required=True)
a.add_argument("--release", help="Name of release eg. 1.10.3a", type=str, default=None)
a.add_argument("--variant", help="Only save artefacts which match variant designation eg. minimised [None]", type=str, default=None)
a.add_argument("--java", help="Path to JVM executable [/usr/bin/java]", type=str, default="/usr/bin/java")
a.add_argument("--extractor", help="Path to feature extractor JAR", type=str, default="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar")
a.add_argument("--list", help="List available assets, but do not save to DB", action="store_true")
a.add_argument("--i18n", help="Save internationalised versions of JS [False]", action="store_true", default=False)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]

def save_control(url, family, version, variant):
   resp = requests.get(url)
   content = resp.content

   jsr = JavascriptArtefact(when=str(datetime.utcnow()), sha256=hashlib.sha256(content).hexdigest(),
                             md5 = hashlib.md5(content).hexdigest(), url=url,
                             inline=False, content_type='text/javascript', size_bytes=len(content))
   ret = analyse_script(content, jsr, producer=None, java=args.java, feature_extractor=args.extractor)
   if ret is None:
       raise ValueError('Could not analyse script {}'.format(jsr.url))
   ret.update({ 'family': family, 'release': version, 'variant': variant, 'origin': url })
   #print(ret)
   # NB: only one control per url/family pair (although in theory each CDN url is enough on its own)
   resp = db.javascript_controls.find_one_and_update({ 'origin': url, 'family': family }, { "$set": ret }, upsert=True)
   if args.v:
       print(resp) 

cdnjs = CDNJS()
for url, family, variant, version in cdnjs.fetch(args.family, args.variant, args.release, ignore_i18n=not args.i18n):
    if args.v or args.list:
       print("Found control artefact: {}".format(url))
    if not args.list:
       try: 
           save_control(url, family, variant, version)
       except Exception as e:
           print(str(e))
    