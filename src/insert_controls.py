#!/usr/bin/python3
import pymongo
import argparse
import json
import hashlib
from datetime import datetime
from utils.features import analyse_script
from utils.models import JavascriptArtefact

a = argparse.ArgumentParser(description="Insert AST vectors and relate it to the JS release family into the specified MongoDB for later matching")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--family", help="Name of JS family eg. jquery", type=str, required=True)
a.add_argument("--release", help="Name of release eg. 1.10.3a", type=str, required=True)
a.add_argument("--variant", help="Variant designation eg. minimised (empty is permitted, but not recommended) [minimised]", type=str, default="minimised")
a.add_argument("--file", help="File to read from eg. jquery-1.10.3.min.js", type=str, required=True)
a.add_argument("--java", help="Path to JVM executable [/usr/bin/java]", type=str, default="/usr/bin/java")
a.add_argument("--extractor", help="Path to feature extractor JAR", type=str, default="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port)
db = mongo[args.dbname]

with open(args.file, 'rb') as fp:
    content = fp.read()
    jsr = JavascriptArtefact(when=str(datetime.utcnow()), sha256=hashlib.sha256(content).hexdigest(),
                             md5 = hashlib.md5(content).hexdigest(), url=args.file, 
                             inline=False, content_type='text/javascript', size_bytes=len(content))
    ret = analyse_script(content, jsr, producer=None, java=args.java, feature_extractor=args.extractor)
    ret.update({ 'family': args.family, 'release': args.release, 'variant': args.variant })
    print(ret)
    db.javascript_controls.insert_one(ret)
