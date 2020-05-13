#!/usr/bin/python3
import pymongo
import argparse
import json
import hashlib
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from utils.misc import add_mongo_arguments, add_extractor_arguments, add_debug_arguments
from utils.cdn import CDNJS, JSDelivr
from utils.io import save_control

a = argparse.ArgumentParser(description="Insert control artefact features into MongoDB using artefacts from CDN providers")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
a.add_argument("--family", help="Name of JS family eg. jquery (ignored iff --update)", type=str)
a.add_argument("--release", help="Name of release eg. 1.10.3a", type=str, default=None)
a.add_argument("--variant", help="Only save artefacts which match variant designation eg. minimised [None]", type=str, default=None)
a.add_argument("--i18n", help="Save internationalised versions of JS [False]", action="store_true", default=False)
a.add_argument("--provider", help="Specify CDN provider [cdnjs]", type=str, choices=['cdnjs', 'jsdelivr'], required=False) # not mandated as it will side-effect matches if --update-all is specified
group = a.add_mutually_exclusive_group()
group.add_argument("--list", help="List available assets, but do not save to DB", action="store_true")
group.add_argument("--update-all", help="Only update existing controls by re-fetching from CDN provider", action="store_true")
add_extractor_arguments(a)
add_debug_arguments(a)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

if args.update_all:
    d = {}
    if args.family is not None:
       d['family'] = args.family
    if args.provider:
       d['provider'] = args.provider
    print("Matching controls {}".format(d))
    controls_to_save = [(ret['origin'], ret['family'], ret['variant'], ret['variant'], ret.get('provider', '')) for ret in db.javascript_controls.find(d)]
    existing_control_hashes = set()
    print("Updating all MongoDB data for {} JS controls.".format(len(controls_to_save)))
else:
    fetcher = JSDelivr() if args.provider == "jsdelivr" else CDNJS()
    existing_control_hashes = set(db.javascript_controls.distinct('sha256'))
    family = args.family if args.family is not None else 'jquery' 
    controls_to_save = fetcher.fetch(family, args.variant, args.release, ignore_i18n=not args.i18n, provider=args.provider)
    print("Using {} to fetch matching JS controls for family={}, release={}, variant={}".format(args.provider, family, args.release, args.variant))

for url, family, variant, version, provider in controls_to_save:
    if args.v or args.list:
       print("Found artefact: {}".format(url))
    if not args.list:
       try: 
           artefact = save_control(db, url, family, variant, version, 
                                   refuse_hashes=existing_control_hashes, 
                                   provider=provider, 
                                   java=args.java, feature_extractor=args.extractor)
           if not args.update_all:
               existing_control_hashes.add(artefact.sha256)
           if args.v:
               print(artefact)
       except Exception as e:
           print(str(e))
    
