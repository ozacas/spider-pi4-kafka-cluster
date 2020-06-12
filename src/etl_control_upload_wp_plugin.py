#!/usr/bin/python3
import pymongo
import argparse
import json
import os
from utils.misc import add_mongo_arguments, add_extractor_arguments, add_debug_arguments
from utils.io import save_control

a = argparse.ArgumentParser(description="Insert control artefact features into MongoDB using artefacts from CDN providers")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
a.add_argument("--family", help="Name of WordPress plugin eg. contact-form-7", type=str, required=True)
a.add_argument("--local", help="Local checked-out SVN plugin copy for scanning for JS artefacts", type=str, required=True)
a.add_argument("--variant", help="Only update artefacts with the specified string in their name eg. min.js", type=str, default=None)
a.add_argument("--force", help="Force update even if database entry exists", action="store_true")
add_extractor_arguments(a)
add_debug_arguments(a)
a.add_argument("--list", help="List the artefacts to save, but do not change the database", action="store_true")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

controls_to_save = []
for dir, dirs, files in os.walk(args.local, topdown=True):
    js_files = list(filter(lambda f: f.endswith(".js"), files))
    if len(js_files) < 1:
        continue
    if '/tags/' not in dir:
        continue
    for js_file in js_files:
        if args.variant and not args.variant in js_file:
            continue
        remnant_idx = dir.find("/tags/")
        if remnant_idx < 0:
            continue 
        remnant = dir[remnant_idx + len("/tags/"):]
        if remnant.startswith("/"):
            remnant = remnant[1:]
        
        release_idx = remnant.find('/') 
        if release_idx > 0:
           release = remnant[0:release_idx]
           rest = remnant[release_idx+1:] + '/'
        else:
           rest = ''
           release = remnant
        wp_url = "https://plugins.svn.wordpress.org/{}/tags/{}/{}{}".format(args.family, release, rest, js_file)
        if '/i18n/'  in wp_url:
            continue
        with open("{}/{}".format(dir, js_file), "rb") as fp:
            tuple = (wp_url, args.family, release, release, "plugins.svn.wordpress.org", fp.read())
            controls_to_save.append(tuple)

existing_control_hashes = set(db.javascript_controls.distinct('sha256'))
for url, family, variant, version, provider, content in controls_to_save:
    if args.v or args.list:
       print("Found artefact: {}".format(url))
    if not args.list:
       try: 
           # since python requests will 403 without cookies/UA etc. we use the local filesystem content instead
           artefact = save_control(db, url, family, variant, version, 
                                   refuse_hashes=existing_control_hashes, 
                                   provider=provider, force=args.force,
                                   java=args.java, feature_extractor=args.extractor, content=content)
           existing_control_hashes.add(artefact.sha256)
           if args.v:
               print(artefact)
       except AssertionError as a:
           raise(a)
       except Exception as e:
           print(str(e))
    
