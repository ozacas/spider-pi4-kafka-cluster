#!/usr/bin/python3
import pymongo
import argparse
import os
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from utils.misc import add_mongo_arguments, add_extractor_arguments, add_debug_arguments
from utils.io import save_control

a = argparse.ArgumentParser(description="Insert control artefact features into MongoDB using artefacts from local magento release tree")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
a.add_argument("--family", help="Software family designation eg. Magento (community edition)", type=str, required=True)
a.add_argument("--urlroot", help="Root of all URLs constructed. Should be valid artefact URL eg. github.com/owner/repo/...", type=str, required=True)
a.add_argument("--release", help="Name of release eg. 1.10.3a", type=str, required=True)
a.add_argument("--local", help="Root of release tree", type=str, required=True)
a.add_argument("--min-size", help="Minimum JS artefact size (bytes) [1500]", type=int, default=1500)
a.add_argument("--dry-run", help="Do not change the database, but report details of each artefact", action="store_true")
a.add_argument("--provider", help="Provider of package [github.com]", type=str, default="github.com")
add_extractor_arguments(a)
add_debug_arguments(a)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

family = args.family
assert len(family) > 0
urlroot = args.urlroot
assert len(urlroot) > 0
assert urlroot.startswith("https://")
urlbase = urlroot + "/{}/{}"
variant = None
controls_to_save = []
existing_control_hashes = set(db.javascript_controls.distinct('sha256'))

for dirpath, dirnames, files in os.walk(args.local):
    for fname in files:
        fname = "{}/{}".format(dirpath, fname)
        if fname.endswith(".js") and os.path.exists(fname):
            item = fname[len(args.local):]   # only the part of the filename after the root is included in the URL
            if item.startswith("/"):
                item = item[1:]
            #if not item.startswith("app/"):  # do not include test JS etc...
            #    continue
            url = urlbase.format(args.release, item)
            t = (url, family, variant, args.release, args.provider, fname)
            print(t)
            controls_to_save.append(t)
print("Found {} artefacts to save.".format(len(controls_to_save)))

for url, family, variant, version, provider, fname in controls_to_save:
    if args.v:
        print("Found artefact: {}".format(url))
    assert url is not None
    assert family is not None
    assert version is not None
    if args.dry_run:
        print(url, " ", family, " ", variant, " ", version, " ", provider, " ", fname)
        continue
    try: 
        with open(fname, 'rb') as fp:
            artefact = save_control(db, url, family, variant, version, 
                                   refuse_hashes=existing_control_hashes, 
                                   provider=provider, content=fp.read(),
                                   java=args.java, feature_extractor=args.extractor)
            existing_control_hashes.add(artefact.sha256)
            if args.v:
               print(artefact)
    except Exception as e:
       print(str(e))

exit(0)
