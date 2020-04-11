#!/usr/bin/python3
import os
import pymongo
import requests
import logging
import argparse
import tempfile
from subprocess import run
from utils.models import Password
from utils.features import find_script 
from datetime import datetime

a = argparse.ArgumentParser(description="Run meld on the chosen URL as its best control, after JS beatification (optional)")
a.add_argument("--mongo-host", help="Hostname/IP with mongo instance [pi1]", type=str, default="pi1")
a.add_argument("--mongo-port", help="TCP/IP port for mongo instance [27017]", type=int, default=27017)
a.add_argument("--db", help="Mongo database to populate with JS data [au_js]", type=str, default="au_js")
a.add_argument("--user", help="Database user to read artefacts from (read-only access required)", type=str, required=True)
a.add_argument("--password", help="Password (prompted if not specified)", type=Password, default=Password.DEFAULT)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--url", help="URL of Javascript to investigate (code fetched from DB, not internet)", type=str, required=True)
a.add_argument("--diff", help="Diff program to run [/usr/bin/meld]", type=str, default="/usr/bin/meld")
a.add_argument("--beautifier", help="JS Beautifier to run [/usr/local/bin/js-beautify]", type=str, default="/usr/local/bin/js-beautify")
args = a.parse_args()

mongo = pymongo.MongoClient(args.mongo_host, args.mongo_port, username=args.user, password=str(args.password))
db = mongo[args.db]

# look for suitable control in etl_hits
result = db.etl_hits.find_one({ 'origin_url': args.url })
print(result)
if not result:
   print("Failed to find suitable control - nothing to report!")
   exit(1)
with tempfile.TemporaryDirectory() as tdir: 
   print(tdir)
   control_url = result.get('control_url')
   if not len(control_url):
       print("No suitable control {}".format(control_url))
   print("Downloading control {}".format(control_url))
   resp = requests.get(control_url)
   # 1. prepare the artefacts into a temporary directory
   with open('{}/control.js'.format(tdir), 'wb+') as fp:
       fp.write(resp.content)
   script, url_id = find_script(db, args.url)
   if not script:
       print("Unable to find {} in database!".format(args.url))
       exit(1)
   with open('{}/artefact.js'.format(tdir), 'wb+') as fp:
       fp.write(script.get('code'))
   # 2. beautify them (ie. reduce minimisation to ensure consistent representation for diff'ing etc.)
   bs1 = "{}/1.js".format(tdir)
   bs2 = "{}/2.js".format(tdir)
   proc = run([args.beautifier, "-o", bs1, "{}/control.js".format(tdir)])
   print(proc)
   proc = run([args.beautifier, "-o", bs2, "{}/artefact.js".format(tdir)])
   print(proc)

   # 3. run meld on the resulting beautified-JS for both control and artefact
   diff_proc = run([args.diff, bs1, bs2])

mongo.close()
exit(0)
