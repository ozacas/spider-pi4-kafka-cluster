#!/usr/bin/python3
import os
import pymongo
import requests
import logging
import argparse
import tempfile
from bson.objectid import ObjectId
from subprocess import run
from utils.features import find_script 
from utils.misc import add_mongo_arguments, add_debug_arguments
from datetime import datetime

a = argparse.ArgumentParser(description="Run meld on the chosen URL as its best control, after JS beatification (optional)")
add_mongo_arguments(a)
add_debug_arguments(a)
a.add_argument("--url", help="URL of Javascript to investigate (code fetched from DB, not internet)", type=str, required=True)
a.add_argument("--diff", help="Diff program to run [/usr/bin/meld]", type=str, default="/usr/bin/meld")
a.add_argument("--beautifier", help="JS Beautifier to run [/usr/local/bin/js-beautify]", type=str, default="/usr/local/bin/js-beautify")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

def save_control(filename, control_url):
   script = db.javascript_control_code.find_one({ 'origin': control_url }) 
   if script is None:
       raise ValueError('Unable to fetch code for {}'.format(control_url))
   with open(filename, 'wb+') as fp:
       fp.write(script.get('code'))

def save_script(filename, artefact_url):
   script, url_id = find_script(db, artefact_url)
   if script is None:
       script = db.scripts.find_one({ '_id': ObjectId(result.get('origin_js_id')) })
       if script is None:
           raise ValueError("Unable to find {} in database!".format(artefact_url))
       # else FALLTHRU...
   with open(filename, 'wb+') as fp:
       fp.write(script.get('code'))
   
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

   # 1. prepare the artefacts into a temporary directory
   control_fname = '{}/control.js'.format(tdir)
   save_control(control_fname, control_url)
   artefact_fname = '{}/artefact.js'.format(tdir)
   save_script(artefact_fname, args.url)

   # 2. beautify them (ie. reduce minimisation to ensure consistent representation for diff'ing etc.)
   bs1 = "{}/1.js".format(tdir)
   bs2 = "{}/2.js".format(tdir)
   proc = run([args.beautifier, "-o", bs1, "{}/control.js".format(tdir)])
   if proc.returncode != 0:
      raise Exception("Unable to beautify control!")
   proc = run([args.beautifier, "-o", bs2, "{}/artefact.js".format(tdir)])
   if proc.returncode != 0:
      raise Exception("Unable to beautify artefact!")

   # 3. run meld on the resulting beautified-JS for both control and artefact
   diff_proc = run([args.diff, bs1, bs2])

mongo.close()
exit(0)
