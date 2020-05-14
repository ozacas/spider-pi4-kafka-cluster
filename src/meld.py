#!/usr/bin/python3
import os
import pymongo
import requests
import logging
import argparse
import tempfile
from bson.objectid import ObjectId
from subprocess import run
from utils.features import find_script, calculate_ast_vector, analyse_script, compute_distance, calculate_vector
from utils.misc import add_mongo_arguments, add_debug_arguments
from utils.models import JavascriptArtefact
from datetime import datetime

a = argparse.ArgumentParser(description="Run meld on the chosen URL as its best control, after JS beatification (optional)")
add_mongo_arguments(a, default_access='read-only', default_user='ro')
add_debug_arguments(a)
a.add_argument("--url", help="URL of Javascript to investigate (code fetched from DB, not internet)", type=str, required=True)
a.add_argument("--diff", help="Diff program to run [/usr/bin/meld]", type=str, default="/usr/bin/meld")
a.add_argument("--beautifier", help="JS Beautifier to run [/usr/local/bin/js-beautify]", type=str, default="/usr/local/bin/js-beautify")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

def save_control(db, filename, control_url):
   script = db.javascript_control_code.find_one({ 'origin': control_url }) 
   if script is None:
       raise ValueError('Unable to fetch code for {}'.format(control_url))
   with open(filename, 'wb+') as fp:
       fp.write(script.get('code'))

def save_script(db, filename, artefact_url, js_id=None):
   assert db is not None
   assert isinstance(filename, str) and len(filename) > 0

   script, url_id = find_script(db, artefact_url)
   if script is None:
       print("Could not locate {} in database! Now try object id...".format(artefact_url))
       script = db.scripts.find_one({ '_id': ObjectId(js_id) })
       if script is None:
           raise ValueError("Failed objectid lookup: {}!".format(js_id))
       else:
           print("Found {} in scripts collection".format(js_id))
       # else FALLTHRU...
   else:
       print("Found {} in scripts collection.".format(artefact_url))
   with open(filename, 'wb+') as fp:
       fp.write(script.get('code'))

def report_vectors(db, artefact_fname, control_url: str, artefact_url: str):
   assert len(control_url) > 0 and len(artefact_url) > 0
   assert os.path.exists(artefact_fname)

   cntrl = db.javascript_controls.find_one({ 'origin': control_url })
   assert cntrl is not None
   assert 'literals_by_count' in cntrl
   assert 'statements_by_count' in cntrl
   assert 'calls_by_count' in cntrl

   # we must analyse the artefact to get the vectors for the artefact (since its too expensive to search kafka for it)
   jsr = JavascriptArtefact(url=artefact_url, sha256='XXX', md5='XXX', inline=False)
   ret, failed, stderr = analyse_script(artefact_fname, jsr)
   if failed:
      raise ValueError("Unable to analyse script: {}".format(artefact_url))
   assert 'literals_by_count' in ret
   assert 'statements_by_count' in ret 
   assert 'calls_by_count' in ret

   # ok, now we have the vectors, lets report the comparison between control and artefact...   
   v1, ast1_sum = calculate_ast_vector(cntrl['statements_by_count'])
   v2, ast2_sum = calculate_ast_vector(ret['statements_by_count'])
   print("AST vector magnitudes: {} {}".format(ast1_sum, ast2_sum))
   print(v1)
   print(v2)
   dist = compute_distance(v1, v2) 
   print("AST distance: {:.2f}".format(dist))
   diffs = []
   all_calls = set(cntrl['calls_by_count'].keys()).union(ret['calls_by_count'].keys()) 
   for fn in all_calls:
       cntl_cnt = cntrl['calls_by_count'].get(fn, 0)
       artefact_cnt = ret['calls_by_count'].get(fn, 0)
       if cntl_cnt != artefact_cnt:
           diffs.append(fn)
   v1, fn1_sum = calculate_vector(cntrl['calls_by_count'], feature_names=all_calls)
   v2, fn2_sum = calculate_vector(ret['calls_by_count'], feature_names=all_calls)
   print("Function call magnitudes: {} {}".format(fn1_sum, fn2_sum))
   print(v1)
   print(v2)
   dist = compute_distance(v1, v2)
   print("Function call distance: {:.2f}".format(dist))
   if len(diffs) == 0:
       print("All literals seen the expected number of times.")
   else:
       print("Functions not called the expected number of times: {}".format(' '.join(diffs)))
   diffs = []
   all_literals = set(cntrl['literals_by_count'].keys()).union(ret['literals_by_count'].keys())
   for lit in all_literals:
       cntl_cnt = cntrl['literals_by_count'].get(lit, 0)
       artefact_cnt = ret['literals_by_count'].get(lit, 0)
       if cntl_cnt != artefact_cnt:
           diffs.append(lit) 
   if len(diffs) > 0:
       print("Literals not seen the expected number of times: {}".format(','.join(diffs)))
   else:
       print("All literals seen the expected number of times.")
 

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

   ##### PREPARE TMP COPIES OF ARTEFACTS UNDER STUDY
   # 1. prepare the artefacts into a temporary directory
   control_fname = '{}/control.js'.format(tdir)
   save_control(db, control_fname, control_url)
   artefact_fname = '{}/artefact.js'.format(tdir)
   save_script(db, artefact_fname, args.url, js_id=result.get('origin_js_id'))

   #### REPORT VECTORS SUMMARISING THE NUMERIC DIFF
   report_vectors(db, artefact_fname, control_url, args.url)
 
   #### BEAUTIFY THEN DISPLAY GRAPHICAL DIFF
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
