#!/usr/bin/python3
import pymongo
import argparse
import json
import requests
from datetime import datetime
from dataclasses import asdict
from utils.features import analyse_script, normalise_vector
from utils.models import JavascriptArtefact, Password, JavascriptVectorSummary

a = argparse.ArgumentParser(description="Insert feature vectors from artefacts into MongoDB")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="User to authenticate to MongoDB RBAC (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="Password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]

controls = list(db.javascript_controls.find()) 

cnt = 0
for c in controls:
    vector, total_sum = normalise_vector(c['statements_by_count'])
    sum_of_function_calls = sum(c['calls_by_count'].values())
    if args.v:
       print(c['origin'])
    vs = JavascriptVectorSummary(origin=c['origin'], sum_of_ast_features=total_sum, 
                                 sum_of_functions=sum_of_function_calls, last_updated=str(datetime.utcnow()))
    db.javascript_controls_summary.find_one_and_update({ 'origin': c['origin'] }, { "$set": asdict(vs) }, upsert = True)
    if args.v and cnt % 1000 == 0:
        print("Processed {} records.".format(cnt))
    cnt += 1
print("Updated {} records in collection 'javascript_controls_summary'.".format(cnt))
exit(0)
