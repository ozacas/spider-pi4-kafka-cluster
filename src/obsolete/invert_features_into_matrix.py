#!/usr/bin/python3
import argparse
import json
import pymongo 
import argparse

a = argparse.ArgumentParser(description="Report scripts from html-page-stats which are not US/AU geolocated based on any IP associated with the URL hostname")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port)
db = mongo[args.dbname]

pipeline = [ 
              {"$project":{"arrayofkeyvalue":{"$objectToArray":"$$ROOT"}}},   
              {"$unwind":"$arrayofkeyvalue"},   
              {"$group":{"_id":None,"allkeys":{"$addToSet":"$arrayofkeyvalue.k"}}} 
           ]

# 1. compute known set of AST-related column names
ast_keys = { i: 1 for i in list(db.statements_by_count.aggregate(pipeline))[0]['allkeys'] }
ast_keys.pop('_id', None) # not interested in this column
ast_keys.pop('id', None) # this is a legacy field which was renamed url, but we dont want this in the data matrix as a column
#print(ast_keys)

# 2. compute initially desired set of function names to look for. We will then threshold these to get rid of rarely used functions
func_keys = list(db.count_by_function.aggregate(pipeline))[0]
#print(func_keys)
wanted_terms = ('createElement', 'getElementByName', 'script', 'uri', 'url', 'json', 'ajax', 'http', 'req', 'host')
wanted_funcs = set([k for k in func_keys["allkeys"] if any(j in k.lower() for j in wanted_terms)])
print(wanted_funcs)

# 3. scan db.count_by_function as count each of the wanted_funcs in the total set of documents
want_freq = { }
cnt = 0
for rec in db.count_by_function.find({}):
    cnt += 1
    for k in rec.keys():
        if k in wanted_funcs:
             if not k in want_freq:
                 want_freq[k] = 0
             want_freq[k] += 1 

# if k occurs in at least 200 documents, we consider it worthy of being a column in the data matrix, thus rarely used columns wont appear. What is the right threshold?
wanted_funcs = { k: want_freq[k] for k in want_freq.keys() if want_freq[k] >= 200 and not k.startswith("_") }
non_blacklisted_funcs = list(filter(lambda x: not ('subscription' in x.lower() or 'required' in x.lower() or x == "url"), wanted_funcs.keys()))
print(non_blacklisted_funcs)
for fn in non_blacklisted_funcs:
    db.wanted_funcs.insert_one({ "function_name": fn})
exit(0)
