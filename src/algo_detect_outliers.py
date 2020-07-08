#!/usr/bin/python3
import argparse
import pymongo
import pandas as pd
from utils.misc import add_mongo_arguments, add_debug_arguments
from build_data_matrix_from_etl import calc_rarest_function
from pyod.models.mcd import MCD

if __name__ == "__main__":
    a = argparse.ArgumentParser(description='Detect outliers for a given control URL and report them to stdout based on available data')
    add_mongo_arguments(a, default_access="read-only", default_user="ro")
    add_debug_arguments(a)
    a.add_argument("--since", help="Only consider hits since .... date (YYYY-mm-dd) []", type=str, required=False)
    a.add_argument("--control", help="Consider hits for XXX control URL", type=str, required=True)
    a.add_argument("--cluster", help="Save cluster data to specified file []", type=str, required=False)
    args = a.parse_args()

    mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
    db = mongo[args.dbname]

    df = pd.DataFrame(columns=['ast_dist', 'fcall_dist', 'literal_dist', 'xref', 'rarest_function_probability'])
    for rec in db.etl_hits.aggregate([ { "$match": {"control_url": args.control}},
                            { "$match": { "sha256_matched": False }},    # hash matches are not useful for consideration as outliers
                            { "$unwind": "$diff_functions" },
			    { "$group": {
                                  "_id": { "xref": "$xref" },
                                  "ast_dist": { "$first": "$ast_dist" },
                                  "fcall_dist": { "$first": "$function_dist" },
                                  "literal_dist": { "$first": "$literal_dist" },
                                  "diff_functions": { "$addToSet": "$diff_functions" },
                            }},
                            { "$project": {
                                  "xref": "$_id.xref",
                                  "ast_dist": 1,
                                  "fcall_dist": 1,
                                  "literal_dist": 1,
                                  "diff_functions": 1 
                            }},
                            { "$project": { "_id": 0 } },
                          ]):
        rec.update({ 'control_url': args.control })  # so we can call calc_rarest_function which requires it, in order to identify function probability hits
        rarest_prob, fn = calc_rarest_function(db, rec)
        rec.update({ "rarest_function_probability": rarest_prob }) 
        rec.pop('diff_functions', None)
        rec.pop('control_url', None)
        df = df.append(rec, ignore_index=True, verify_integrity=True)
    print("Found {} hits".format(len(df)))
    df = df.set_index('xref')
    deduped = df.drop_duplicates()
    deduped.to_csv('/tmp/crap.txt', sep='\t') 
    if len(deduped) > 15: # require at least 15 different vectors to be suitable for analysis 
        mcd = MCD()
        mcd.fit(deduped)
        labels = mcd.predict(df) 
        print(labels)
    else:
        print("ERROR! Not enough datapoints for analysis - check /tmp/crap.txt for details")
    exit(0)
