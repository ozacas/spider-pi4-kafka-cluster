#!/usr/bin/python3
import pymongo
import csv
import sys
import argparse
import pprint
from statistics import mean
from utils.models import Password

def dump_pretty(result):
    for rec in result:
        pprint.pprint(rec)

def dump_distances(db, pretty=False, threshold=10.0):
    result = db.etl_hits.aggregate([
        { "$match": { "ast_dist": { "$lt": threshold } } },
        { "$group":
             { "_id": { "control_url": "$control_url",
                        "origin_url": "$origin_url",
                        "cited_on_host": "$cited_on_host" },
               "distances": { "$addToSet": "$ast_dist" },
               "min_function_dist": { "$min": "$function_dist" },
               "pages": { "$addToSet": "$cited_on_path" }
             }
        },
    ], allowDiskUse=True)

    if pretty:
        dump_pretty(result)
    else:
        headers = ['host', 'min_distance', 'avg_distance', 'max_distance', 'n_pages', 'control_url', 'origin_url']
        print('\t'.join(headers))
        for rec in result:
           #print(rec)
           # eg. {'_id': {'control_url': 'https://cdnjs.cloudflare.com/ajax/libs/jquery-mousewheel/3.1.0/jquery.mousewheel.min.js', 'cited_on_host': 'www2.dtwd.wa.gov.au'}, 'distances': [31.32091952673165], 'pages': ['/mureskinstitute/']}
           id = rec.get('_id')
           distances = rec.get('distances')
           l = [ id['cited_on_host'], str(min(distances)), str(mean(distances)), str(max(distances)), 
                 str(rec.get('min_function_dist')), str(len(rec.get('pages'))), id['control_url'], id['origin_url'] ]
           print('\t'.join(l))

def dump_sites_by_control(db, pretty=False, want_set=False):
    result = db.etl_hits.aggregate([ 
                 { "$group": 
                      { "_id": "$control_url", 
                        "set": { "$addToSet": "$cited_on_host" } 
                      } 
                 } 
    ], allowDiskUse=True)

    if pretty:
        dump_pretty(result.find())
    else:
        headers = ['control_url', 'n_sites']
        if want_set:
            headers.append('site_set')
        print('\t'.join(headers))
        for rec in result:
            l = [rec['_id'], str(len(rec['set']))]
            if want_set:
                l.append(','.join(rec['set']))
            print('\t'.join(l))

def dump_diff_functions_by_control(db, pretty=False, control=None, want_set=False):
    # we want to aggregate hits to the specified control where we obtain the list of sites for each function
    result = db.etl_hits.aggregate([ 
                 { "$match":  
                      { "control_url": control, 
                        "ast_dist": { "$lt": 10.0 } },
                 },
                 { "$count": "stage1" },
                 { "$group": 
                      { "_id": { "diff_function": "$diff_function", 
                                 "cited_on_host": "$cited_on_host",
                                 "cited_on_path": "$cited_on_path",
                                 "origin_url": "$origin_url",
                               },
                        "best_ast_dist": { "$min": "$ast_dist" },
                      } 
                 },
                 { "$count": "stage2" },
                 { "$sort": 
                      { "_id.cited_on_host": 1, "_id.diff_function": 1 } 
                 },
    ], allowDiskUse=True)
    if pretty:
        dump_pretty(result)
    else:
        headers = ['diff_function', 'site', 'n_pages_seen_at_site', 'best_ast_dist', 'origin_url', 'control_url']
        if want_set:
            headers.append('paths_seen')
        print('\t'.join(headers))
        page_counts = { }
        other_data = { }
        # finish group by with memory expensive components which MongoDB balks out: TODO FIXME: scalability
        for rec in result:
            id = rec.get('_id')
            host = id['cited_on_host']
            other_data[host] = { "fn": id['diff_function'], 'best_ast_dist': rec.get('best_ast_dist'), 'origin_url': id['origin_url'], 'control': control }
            if not host in page_counts:
                page_counts[host] = set()
            page_counts[host].add(id['cited_on_path'])
        
        for host in page_counts.keys():
            d = other_data.get(host)
            l = [d['fn'], host, str(len(page_counts[host])), str(d['best_ast_dist']), d['origin_url'], control]
            if want_set:
                l.append(' '.join(page_counts[host]))
            print('\t'.join(l))


a = argparse.ArgumentParser(description="Process results for a given query onto stdout for ingestion in data analysis pipeline")
a.add_argument("--host", help="Hostname/IP with Mongo instance [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP/IP port for Mongo instance [27017]", type=int, default=27017)
a.add_argument("--db", help="Mongo database to populate with JS data from kafkaspider [au_js]", type=str, default="au_js")
a.add_argument("--user", help="MongoDB user to connect as (read-only access required)", type=str, required=True)
a.add_argument("--password", help="MongoDB password (if not specified, will be prompted)", type=Password, default=Password.DEFAULT)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--pretty", help="Use pretty-printed JSON instead of TSV as stdout format", action="store_true")
a.add_argument("--query", help="Run specified query, one of: sitesbycontrol|functionsbycontrol", type=str, choices=['sitesbycontrol', 'functionsbycontrol', 'distances'])
a.add_argument("--extra", help="Parameter for query", type=str)
args = a.parse_args()

mongo = pymongo.MongoClient(args.host, args.port, username=args.user, password=str(args.password))
db = mongo[args.db]
if args.query == "functionsbycontrol":
    dump_diff_functions_by_control(db, pretty=args.pretty, control=args.extra, want_set=args.v)
elif args.query == "sitesbycontrol":
    dump_sites_by_control(db, want_set=args.v)
elif args.query == "distances":
    dump_distances(db, pretty=args.pretty)
exit(0)
