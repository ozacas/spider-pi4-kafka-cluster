#!/usr/bin/python3
import pymongo
import sys
import argparse
from dataclasses import dataclass, field
from typing import List
from statistics import mean
from utils.misc import *

@dataclass
class FunctionProbability:
   control_family: str
   function_name: str
   sites: List[str] = field(default_factory=list)
   ast_dist: List[str] = field(default_factory=list)
   function_dist: List[str] = field(default_factory=list) 
   n_sites: int = 0
   n_pages: int = 0
   n_sites_for_family: int = 0
   n_pages_for_family: int = 0

def dump_pretty(result):
    import pprint
    for rec in result:
        pprint.pprint(rec)

def dump_rare_diff_functions(db, pretty=False, threshold=20.0):
    # 1. compute function probability across each family of controls (per differentially expressed function)
    print("Applying threshold of {} to hits (either ast_dist or function_dist below threshold)".format(threshold))

    sites_by_function_count = db.etl_hits.aggregate([
        { "$match": { "$or": [{"ast_dist": { "$lt": threshold } }, {"function_dist": { "$lt": threshold } } ] } },
        { "$group": 
             { "_id": { "family": "$control_url", "diff_function": "$diff_function" } ,
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" },
               "ast_dist": { "$push": "$ast_dist" },
               "function_dist": { "$push": "$function_dist" },
             }
        },
        { "$sort": { "_id.family": 1, "_id.diff_function": 1 } },
    ], allowDiskUse=True)

    final_result = []
    for rec in sites_by_function_count:
        id = rec.get('_id') 
        fp = FunctionProbability(control_family=id['family'], 
                                 function_name=id['diff_function'], 
                                 n_sites=len(rec.get('sites')), 
                                 n_pages=len(rec.get('unique_pages')),
                                 sites=rec.get('sites'),
                                 ast_dist=rec.get('ast_dist'),
                                 function_dist=rec.get('function_dist'),
             )
        final_result.append(fp)
    print("Len final_result is {}".format(len(final_result)))

    # 2. and update with the values for number of unique sites and pages for each family
    family_results = db.etl_hits.aggregate([
        { "$match": { "ast_dist": { "$lt": threshold }, "function_dist": { "$lt": threshold } } },
        { "$group": 
             { "_id": { "family": "$control_url" },
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" }
             }
        },
    ], allowDiskUse=True)
    result = { }
    for rec in family_results:
        id = rec['_id']
        family = id.get('family')
        result[family] = { "n_sites": len(rec.get('sites')), "n_pages": len(rec.get('unique_pages')) }

    # 3. merge into final_result dataclasses
    for fp in final_result:
         if fp.control_family not in result: # maybe not any diff functions for a given control?
            continue
         obj = result[fp.control_family]
         fp.n_sites_for_family = obj.get('n_sites')
         fp.n_pages_for_family = obj.get('n_pages')
         rare_site = (fp.n_sites / fp.n_sites_for_family) < 0.05
         rare_page = (fp.n_pages / fp.n_pages_for_family) < 0.05
         rare = rare_site or rare_page
         if fp.n_pages >= 5 and fp.n_sites_for_family >= 5 and fp.n_pages_for_family >= 15 and rare:
             print(fp)


def dump_distances(db, pretty=False, threshold=10.0):
    result = db.etl_hits.aggregate([
        { "$match": { "ast_dist": { "$lt": threshold }, "function_dist": { "$lt": threshold }  } },
        { "$group":
             { "_id": { "control_url": "$control_url",
                        "origin_url": "$origin_url",
                        "cited_on_host": "$cited_on_host" },
               "distances": { "$addToSet": "$ast_dist" },
               "diff_functions": { "$addToSet": "$diff_function" },
               "min_function_dist": { "$min": "$function_dist" },
               "pages": { "$addToSet": "$cited_on_path" }
             }
        },
    ], allowDiskUse=True)

    if pretty:
        dump_pretty(result)
    else:
        headers = ['host', 'min_distance', 'avg_distance', 'max_distance', 'min_function_dist', 'n_pages', 'diff_functions', 'control_url', 'origin_url']
        print('\t'.join(headers))
        for rec in result:
           #print(rec)
           # eg. {'_id': {'control_url': 'https://cdnjs.cloudflare.com/ajax/libs/jquery-mousewheel/3.1.0/jquery.mousewheel.min.js', 'cited_on_host': 'www2.dtwd.wa.gov.au'}, 'distances': [31.32091952673165], 'pages': ['/mureskinstitute/']}
           id = rec.get('_id')
           distances = rec.get('distances')
           l = [ id['cited_on_host'], str(min(distances)), str(mean(distances)), str(max(distances)), 
                 str(rec.get('min_function_dist')), str(len(rec.get('pages'))), 
                 str(sorted(list(rec.get('diff_functions')))), id['control_url'], id['origin_url'] ]
           print('\t'.join(l))

def dump_unresolved_clusters(db, pretty=False, threshold=50.0, want_set=False, min_sites=5):
    result = db.etl_bad_hits.aggregate([
                 { "$match": { "sha256": { "$ne": None } } },
                 { "$group":
                      { "_id": "$sha256",
                        "unique_js": { "$addToSet": "$origin_url" },
                        "sites": { "$addToSet": "$cited_on_host" } 
                      }
                 },
                 { "$sort": { "_id": 1, "unique_js": -1 } }
    ], allowDiskUse=True)
 
    existing_controls = set( db.javascript_controls.distinct('sha256') )  # dont report clusters for which we have an existing control (based on sha256)

    if pretty:
        dump_pretty(result.find())
    else:
        headers = ["sha256", "n_js", "n_sites_observed"]
        if want_set: 
            headers.append("cited_on")
            headers.append("first_origin_url")
        print('\t'.join(headers))
        for rec in filter(lambda v: v['_id'] not in existing_controls, result):
             n_sites = len(rec.get('sites'))
             if n_sites < min_sites:
                 continue
             l = [rec['_id'], str(len(rec.get('unique_js'))), str(n_sites)]
             if want_set:
                 l.append(' '.join(rec.get('sites')))
                 l.append(rec.get('unique_js')[0])
             print('\t'.join(l))

def dump_sites_by_control(db, pretty=False, want_set=False, threshold=10.0):
    result = db.etl_hits.aggregate([ 
                 { "$match": { "$or": [ { "ast_dist": { "$lt": threshold } }, { "function_dist": { "$lt": threshold } } ] } },
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

def dump_control_hits(db, pretty=False, threshold=10.0, control_url=None): # origin must be specified or ValueError
   cntrl = db.javascript_controls.find_one({ 'origin': control_url })
   if cntrl is None:
       raise ValueError('No control matching: {}'.format(control_url))
   hits = db.etl_hits.aggregate([ 
                    { "$match": { "$or": [ { "ast_dist": { "$lt": threshold } }, { "function_dist": { "$lt": threshold } } ] } },
                    { "$match": { "control_url": control_url } },
                    { "$group": { 
                          "_id": { "control_url": "$control_url", "origin_url": "$origin_url" },
                          "changed_functions": { "$addToSet": "$diff_function" },
                          "min_ast": { "$min": "$ast_dist" },
                          "max_ast": { "$max": "$ast_dist" },
                          "min_function_dist": { "$min": "$function_dist" },
                          "max_function_dist": { "$max": "$function_dist" },
                    }},
          ])
   # eg. {'_id': {'control_url': 'XXX/assets/js/lib/jquery.maskedinput.min.js', 
   #         'origin_url': 'XXX/gravityforms/js/jquery.maskedinput.min.js'}, 
   # 'changed_functions': ['b', 'charAt', 'test', 'log', 'String', 'call', 'trigger', 'T', 'u', 'S', 'setTimeout', 'm', 'caret', 
   #  'proxy', 'd', 'get', 'f', 'R', 's', 'c', 'A', 'h', 'v', 'k'], 
   #  'min_ast': 16.431676725154983, 'max_ast': 16.431676725154983, 'min_function_dist': 0.5006882315714638, 'max_function_dist': 0.5006882315714638}
   headers = ['control_url', 'origin_url', 'changed_functions', 'min_ast_dist', 'min_function_dist']
   print('\t'.join(headers))
   for hit in hits:
       id = hit.get('_id')
       data = [ id.get('control_url'), id.get('origin_url'), 
                str(sorted(hit.get('changed_functions'))),
                str(hit.get('min_ast')), str(hit.get('min_function_dist')) ]
       print('\t'.join(data)) 

def list_controls(db, pretty=False):
   headers =  ['control_url', 'provider', 'family', 'release', 'size_bytes', 'sha256', 'md5', 'total_calls', 'total_literals', 'total_ast' ]
   print('\t'.join(headers))
   for c in sorted(db.javascript_controls.find({}), key=lambda c: c.get('family')):
      total_ast = sum(c.get('statements_by_count').values())
      total_literals = sum(c.get('literals_by_count').values())
      total_calls = sum(c.get('calls_by_count').values()) 
      prov = c.get('provider')
      if prov is None:
          prov = ''
      rel = c.get('release')
      if rel is None:
          rel = ''
      l = [c.get('origin'), prov, c.get('family'),
           rel, str(c.get('size_bytes')), c.get('sha256'),
           c.get('md5'), str(total_calls), str(total_literals), str(total_ast)]
      print('\t'.join(l))

a = argparse.ArgumentParser(description="Process results for a given query onto stdout for ingestion in data analysis pipeline")
add_mongo_arguments(a)
add_debug_arguments(a)
a.add_argument("--pretty", help="Use pretty-printed JSON instead of TSV as stdout format", action="store_true")
a.add_argument("--query", help="Run specified query, one of: function_probabilities|unresolved_clusters|distances|sitesbycontrol|functionsbycontrol", type=str, choices=['unresolved_clusters', 'sitesbycontrol', 'functionsbycontrol', 'distances', 'function_probabilities', 'list_controls', 'control_hits'])
a.add_argument("--extra", help="Parameter for query", type=str)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]
if args.query == "functionsbycontrol":
    dump_diff_functions_by_control(db, pretty=args.pretty, control=args.extra, want_set=args.v)
elif args.query == "sitesbycontrol":
    dump_sites_by_control(db, want_set=args.v)
elif args.query == "distances":
    dump_distances(db, pretty=args.pretty, threshold=10.0 if not args.extra else float(args.extra))
elif args.query == "unresolved_clusters":
    dump_unresolved_clusters(db, pretty=args.pretty, threshold=100.0, want_set=args.v) # distances over 100 will be considered for clustering
elif args.query == "function_probabilities":
    dump_rare_diff_functions(db, pretty=args.pretty, threshold=20.0 if not args.extra else float(args.extra)) 
elif args.query == "list_controls":
    list_controls(db, pretty=args.pretty)
elif args.query == "control_hits":
    dump_control_hits(db, pretty=args.pretty, control_url=args.extra)
exit(0)
