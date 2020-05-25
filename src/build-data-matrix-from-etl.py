#!/usr/bin/python3
import pymongo
import sys
import argparse
from dataclasses import dataclass, field
from collections import namedtuple
from typing import List
from statistics import mean, stdev
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
    #print("Applying threshold of {} to hits (either ast_dist or function_dist below threshold)".format(threshold))

    sites_by_function_count = db.etl_hits.aggregate([
        { "$addFields": {
            "dist_prod" : { "$multiply": [ "$ast_dist", "$function_dist" ] }
        }},
        { "$match": { "dist_prod": { "$lt": threshold } } },
        { "$group": 
             { "_id": { "family": "$control_url", "diff_function": "$diff_function" } ,
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" },
               "ast_dist": { "$push": "$ast_dist" },
               "function_dist": { "$push": "$function_dist" },
             }
        },
        { "$project": { # POST-CONDITION: exactly the same fields as for FunctionProbability model
            "_id": 0,
            "control_family": "$_id.family",
            "function_name": "$_id.diff_function",
            "n_sites": { "$size": "$sites" },
            "n_pages": { "$size": "$unique_pages" },
            "sites": 1,
            "ast_dist": 1,
            "function_dist": 1,
        }},
        { "$sort": { "control_family": 1, "function_name": 1 } },
    ], allowDiskUse=True)

    final_result = [FunctionProbability(**rec) for rec in sites_by_function_count]
    #print("Len final_result is {}".format(len(final_result)))

    # 2. and update with the values for number of unique sites and pages for each family
    family_results = db.etl_hits.aggregate([
        { "$addFields": {
            "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] }
        }},
        { "$match": { "dist_prod": { "$lt": threshold } }},
        { "$group": 
             { "_id": { "family": "$control_url" },
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" }
             }
        },
        { "$project": {
             "family": "$_id.family",
             "n_sites": { "$size": "$sites" },
             "n_pages": { "$size": "$unique_pages" },
        }},
    ], allowDiskUse=True)
    result = { rec['family']: rec for rec in family_results }

    # 3. merge into final_result dataclasses
    print('\t'.join(['control_url', 'function', 'n_sites', 'n_sites_with_control', 'n_pages', 'n_pages_with_control', 'ast_max', 'ast_stdev', 'function_max', 'function_stdev']))

    for fp in final_result:
         if fp.control_family not in result: # maybe not any diff functions for a given control?
            continue
         obj = result[fp.control_family]
         fp.n_sites_for_family = obj.get('n_sites')
         fp.n_pages_for_family = obj.get('n_pages')
         assert fp.n_pages_for_family > 0 and fp.n_sites_for_family <= fp.n_pages_for_family
         rare_site = (fp.n_sites / fp.n_sites_for_family) < 0.05
         rare_page = (fp.n_pages / fp.n_pages_for_family) < 0.05
         rare = rare_site or rare_page
         if fp.n_pages >= 5 and fp.n_sites_for_family >= 5 and fp.n_pages_for_family >= 15 and rare:
             assert len(fp.ast_dist) > 0
             assert len(fp.function_dist) > 0
             l = [fp.control_family, fp.function_name, str(fp.n_sites), str(fp.n_sites_for_family), str(fp.n_pages), str(fp.n_pages_for_family),
                  str(max(fp.ast_dist)), str(stdev(fp.ast_dist)), str(max(fp.function_dist)), str(stdev(fp.function_dist)) ]
             print('\t'.join(l))


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
                 { "$project": {
                      "_id": 0,
                      "sha256": "$_id",
                      "unique_js": 1,
                      "sites":   1,
                      "n_sites": { "$size": "$sites" },
                      "n_unique_js": { "$size": "$unique_js" },
                 } } ,
                 { "$sort": { "n_sites": -1, "n_unique_js": -1 } }
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
        for rec in filter(lambda v: v['sha256'] not in existing_controls, result):
             n_sites = rec.get('n_sites')
             n_js = rec.get("n_unique_js")
             if n_sites < min_sites:
                 continue
             l = [rec['sha256'], str(n_js), str(n_sites)]
             if want_set:
                 l.append(' '.join(rec.get('sites')))
                 l.append(rec.get('unique_js')[0])
             print('\t'.join(l))

def dump_sites_by_control(db, pretty=False, want_set=False, threshold=10.0):
    control2bytes = { k.get('origin'): k.get('size_bytes') for k in 
                         db.javascript_controls.find({}, 
                                                     { 'statements_by_count': 0, 'calls_by_count': 0, 'literals_by_count': 0 })}

    result = db.etl_hits.aggregate([ 
                 { "$match": { "$or": [ { "ast_dist": { "$lt": threshold } }, { "function_dist": { "$lt": threshold } } ] } },
                 { "$group": { 
                        "_id": "$control_url", 
                        "set": { "$addToSet": "$cited_on_host" },
                        "min_ast_dist": { "$min": "$ast_dist" },
                        "max_ast_dist": { "$max": "$ast_dist" },
                        "min_function_dist": { "$min": "$function_dist" },
                        "max_function_dist": { "$max": "$function_dist" },
                        "min_literal_dist": { "$min": "$literal_dist" },
                        "max_literal_dist": { "$max": "$literal_dist" },
                      } 
                 },
                 { "$project": {
                       "_id": 0,
                       "control_url": "$_id",
                       "hosts": "$set",
                       "n_hosts": { "$size": "$set" },
                       "min_ast_dist": 1,
                       "max_ast_dist": 1,
                       "min_function_dist": 1,
                       "max_function_dist": 1,
                       "min_literal_dist": 1,
                       "max_literal_dist": 1
                 }},
                 { "$sort": { "n_hosts": -1, "control_url": 1,  "max_ast_dist": -1 } }
    ], allowDiskUse=True)

    if pretty:
        dump_pretty(result.find())
    else:
        headers = ['control_url', 'control_bytes', 'n_sites', 'min_ast_dist', 'max_ast_dist', 'min_function_dist', 'max_function_dist', 'min_literal_dist', 'max_literal_dist']
        if want_set:
            headers.append('site_set')
        print('\t'.join(headers))
        nt = namedtuple('Record', 'control_url n_hosts min_ast_dist max_ast_dist min_function_dist max_function_dist min_literal_dist max_literal_dist hosts')
        for rec in result:
            t = nt(**rec)
            l = [t.control_url, str(control2bytes.get(t.control_url)), 
                 str(t.n_hosts), str(t.min_ast_dist), str(t.max_ast_dist), 
                 str(t.min_function_dist), str(t.max_function_dist), 
                 str(t.min_literal_dist), str(t.max_literal_dist)]
            if want_set:
                l.append(','.join(t.hosts))
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

def dump_control_hits(db, pretty=False, threshold=50.0, control_url=None, max_dist_prod=200.0): # origin must be specified or ValueError
   cntrl = db.javascript_controls.find_one({ 'origin': control_url })
   if cntrl is None:
       raise ValueError('No control matching: {}'.format(control_url))
   hits = db.etl_hits.aggregate([ 
                    { "$match": { "control_url": control_url } },
                    { "$addFields": { "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] } } },
                    { "$match": { "dist_prod": { "$lt": threshold } } },
                    { "$group": { 
                          "_id": { "control_url": "$control_url", "origin_url": "$origin_url" },
                          "changed_functions": { "$addToSet": "$diff_function" },
                          "min_ast": { "$min": "$ast_dist" },
                          "max_ast": { "$max": "$ast_dist" },
                          "min_function_dist": { "$min": "$function_dist" },
                          "max_function_dist": { "$max": "$function_dist" },
                          "min_literal_dist":  { "$min": "$literal_dist" },
                          "max_literal_dist":  { "$max": "$literal_dist" },
                          "max_literals_not_in_origin": { "$max": "$literals_not_in_origin" },
                          "max_literals_not_in_control": { "$max": "$literals_not_in_control" },
                          "max_n_diff_literals": { "$max": "$n_diff_literals" },
                    }},
                    { "$project": {
                          "_id": 0,
                          "control_url": "$_id.control_url",
                          "origin_url": "$_id.origin_url",
                          "min_ast": "$min_ast",
                          "max_ast": "$max_ast",
                          "min_fdist": "$min_function_dist",
                          "max_fdist": "$max_function_dist",
                          "min_ldist": "$min_literal_dist",
                          "max_ldist": "$max_literal_dist",
                          "max_literals_not_in_origin": 1,
                          "max_literals_not_in_control": 1,
                          "max_n_diff_literals": 1,
                          "changed_functions": 1
                    }},
                    { "$sort": { "min_ast": 1, "min_fdist": 1 } }
          ], allowDiskUse=True)
   headers = ['control_url', 'origin_url', 'changed_functions', 
              'min_ast_dist', 'min_function_dist', 'min_ldist', 
              "max_literals_not_in_origin", "max_literals_not_in_control", "max_n_diff_literals"]

   print('\t'.join(headers))
   for hit in hits:
       if hit.get('min_ast') * hit.get('min_fdist') * hit.get('min_ldist') > max_dist_prod: # usually bad hits have this characteristic
           continue

       data = [ hit.get('control_url'), 
                hit.get('origin_url'), 
                ','.join(sorted(hit.get('changed_functions'))), 
                str(hit.get('min_ast')), 
                str(hit.get('min_fdist')),
                str(hit.get('min_ldist')),
                str(hit.get('max_literals_not_in_origin')),
                str(hit.get('max_literals_not_in_control')),
                str(hit.get('max_n_diff_literals'))
               ]
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

def dump_hosts(db, pretty=False, threshold=50.0):
   hits = db.etl_hits.aggregate([
      { "$addFields": { "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] } } },
      { "$match": { "dist_prod": { "$lt": threshold } } },
      { "$group": {
           "_id": { "host": "$cited_on_host" },
           "controls": { "$addToSet": "$control_url" },
           "pages": { "$addToSet": "$cited_on" },
      }},
      { "$project": {
           "host": "$_id.host",
           "n_controls": { "$size": "$controls" },
           "n_pages": { "$size": "$pages" },
      }},
      { "$sort": { "host": 1, "n_controls": -1 } },
   ], allowDiskUse=True)

   print('\t'.join(['host', 'n_controls_hit', 'unique_pages_visited']))
   for hit in hits:
       print('\t'.join([hit.get('host'), str(hit.get('n_controls')), str(hit.get('n_pages'))]))

def dump_host(db, hostspec, pretty=False, threshold=50.0):
   regexp = '.*{}.*'.format(hostspec.replace('.', '\.'))
   hits = db.etl_hits.aggregate([
      { "$addFields": {  "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] }}},
      { "$match": { "dist_prod": { "$lt": threshold }}},
      { "$match": { "cited_on_host": { "$regex": regexp } } },
      { "$group": { "_id": { "xref": "$xref" },
                    "hit": { "$first": "$$ROOT" },
      }},
      { "$sort": { "dist_prod": 1 } },
   ])
   first = True
   for hit in hits:
       h = hit.get('hit')
       if first:
           first = False
           fields = list(filter(lambda k: not k in ['_id'], sorted(h.keys())))
           print('\t'.join(fields))
       v = []
       for k in fields:
           v.append(str(h.get(k, None)))
       print('\t'.join(v))
 
a = argparse.ArgumentParser(description="Process results for a given query onto stdout for ingestion in data analysis pipeline")
add_mongo_arguments(a, default_user='ro') # running queries is basically always read-only role
add_debug_arguments(a)
a.add_argument("--pretty", help="Use pretty-printed JSON instead of TSV as stdout format", action="store_true")
a.add_argument("--query", help="Run specified query, one of: function_probabilities|unresolved_clusters|distances|sitesbycontrol|functionsbycontrol", type=str, choices=['unresolved_clusters', 'sitesbycontrol', 'functionsbycontrol', 'distances', 'function_probabilities', 'list_controls', 'control_hits', 'hosts', 'host'])
a.add_argument("--extra", help="Parameter for query", type=str)
a.add_argument("--threshold", help="Maximum distance to permit [50.0]", type=float, default=50.0)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]
if args.query == "functionsbycontrol":
    dump_diff_functions_by_control(db, pretty=args.pretty, control=args.extra, want_set=args.v)
elif args.query == "sitesbycontrol":
    dump_sites_by_control(db, want_set=args.v)
elif args.query == "distances":
    dump_distances(db, pretty=args.pretty, threshold=args.threshold)
elif args.query == "unresolved_clusters":
    dump_unresolved_clusters(db, pretty=args.pretty, threshold=args.threshold, want_set=args.v) # distances over 100 will be considered for clustering
elif args.query == "function_probabilities":
    dump_rare_diff_functions(db, pretty=args.pretty, threshold=args.threshold)
elif args.query == "list_controls":
    list_controls(db, pretty=args.pretty)
elif args.query == "control_hits":
    dump_control_hits(db, pretty=args.pretty, control_url=args.extra)
elif args.query == "hosts":
    dump_hosts(db, pretty=args.pretty, threshold=args.threshold)
elif args.query == "host": # host to dump is supplied in args.extra (partial matching is performed) eg. blah.com.au will match <anything>blah.com.au
    dump_host(db, args.extra, pretty=args.pretty, threshold=args.threshold)
exit(0)
