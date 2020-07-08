#!/usr/bin/python3
import pymongo
import sys
import re
import argparse
from dataclasses import dataclass, field
from collections import namedtuple
from typing import List
from statistics import mean, stdev
from utils.misc import *
from utils.io import load_controls
from etl_update_probabilities import ControlProbability

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


def dump_distances(db, pretty=False, threshold=10.0):
    result = db.etl_hits.aggregate([
        { "$match": { "ast_dist": { "$lt": threshold }, "function_dist": { "$lt": threshold }  } },
        { "$group":
             { "_id": { "control_url": "$control_url",
                        "origin_url": "$origin_url",
                        "cited_on_host": "$cited_on_host" },
               "distances": { "$addToSet": "$ast_dist" },
               "diff_functions": { "$addToSet": { "$unwind": "$diff_functions", "preserveNullAndEmptyArrays": True } },
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

def dump_unresolved_clusters(db, pretty=False, threshold=50.0, want_set=False, min_sites=15):
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
                 { "$match": { "n_sites": { "$gte": min_sites } }},
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

def dump_subfamily(db, subfamily, pretty=False, exclude_hash_matches=False):
   assert len(subfamily) > 0
   subfamily_regexp = re.compile('.*{}.*'.format(subfamily), re.IGNORECASE)
   cu = [rec.get('origin') for rec in db.javascript_controls.find({ 'subfamily': subfamily_regexp })]
   dump_family(db, subfamily, pretty=pretty, control_urls=cu, exclude_hash_matches=exclude_hash_matches)

def dump_family(db, family, pretty=False, control_urls=None, exclude_hash_matches=False):
   assert len(family) > 0
   if control_urls is None:
       control_urls = [rec.get('origin') for rec in db.javascript_controls.find({ 'family': family })]
   is_first = True
   for u in control_urls:
       assert len(u) > 0
       dump_control_hits(db, u, pretty=pretty, dump_headers=is_first, exclude_hash_matches=exclude_hash_matches)
       is_first = False

def dump_control_hits(db, control_url, pretty=False, dump_headers=True, exclude_hash_matches=False): # origin must be specified or ValueError
   cntrl = db.javascript_controls.find_one({ 'origin': control_url })
   if cntrl is None:
       raise ValueError('No control matching: {}'.format(control_url))
   d = { "control_url": control_url }
   if exclude_hash_matches:
      d.update({ 'sha256_matched': False })
   hits = db.etl_hits.aggregate([ 
                    { "$match": d },
                    { "$unwind": { "path": "$diff_functions", "preserveNullAndEmptyArrays": True } },
                    { "$group": { 
                          "_id": { "control_url": "$control_url", "origin_url": "$origin_url" },
                          "changed_functions": { "$addToSet": "$diff_functions" },
                          "xrefs": { "$addToSet": "$xref" },
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
                          "xrefs": 1,
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
   if dump_headers:
       headers = ['xrefs', 'changed_functions', 'min_ast_dist', 'min_function_dist', 'min_ldist', 
                  "max_literals_not_in_origin", "max_literals_not_in_control", "max_n_diff_literals", 
                  "origin_url", "control_url"]
       print('\t'.join(headers))

   for hit in hits:
       data = [ ' '.join(hit.get('xrefs')), 
                ','.join(sorted(hit.get('changed_functions'))), 
                str(hit.get('min_ast')), 
                str(hit.get('min_fdist')),
                str(hit.get('min_ldist')),
                str(hit.get('max_literals_not_in_origin')),
                str(hit.get('max_literals_not_in_control')),
                str(hit.get('max_n_diff_literals')),
                hit.get('origin_url'),
                control_url
               ]
       print('\t'.join(data)) 

def list_controls(db, pretty=False):
   headers =  ['control_url', 'provider', 'family', 'release', 'size_bytes', 'sha256', 'md5', 'total_calls', 'total_literals', 'total_ast' ]
   print('\t'.join(headers))
   for c, total_ast, ast_vector, function_call_vector, literal_vector in load_controls(db, all_vectors=True):
      total_literals = sum(literal_vector.values())
      total_calls = sum(function_call_vector.values()) 
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

def calc_suspicious(csv_string_iterable):
   suspicious = ['cart', 'password', 'login', 'account', 'submit', 'eval', 'script',
                 'https://', 'http://', 'encode', 'decode', 'URI', 'network', 
                 'dns', 'get', 'post', 'fromCharCode', 'xmlHttpRequest', 'ajax', 
                 'charCodeAt', 'createElement', 'insertBefore', 'appendChild']
   n = 0
   for csv in csv_string_iterable: 
       assert isinstance(csv, str)
       terms = filter(lambda t: t is not None and len(t) > 0, csv.split(","))
       for s in suspicious: # TODO FIXME: not quite right since it will count many times, but oh well....
           cnt = len(list(filter(lambda v: v == True, map(lambda t: s in t.lower(), terms))))
           if cnt > 0:
               #print("{} suspicious terms in {}".format(cnt, csv))
               n += cnt

   return n

def calc_len_diff_literals(string_iterable):
   n = 0
   for s in string_iterable:
       n += len(s)
   return n

def dump_hosts(db, pretty=False, threshold=50.0):
   hits = db.etl_hits.aggregate([
      { "$addFields": { "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] } } },
      { "$match": { "dist_prod": { "$lt": threshold } } },
      { "$unwind": { "path": "$diff_functions", "preserveNullAndEmptyArrays": True } },
      { "$group": {
           "_id": { "control": "$control_url", "host": "$cited_on_host" },
           "pages": { "$addToSet": "$cited_on" },
           "max_diff_literals": { "$max": "$n_diff_literals" },
           "diff_literals": { "$addToSet": "$diff_literals" },
           "diff_functions": { "$addToSet": "$diff_functions" },
      }},
      { "$project": {
           "host": "$_id.host",
           "control_url": "$_id.control",
           "n_pages": { "$size": "$pages" },
           "max_diff_literals": 1,
           "diff_functions": 1,
           "diff_literals": 1,
           "n_diff_functions": { "$size": "$diff_functions" },
      }},
      { "$sort": { "host": 1, "max_diff_literals": -1 } },
   ], allowDiskUse=True)

   print('\t'.join(['unique_pages_visited', 'max_diff_literals', 
                    'n_diff_functions', 'n_suspicious_functions', 'n_suspicious_literals',
                    'len_diff_literals', 'rarest_probability', 'rarest_diff_function', 'host', 'control_url']))
   for hit in hits:
       hit['rarest_probability'], hit['rarest_diff_function'] = calc_rarest_function(db, hit)
       print('\t'.join([str(hit.get('n_pages')), 
                        str(hit.get('max_diff_literals')), 
                        str(hit.get('n_diff_functions')), 
                        str(calc_suspicious(hit.get('diff_functions'))),
                        str(calc_suspicious(hit.get('diff_literals'))),
                        str(calc_len_diff_literals(hit.get('diff_literals'))),
                        str(hit.get('rarest_probability')),
                        hit.get('rarest_diff_function'),
                        hit.get('host'),
                        hit.get('control_url')
       ]))

def calc_rarest_function(db, hit):
   assert hit is not None
   assert 'control_url' in hit and 'diff_functions' in hit
   diff_fns = hit.get('diff_functions', None)
   if diff_fns is None or len(diff_fns) < 1:
       return (None, None)

   min_prob = float('Inf')
   best_fn = None
   for fn in diff_fns:
       doc = db.function_probabilities.find_one({ 'control_url': hit.get('control_url'), 'function_name': fn })
       if doc is None:
           if min_prob > 1.0: # function not known? we consider it not very rare, but will report it if nothing better comes along
               min_prob = 1.0
               best_fn = fn
           continue
      
       doc.pop('_id', None) 
       cp = ControlProbability(**doc)
       prob = cp.subfamily_probability()
       if prob < min_prob:
           best_fn = fn 
           min_prob = prob

   assert min_prob == float('Inf') or (min_prob >= 0.0 and min_prob <= 1.0)
   return (min_prob, best_fn)

def dump_host(db, hostspec, pretty=False, threshold=50.0):
   if hostspec is None:
       raise ValueError('No host specified via --extra!')

   regexp = '.*{}.*'.format(hostspec.replace('.', '\.'))
   hits = db.etl_hits.aggregate([
      { "$addFields": {  "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] }}},
      { "$match": { "dist_prod": { "$lt": threshold }}},
      { "$match": { "sha256_matched": False }},   # sha256 matches are boring, dont want them
      { "$match": { "cited_on_host": { "$regex": regexp } } },
      { "$group": { "_id": { "xref": "$xref" },
                    "hit": { "$first": "$$ROOT" },
      }},
      { "$project": { "origin_js_id": 0 } }, # we dont want this since xref is better for use with meld.py
      { "$sort": { "dist_prod": 1 } },
   ])
   first = True
   for hit in hits:
       h = hit.get('hit')
       h['rarest_function_probability'], h['rarest_function'] = calc_rarest_function(db, h)
       if first:
           first = False
           fields = list(filter(lambda k: not k in ['_id'], sorted(h.keys())))
           fields.extend(['rarest_function_probability', 'rarest_function'])
           print('\t'.join(fields))
       v = []
       for k in fields:
           v.append(str(h.get(k, None)))
       print('\t'.join(v))

if __name__ == "__main__": 
   a = argparse.ArgumentParser(description="Process results for a given query onto stdout for ingestion in data analysis pipeline")
   add_mongo_arguments(a, default_user='ro') # running queries is basically always read-only role
   add_debug_arguments(a)
   a.add_argument("--pretty", help="Use pretty-printed JSON instead of TSV as stdout format", action="store_true")
   a.add_argument("--query", help="Run specified query, one of above choices: [None]", type=str, 
                          choices=['unresolved_clusters', 'sites_by_control', 'distances', "subfamily_hits",
                                   'list_controls', 'control_hits', 'family_hits', 'hosts', 'host'])
   a.add_argument("--extra", help="Parameter for query", type=str)
   a.add_argument("--threshold", help="Maximum distance product (AST distance * function call distance) to permit [100.0]", type=float, default=100.0)
   a.add_argument("--exclude-hash-matches", help="Exclude hits which hash match a control [False]", action="store_true")
   args = a.parse_args()

   mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
   db = mongo[args.dbname]
   if args.query == "sites_by_control":
       dump_sites_by_control(db, want_set=args.v)
   elif args.query == "distances":
       dump_distances(db, pretty=args.pretty, threshold=args.threshold)
   elif args.query == "unresolved_clusters":
       dump_unresolved_clusters(db, pretty=args.pretty, threshold=args.threshold, want_set=args.v) # distances over 100 will be considered for clustering
   elif args.query == "list_controls":
       list_controls(db, pretty=args.pretty)
   elif args.query == "control_hits":
       dump_control_hits(db, args.extra, pretty=args.pretty, control_url=args.extra, exclude_hash_matches=args.exclude_hash_matches)
   elif args.query == "family_hits":
       dump_family(db, args.extra, pretty=args.pretty, exclude_hash_matches=args.exclude_hash_matches)
   elif args.query == "subfamily_hits":
       dump_subfamily(db, args.extra, pretty=args.pretty, exclude_hash_matches=args.exclude_hash_matches)
   elif args.query == "hosts":
       dump_hosts(db, pretty=args.pretty, threshold=args.threshold)
   elif args.query == "host": # host to dump is supplied in args.extra (partial matching is performed) eg. blah.com.au will match <anything>blah.com.au
       dump_host(db, args.extra, pretty=args.pretty, threshold=args.threshold)
   exit(0)
