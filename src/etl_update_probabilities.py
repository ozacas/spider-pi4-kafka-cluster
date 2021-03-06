#!/usr/bin/python3
import pymongo
import sys
import argparse
import json
import pylru
from utils.misc import *
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List

@dataclass
class ControlProbability:
   control_url: str
   function_name: str        # name of function call or "N/A" if anonymous
   sites: List[str] = field(default_factory=list)  # can be a large amount of memory!
   n_sites: int = 0          # number of sites with a hit to control_url and DE function function_name
   n_pages: int = 0          # number of unique pages (URL) with a hit to control_url and DE function function_name
   n_sites_subfamily: int = 0  # as per n_sites BUT FOR ALL ARTEFACTS in the control *subfamily* eg. password-strength-meter
   n_pages_subfamily: int = 0  # as per n_pages BUT FOR ALL ARTEFACTS in the control subfamily 

   def subfamily_probability(self):
       assert self.n_sites_subfamily > 0
       assert self.n_sites <= self.n_sites_subfamily
       return self.n_sites / self.n_sites_subfamily

   def page_probability(self):
       assert self.n_pages_subfamily > 0
       assert self.n_pages <= self.n_pages_subfamily
       return self.n_pages / self.n_pages_subfamily

@dataclass
class SubFamilyProbability:
   function_name: str
   subfamily: str
   n_sites: int = 0
   n_pages: int = 0 

def add_aggregate_stages(threshold, family=None):
    assert threshold >= 0.0

    l = [ 
        { "$addFields": { "dist_prod": { "$multiply": [ "$ast_dist", "$function_dist" ] } } },
        { "$match":     { "dist_prod": { "$lt": threshold } } },
        { "$unwind":    { "path":  "$diff_functions" } }  ]
    prune = { "_id": 0 }

    if family is None:
        l.extend([
        { "$group": 
             { "_id": { "family": "$control_url", "diff_function": "$diff_functions" },
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" },
             }
        }])
        label = 'control_url'
    else:
        assert isinstance(family, str) and len(family) > 0
        l.extend([
        { "$group":
             { "_id": { "family": "$subfamily", "diff_function": "$diff_functions" },
               "sites": { "$addToSet": "$cited_on_host" },
               "unique_pages": { "$addToSet": "$cited_on" },
             }
        }])
        prune.update({ "sites": 0 })
        label = 'subfamily'

    l.extend([
        { "$project": { # POST-CONDITION: exactly the same fields as for FunctionProbability model
            label: "$_id.family",
            "function_name": "$_id.diff_function",
            "n_sites": { "$size": "$sites" },
            "n_pages": { "$size": "$unique_pages" },
            "sites": 1 
        }},
        { "$project": prune
        },
        { "$sort": { label: 1, "function_name": 1 } } ])
    return l

def subfamily_probability(family, threshold, family_cache, matching_urls):
    assert isinstance(family, str)
    assert family is not None and len(family) > 0
    assert family_cache is not None
    assert isinstance(matching_urls, list) and len(matching_urls) > 0  # so that mongo query is correctly formatted
    
    l = [ { "$match": { "sha256_matched": False } },   # nothing interesting if sha256 matches... so filter upfront for speed
          { "$match": { "control_url": { "$in": matching_urls } } },
          { "$addFields": {
                  "n_diff_functions": { "$size": "$diff_functions" },
                  "subfamily":  family, 
          } },  # only compute family stats for those hits with diff_functions NOT all sites hitting family
          { "$match": { "n_diff_functions": { "$gt": 0 } } }
        ]
    l.extend(add_aggregate_stages(threshold, family=family))
    if family_cache is not None and family in family_cache:
        return family_cache[family]
    ret = db.etl_hits.aggregate(l)
    family_cache[family] = ret
    return ret

def save_function_probabilities(db, control, threshold=50.0, family_cache=None, verbose=False, matching_urls=None):
    # 1. compute function probability for all reported functions which have an unexpected number of calls
    l = [{ "$match": { "control_url": control } }]
    l.extend(add_aggregate_stages(threshold))
    sites_by_function_count = db.etl_hits.aggregate(l)

    final_result = [ControlProbability(**rec) for rec in sites_by_function_count]
    print("{} unique differentially expressed functions for {}".format(len(final_result), control))

    if len(final_result) == 0:
        db.function_probabilities.delete_many({ 'control_url': control })
        print("No DE functions related to {} - skipping".format(control))
        return

    # 2. lookup control_url to identify software family it relates to eg. bootstrap and then compute counts for the entire family
    control_doc = db.javascript_controls.find_one({ 'origin': control })
    assert control_doc is not None

    # 3. and then find all controls in the same software family and compute the family-wide stats
    subfamily = control_doc.get('subfamily')
    count_s2f = subfamily_probability(subfamily, threshold, family_cache, matching_urls)
    family_function_counts = { rec['function_name']: SubFamilyProbability(**rec) for rec in count_s2f } 
    print("{} unique functions in the {} subfamily.".format(len(family_function_counts.keys()), subfamily))

    print("Updating db.function_probabilities for {}".format(control))
    db.function_probabilities.delete_many({ 'control_url': control })
    for fp in final_result:
        assert len(fp.function_name) > 0
        family_counts = family_function_counts.get(fp.function_name)
        assert family_counts is not None  # since function_name must be part of the family! 
        fp.n_sites_subfamily = family_counts.n_sites
        fp.n_pages_subfamily = family_counts.n_pages
        assert fp.n_sites <= fp.n_sites_subfamily
        assert fp.n_pages <= fp.n_pages_subfamily
        d = asdict(fp)
        d.pop('sites', None)    # NB: dont want sites in Mongo - waste of storage since we can readily get it from db.etl_hits
        if verbose:
            print(fp)
        db.function_probabilities.insert_one(d)

if __name__ == "__main__":
    a = argparse.ArgumentParser(description="Update the function and family probabilities for all hits currently in the database. Used to identify rare functions") 
    add_mongo_arguments(a, default_user='rw')   # read-write is required: will update the database with new function probabilities for all controls with hits
    add_debug_arguments(a)
    a.add_argument("--threshold", help="Maximum distance product (AST distance * function call distance) to permit [100.0]", type=float, default=100.0)
    args = a.parse_args()

    mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
    db = mongo[args.dbname]
    controls_with_hits = db.etl_hits.distinct('control_url')
    print("Found {} unique controls with good hits. Started at {}".format(len(controls_with_hits), datetime.utcnow()))
    s2c = { }
    c2s = { }
    for c in filter(lambda c: c.get('origin') in controls_with_hits, db.javascript_controls.find()):
        subfamily = c.get('subfamily')
        u = c.get('origin')
        assert len(u) > 0
        if not subfamily in s2c:
            s2c[subfamily] = []
        s2c[subfamily].append(u)
        c2s[u] = subfamily

    for control in controls_with_hits:
        save_function_probabilities(db, control, 
                                    threshold=args.threshold, 
                                    family_cache=pylru.lrucache(50),
                                    matching_urls=s2c[c2s[control]],
                                    verbose=args.v)
    print("Run completed: {}".format(str(datetime.utcnow())))
    exit(0)
