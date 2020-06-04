#!/usr/bin/python3
import pymongo
import sys
import argparse
from utils.features import identify_control_subfamily
from utils.misc import add_mongo_arguments, add_debug_arguments
from utils.io import load_controls

if __name__ == "__main__": 
    a = argparse.ArgumentParser(description="Fix subfamily for all controls")
    add_mongo_arguments(a, default_user='rw') # need read-write access to update the subfamily for all controls
    add_debug_arguments(a)
    a.add_argument("--dry-run", help="Do not update the database", action="store_true")
    args = a.parse_args()

    mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
    db = mongo[args.dbname]
    all_controls = load_controls(db, verbose=args.v)
    subfamilies = []
    for t in sorted(all_controls, key=lambda t: t[0].get('family')):
        u = t[0].get('origin')
        u2, subfamily = identify_control_subfamily(u)
        assert u == u2
        subfamilies.append(( subfamily, u, t )) 

    print("Sorted subfamilies list by AST vector sum:")
    for s in sorted(subfamilies, key=lambda t: t[2][1]):
        print([s[0], s[2][1], s[1]])

    d = { t[1]: t[0] for t in subfamilies }

    if not args.dry_run:
        for t in all_controls:
            assert isinstance(t[0], dict)
            u = t[0].get('origin')
            assert u in d
            if args.v:          
                print("Updating subfamily {} for {}".format(d[u], u))
            ret = db.javascript_controls.find_one_and_update({ 'origin': u }, { '$set': { 'subfamily': d[u] } }, upsert=True)
            assert ret is not None

    exit(0)
