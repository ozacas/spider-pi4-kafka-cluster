#!/usr/bin/python3
import pymongo
import argparse
import json
from utils.misc import add_mongo_arguments
from utils.models import Password

a = argparse.ArgumentParser(description="Remove controls (and all associated data) which are too small for vector analysis")
add_mongo_arguments(a, default_access="read-write", default_user='rw')
a.add_argument('--json', help='Mongo DB query string to find controls and associated data eg. { "size_bytes": 1500 }', type=str, required=True)
a.add_argument('--y', help="Perform deletion (otherwise will exit after reporting what will be deleted", action='store_true')
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

# remove small controls and all hits as they are too small to match correctly and generate lots of false positives.
# need to remove from: db.javascript_controls
hits = db.javascript_controls.find(json.loads(args.json))
control_urls = [hit['origin'] for hit in hits]
print("Found {} control artefacts to purge all related data from".format(len(control_urls)))

if len(control_urls) == 0:
   print("No matching controls to delete!")
   exit(0)

if not args.y:
   exit(0)

# db.javascript_control_code
for u in control_urls:
   print("Purging data related to".format(u))
   result = db.javascript_control_code.delete_one({ 'origin': u })
   assert result.deleted_count == 1
   result = db.javascript_controls_summary.delete_one({ 'origin': u })
   assert result.deleted_count == 1
   result = db.javascript_controls.delete_one({ 'origin': u })
   assert result.deleted_count == 1
   result = db.vet_against_control.delete_many({ 'control_url': u })
   assert result.deleted_count >= 0  # may be zero if not spidered content hits the control
   result = db.etl_hits.delete_many({ 'control_url': u })
   print("Deleted {} hits to {}".format(result.deleted_count, u))
   result = db.etl_bad_hits.delete_many({ 'control_url': u })
   print("Deleted {} bad hits to {}".format(result.deleted_count, u))

print("All done! Phew....")
exit(0)
