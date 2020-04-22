import pymongo
import json
import argparse
from kafka import KafkaConsumer
from utils.models import JavascriptArtefact
from utils.features import find_script, safe_for_mongo
from utils.io import *
from utils.misc import *

a = argparse.ArgumentParser(description="Temporary script to fix up incorrect layout of two mongo collections")
add_kafka_arguments(a, consumer=True)
add_mongo_arguments(a, default_access="read-write")
add_debug_arguments(a)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]
consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, consumer_timeout_ms=10 * 1000,
                         group_id=args.group, auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

n = n_bad = 0
for message in consumer:
    rec = message.value
    if args.v:
       print(rec)
    try:
       assert 'calls_by_count' in rec
       assert 'statements_by_count' in rec
       assert 'url' in rec and rec['url'] is not None
       assert 'origin' in rec and rec['origin'] is not None
    except AssertionError:
       print(rec)
       n_bad += 1
       continue

    jsr = JavascriptArtefact(url=rec['url'], origin=rec['origin'], sha256=rec.get('sha256', ''), md5=rec.get('md5', ''), inline=rec.get('inline')) 
    script, url_entry = find_script(db, jsr.url, want_code=False)
    if script is None or not '_id' in script:
        print(script)
        print(url_entry)
        n_bad += 1
        continue
    js_id = script.get('_id')
    save_ast_vector(db, jsr, rec['statements_by_count'], js_id=js_id)
    save_call_vector(db, jsr, rec['calls_by_count'], js_id=js_id)

    n += 1
    if n % 1000 == 0:
        print("Processed {} records.".format(n))
    if n >= args.n:
        break

print("Processed {} record, found {} bad.".format(n, n_bad))
exit(0)
