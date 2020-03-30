#!/usr/bin/python3
import argparse
import json
from kafka import KafkaConsumer
import pymongo 
import argparse
import signal
import sys
from utils.models import Password
from utils.features import safe_for_mongo

a = argparse.ArgumentParser(description="Read analysis results kafka topic and ETL into MongoDB")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=1000000000)
a.add_argument("--topic", help="Read analysis results from specified topic [analysis-results]", type=str, default="analysis-results") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [features2mongo]", type=str, default='features2mongo')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [latest]", type=str, default='latest')
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="MongoDB RBAC username to use (readWrite access required)", type=str, required=True)
a.add_argument("--password", help="MongoDB password for user", type=Password, default=Password.DEFAULT)
args = a.parse_args()

group = args.group
start = 'latest'
if len(group) < 1:
    group = None
    start = 'earliest'
consumer = KafkaConsumer(args.topic, group_id=group, auto_offset_reset=start, consumer_timeout_ms=10000, 
                         bootstrap_servers=args.bootstrap, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]
cnt = 0

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Ctrl-C pressed. Terminating...")
    mongo.close()
    consumer.close()
    sys.exit(0)
 
signal.signal(signal.SIGINT, cleanup)
for message in consumer:
    u = message.value.get('id')
    d = { 'url': u }
    d.update(**message.value.get('statements_by_count')) 
    db.statements_by_count.insert_one(d)
    d = { 'url': u }
    calls = safe_for_mongo(message.value.get('calls_by_count'))
    calls.pop('_id', None)           # not wanted since already in d
    d.update(calls)
    db.count_by_function.insert_one(d)
    cnt += 1
    if cnt % 1000 == 0:
        print("Processed {} records.".format(cnt))
cleanup()
exit(0)
