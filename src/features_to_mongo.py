#!/usr/bin/python3
import argparse
import json
from kafka import KafkaConsumer
import pymongo 
import argparse

a = argparse.ArgumentParser(description="Report scripts from html-page-stats which are not US/AU geolocated based on any IP associated with the URL hostname")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=1000000000)
a.add_argument("--topic", help="Read analysis results from specified topic [analysis-results]", type=str, default="analysis-results") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [features2mongo]", type=str, default='features2mongo')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [latest]", type=str, default='latest')
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
args = a.parse_args()

group = args.group
start = 'latest'
if len(group) < 1:
    group = None
    start = 'earliest'
consumer = KafkaConsumer(args.topic, group_id=group, auto_offset_reset=start, consumer_timeout_ms=10000, 
                         bootstrap_servers=args.bootstrap, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
mongo = pymongo.MongoClient(args.db, args.port)
db = mongo[args.dbname]

cnt = 0
for message in consumer:
     d = { 'url': message.value.get('id') }
     d.update(**message.value.get('statements_by_count')) 
     db.statements_by_count.insert_one(d)
     d = { 'url': message.value.get('id') }
     calls = message.value.get('calls_by_count')
     for key in set(calls.keys()):  # since $ is not valid for mongo, we just remove it since it is not useful anyway
         if not key.startswith('$') and not key == '_id':
             d[key] = calls[key]
     db.count_by_function.insert_one(d)
     cnt += 1
     if cnt % 1000 == 0:
         print("Processed {} records.".format(cnt))

exit(0)
