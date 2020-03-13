#!/usr/bin/python3
import argparse
import json
from kafka import KafkaConsumer
import pymongo 

consumer = KafkaConsumer('analysis-results', group_id='features2mongo', auto_offset_reset='earliest', consumer_timeout_ms=10000, 
                         bootstrap_servers='kafka1', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
mongo = pymongo.MongoClient('pi1', 27017)
db = mongo['au_js']

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
