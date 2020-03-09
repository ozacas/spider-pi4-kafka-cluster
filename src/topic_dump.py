#!/usr/bin/python3
from kafka import KafkaConsumer
import json
import argparse

a = argparse.ArgumentParser(description="Read messages from the specified topic and dump to stdout in one-record-per-line JSON format")
a.add_argument("--topic", help="Kafka topic to get visited JS summary", type=str, required=True)
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=int, default=1000000000)
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [None]", type=str, default=None)
a.add_argument("--v", help="Debug verbosely", action="store_true")
args = a.parse_args()

print("Using consumer group: {}".format(args.group))
print("Reading messages from {}".format(args.topic))
print("Bootstrap kafka server {}".format(args.bootstrap))

if args.group is None:
    position = 'earliest'
else:
    position = 'latest'
consumer = KafkaConsumer(args.topic, group_id=args.group, value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                         key_deserializer=lambda m: m.decode('utf-8'), auto_offset_reset=position,
                         bootstrap_servers=args.bootstrap, consumer_timeout_ms=10000)

cnt = 0
for message in consumer:
    print(json.dumps(message, separators=(',', ':'), indent=None))
    cnt += 1
    if cnt >= args.n:
        break
