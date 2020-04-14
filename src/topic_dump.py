#!/usr/bin/python3
from kafka import KafkaConsumer
import json
import argparse

a = argparse.ArgumentParser(description="Read messages from the specified topic and dump to stdout in one-record-per-line JSON format")
a.add_argument("--topic", help="Kafka topic to read", type=str, required=True)
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=int, default=float('Inf'))
a.add_argument("--start", help="Where to start reading messages from (latest|earliest) [latest]", choices=['earliest', 'latest'], default='latest')
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [None]", type=str, default=None)
a.add_argument("--tail", help="Behave as per 'tail -f' on specified topic", action="store_true")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--value-only", help="Dont report metadata associated with each message [False]", action="store_true")
args = a.parse_args()

print("Using consumer group: {}".format(args.group))
print("Reading messages from {} {}".format(args.topic, args.start))
print("Bootstrap kafka server {}".format(args.bootstrap))
timeout = 10 * 1000; # 10s consumer timeout by default
if args.tail:
   timeout = float('Inf')

consumer = KafkaConsumer(args.topic, group_id=args.group, auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                         key_deserializer=lambda m: m.decode('utf-8') if m else None, 
                         bootstrap_servers=args.bootstrap, consumer_timeout_ms=timeout)

cnt = 0
for message in consumer:
    print(json.dumps(message if not args.value_only else message.value, separators=(',', ':'), indent=None))
    cnt += 1
    if cnt >= args.n:
        break

exit(0)
