#!/usr/bin/python3
import argparse
import json
from kafka import KafkaConsumer, KafkaProducer
from utils.misc import *
from urllib.parse import urlparse
from datetime import datetime
from collections import defaultdict

a = argparse.ArgumentParser(description="Add hosts which have been spidered a lot (more than 100 pages) to populate the long-term blacklist")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='html-page-stats',
                    default_start='earliest',
                    default_group='populate-long-term-blacklist',
                    default_to='kafkaspider-long-term-disinterest')
add_debug_arguments(a)
a.add_argument("--dry-run", help="Dry-run, dont send to kafka topic [False]", action="store_true", default=False)
a.add_argument("--threshold", help="Add site if more than X pages seen [80]", type=int, default=80)
args = a.parse_args()

consumer = KafkaConsumer(args.consume_from, consumer_timeout_ms=10 * 1000,
                         bootstrap_servers=args.bootstrap,
                         group_id=args.group,
                         auto_offset_reset=args.start,
                         enable_auto_commit=not args.dry_run, # dont update consumer offset if dry-run specified
                         value_deserializer=json_value_deserializer() )
producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)

sites = defaultdict(int)
n = 0
print("Reading pages visited from {}".format(args.consume_from))
for message in consumer:
    url = message.value.get('url')
    up = urlparse(url)
    h = up.hostname
    sites[h] += 1 if h is not None else 0
    n += 1
    if n % 10000 == 0:
        print("Processed {} records (seen {} sites).".format(n, len(sites)))

print("Saving sites with more than {} visted pages to long-term disinterest list....".format(args.threshold))
n_added = 0
do_it = not args.dry_run
for host, n_seen in sites.items():
   # eg. {"hostname":"www.qld.gov.au","n_pages":139,"date":"04/09/2020"}
   if n_seen > args.threshold:
       d = { 'hostname': host, 'n_pages': n_seen, 'date': str(datetime.utcnow().strftime("%m/%d/%Y")) }
       if args.v:
           print(d)
       if do_it:
           producer.send(args.to, d)
           n_added += 1

print("Added {} entries to long-term blacklist (processed total {} records).".format(n_added, n))
exit(0)
