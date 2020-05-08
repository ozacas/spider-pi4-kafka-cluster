#!/usr/bin/python3
import argparse
import json
from kafka import KafkaConsumer, KafkaProducer
from utils.misc import add_kafka_arguments, add_debug_arguments
from urllib.parse import urlparse
from datetime import datetime

a = argparse.ArgumentParser(description="Add hosts which have been spidered a lot (more than 100 pages) to populate the long-term blacklist")
add_kafka_arguments(a,
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='html-page-stats',
                    default_group='populate-long-term-blacklist',
                    default_to='kafkaspider-long-term-disinterest')
add_debug_arguments(a)
args = a.parse_args()

consumer = KafkaConsumer(args.consume_from, consumer_timeout_ms=10 * 1000,
                         bootstrap_servers=args.bootstrap,
                         group_id=args.group,
                         auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')) )
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, separators=(',', ':')).encode('utf-8'),
                         bootstrap_servers=args.bootstrap)

sites = { }
n = 0
print("Reading pages visited from {}".format(args.consume_from))
for message in consumer:
    url = message.value.get('url')
    up = urlparse(url)
    h = up.hostname
    if h is not None:
       if h not in sites:
           sites[h] = 0
       sites[h] += 1
    n += 1
    if n % 10000 == 0:
        print("Processed {} records (seen {} sites).".format(n, len(sites)))

print("Saving sites with more than 100 pages to long-term disinterest list....")
n_added = 0
for host, n_seen in sites.items():
   # eg. {"hostname":"www.qld.gov.au","n_pages":139,"date":"04/09/2020"}
   if n_seen > 100:
       d = { 'hostname': host, 'n_pages': n_seen, 'date': str(datetime.utcnow().strftime("%m/%d/%Y")) }
       if args.v:
           print(d)
       producer.send(args.to, d)
       n_added += 1

print("Added {} entries to long-term blacklist (processed total {} records).".format(n_added, n))
exit(0)
