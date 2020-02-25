#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer 
import json
import sys
from urllib.parse import urlparse
from utils.url import as_priority
import pylru
import argparse

def main(input_topic, output_topic, bootstrap_servers='kafka1:9092', auto_offset_reset='earliest', max_priority=3, consumer_group=None):
    consumer = KafkaConsumer(input_topic, bootstrap_servers=bootstrap_servers, auto_offset_reset=auto_offset_reset, group_id=consumer_group, value_deserializer=lambda m: json.loads(m), consumer_timeout_ms=10*1000)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    sent = rejected = 0
    cache = pylru.lrucache(10 * 1024)
    for message in consumer:
        url = message.value.get('url')
        up = urlparse(url)
        d = { 'url': url }
        if url in cache or len(url) > 200 or 'anu.edu.au' in up.hostname:
            continue
        d.update(up._asdict())
        d.update({ 'priority' : as_priority(url, up) })
        if d.get('priority') <= max_priority:
            producer.send(output_topic, d, key=up.netloc.encode('utf-8'))
            sent = sent + 1
            cache[url] = 1
        else:
            rejected = rejected + 1
        if sent % 1000 == 0:
            print("Sent {} records to {} topic.".format(sent, output_topic))
    print("Added {} urls to {} (rejected {})".format(sent, output_topic, rejected))
    return 0

if __name__ == '__main__':
    args = argparse.ArgumentParser(description='Process an input topic (with url field) and put only good urls into another topic. Useful to clean a URL queue')
    args.add_argument('--in', type=str, default='4thug.gen4')
    args.add_argument('--out', type=str, default='4thug.gen5')
    args.add_argument('--threshold', type=int, default=3) # maximum priority to put into output queue
    args.add_argument('--bootstrap', type=str, default='kafka1:9092')
    args.add_argument('--group', nargs='?', type=str, default=None) # if specified, only output urls at the current group consumer offset, not earliest
    a = args.parse_args()
    sys.exit(main(getattr(a, 'in'), getattr(a, 'out'), 
                  max_priority=getattr(a, 'threshold'), bootstrap_servers=getattr(a,'bootstrap'),))
