#!/usr/bin/python3
from kafka import KafkaConsumer
import argparse
import json
from urllib.parse import urlparse

def main():
    consumer = KafkaConsumer('javascript-download-failure', value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest', consumer_timeout_ms=10000, bootstrap_servers='kafka1')
    bad = []
    bad_hosts = set()
    for message in consumer:
         if 'file_urls' in message.value:
             url = message.value.get('file_urls')[0]
             up = urlparse(url)
             host = up.hostname
             if host is None:
                 host = ''
             host = host.lower()
             if any([host == "antidote.me"]):
                 print(url,", ",message.value.get('origin'))
                 bad_hosts.add(host)
                 bad.append(url)
                 pass
    print(bad_hosts)

if __name__ == "__main__":
    main()
