#!/usr/bin/python3
import dns.resolver
import json
import argparse
import pylru
import ipaddress
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from utils.AustraliaGeoLocator import AustraliaGeoLocator
from utils.models import PageStats, JavascriptLocation
from urllib.parse import urlparse
from dataclasses import asdict

a = argparse.ArgumentParser(description="Process JS artefact topic records and filesystem JS into specified mongo host")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=1000000000)
a.add_argument("--topic", help="Read scripts from specified topic [html-page-stats]", type=str, default="html-page-stats") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [None]", type=str, default=None)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [latest]", type=str, default='latest')
args = a.parse_args()

consumer = KafkaConsumer('html-page-stats', group_id=args.group, auto_offset_reset=args.start, 
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), bootstrap_servers=args.bootstrap)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
cache = pylru.lrucache(1000)
resolver = dns.resolver.Resolver()
resolver.nameservers = ['8.8.8.8', '8.8.4.4'] # use public resolvers only
geo = AustraliaGeoLocator()

for message in consumer:
   ps = PageStats(**message.value)
   for script in ps.scripts.split():
       up = urlparse(script)
       host = up.hostname
       if host and len(host) and not host in cache:
           try:
               result_ips = [a for a in resolver.query(host, 'a')]
           except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
               result_ips = []
           result_countries = set([geo.country_code(ipaddress.ip_address(r)) for r in result_ips])
           reportable = False
           for country in result_countries:
               if country not in ['AU', '', 'US', None]:
                   reportable = True
                   first_country = country
           if reportable:
               js = JavascriptLocation(country=first_country, script=script, origin=ps.url, ip=str(result_ips[0]), when=str(datetime.utcnow()))
               print(js)
               producer.send('javascript-geolocation', asdict(js))
               cache[host] = js
