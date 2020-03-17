#!/usr/bin/python3
import dns.resolver
from dns.exception import DNSException
import json
import argparse
import pylru
import time
import ipaddress
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from utils.geo import AustraliaGeoLocator
from utils.models import PageStats, JavascriptLocation
from urllib.parse import urlparse
from dataclasses import asdict

a = argparse.ArgumentParser(description="Report scripts from html-page-stats which are not US/AU geolocated based on any IP associated with the URL hostname")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=1000000000)
a.add_argument("--topic", help="Read scripts from specified topic [html-page-stats]", type=str, default="html-page-stats") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [None]", type=str, default=None)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [latest]", type=str, default='latest')
a.add_argument("--geo", help="Maxmind DB to use (must include country code) [...]", type=str, default="/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb")
args = a.parse_args()

consumer = KafkaConsumer('html-page-stats', group_id=args.group, auto_offset_reset=args.start, 
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), bootstrap_servers=args.bootstrap)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
cache = pylru.lrucache(1000)
resolver = dns.resolver.Resolver()
resolver.nameservers = ['8.8.8.8', '8.8.4.4'] # use public resolvers only
geo = AustraliaGeoLocator(db_location=args.geo)

done = 0
for message in consumer:
   ps = PageStats(**message.value)
   for script in ps.scripts.split():
       up = urlparse(script)
       host = up.hostname
       if host and len(host) and not host in cache:
           try:
               result_ips = [a for a in resolver.query(host, 'a')]
           except DNSException as e:
               print(str(e)+" "+host)
               time.sleep(10)  # be nice if dns server overloaded
               result_ips = []
           result_countries = set([geo.country_code(ipaddress.ip_address(r)) for r in result_ips])
           reportable = False
           first_country = '?'
           for country in result_countries:
               if country not in ['AU', '', None]:
                   up2 = urlparse(ps.url) # origin URL
                   reportable = (up.hostname != up2.hostname) # NB: only report to geolocation queue when different to origin hostname 
                   first_country = country
           
           if len(result_ips) > 0:
               js = JavascriptLocation(country=first_country, script=script, origin=ps.url, ip=str(result_ips[0]), when=str(datetime.utcnow()))
               cache[host] = js # ensure LRU cache is populated even if the js record is not to appear in kafka/stdout
               if reportable:
                   print(js)
                   producer.send('javascript-geolocation', asdict(js))
           else:
               cache[host] = None # NB: no ip, we still cache that...
   done += 1
   if done >= args.n:
      print("Processed {} records. Terminating per request.".format(done)) 
      break

exit(0)
