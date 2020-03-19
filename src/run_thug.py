#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from subprocess import run
from utils.thug_parse import ThugLogParser
from utils.models import JavascriptLocation
from utils.geo import AustraliaGeoLocator
from urllib.parse import urlparse
from datetime import datetime
from dataclasses import asdict
import os
import tempfile
import json
import random
import pymongo
import pylru
import argparse

ua = [ "winxpie60", "winxpie61", "winxpie70", "winxpie80", "winxpchrome20",
        "winxpfirefox12", "winxpsafari5", "win2kie60", "win2kie80",
        "win7ie80", "win7ie90", "win7ie100", "win7chrome20",
        "win7chrome40", "win7chrome45", "win7chrome49", "win7firefox3",
        "win7safari5", "win10ie110", "osx10chrome19", "osx10safari5",
        "linuxchrome26", "linuxchrome30", "linuxchrome44", "linuxchrome54",
        "linuxfirefox19", "linuxfirefox40", "galaxy2chrome18", "galaxy2chrome25",
        "galaxy2chrome29", "nexuschrome18", "ipadchrome33", "ipadchrome37",
        "ipadchrome38", "ipadchrome39", "ipadchrome45", "ipadchrome46",
        "ipadchrome47", "ipadsafari7", "ipadsafari8", "ipadsafari9" ]

a = argparse.ArgumentParser(description="Run thug on origin pages which contain JS not in AU")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--topic", help="Read scripts from specified topic [javascript-geolocation]", type=str, default="javascript-geolocation") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off (empty is no group) [run-thug]", type=str, default='run-thug')
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Start from earliest or latest available message [latest]", type=str, choices=['earliest', 'latest'], default='latest')
a.add_argument("--max", help="Maximum number of objects thug can fetch [100]", type=int, default=100)
a.add_argument("--agent", help="Use a fixed instead of randomly chosen user-agent per message [None]", type=str, default=None, choices=ua)
a.add_argument("--host", help="Mongo IP/hostname to store thug results in [pi1]", type=str, default='pi1')
a.add_argument("--port", help="Mongo DB TCP port to use [27017]", type=int, default=27017)
a.add_argument("--db", help="Database to store run_thug.py results (thug results stored separately via /etc/thug/thug.conf) [au_js]", type=str, default="au_js")
a.add_argument("--geo", help="Maxmind DB to use [/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb]", type=str, default="/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb")
a.add_argument("--thug", help="Path to thug executable [/usr/bin/thug]", type=str, default="/usr/bin/thug")
args = a.parse_args()

group = args.group
if len(group) < 1:
    group = None
consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, group_id=group, auto_offset_reset=args.start,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
host = os.uname()[1]
au_locator = AustraliaGeoLocator(db_location=args.geo)
mongo = pymongo.MongoClient(args.host, args.port)
cache = pylru.lrucache(10 * 1000)

# check that executable for thug exists...
if not os.path.exists(args.thug):
   raise Exception("No such executable {}".format(args.thug))

for message in consumer:
        jsloc = JavascriptLocation(**message.value)

        # dont redo analyses of stuff we've just seen
        if jsloc.origin in cache: 
           continue
        cache[jsloc.origin] = 1

        # thug will produce 1) mongodb output 2) log file
        now = str(datetime.utcnow())

        # We process the log here... and push worthy stuff into the relevant queues
        if args.v:
            print(jsloc) 
        with tempfile.NamedTemporaryFile() as fp:
           user_agent = random.choice(ua) if args.agent is None else args.agent     
           try:
               # after sampling over 4000 urls (with random user agents) if a thug run takes longer than 600 seconds, it is likely to take an hour or more.
               # we cant afford that runtime with available resources, so we limit to 1200 seconds and marginally increase the failure rate. In this way, we
               # get through more pages with available resources
               proc = run([args.thug,
                       "--json-logging",      # elasticsearch logging????
                       "--delay=5000",        # be polite
                       "--useragent={}".format(user_agent), # choose random user agent from supported list to maximise coverage
                       "--features-logging",  # ensure JS snippets are recorded in mongo
                       "--no-javaplugin",     # disable functionality we dont need
                       "--no-shockwave", 
                       "--no-adobepdf",
                       "--no-silverlight",
                       "--no-honeyagent",
                       "-t{}".format(args.max),           # max 100 requests per url from kafka
                       "--verbose",           # log level == INFO so we can get sub-resources to fetch
                       jsloc.origin
                ], stderr=fp, timeout=20 * 60)

               status = proc.returncode
               if status == 0 or status == 1: # thug succeed?
                   # will send messages based on log
                   ret = ThugLogParser(db=mongo[args.db], au_locator=au_locator, origin=jsloc.origin, user_agent=user_agent).parse(fp.name) 
                   d = asdict(ret)
                   # NB: since the log can be very long (with deobfuscated JS) we remove it to keep messages short in kafka topic
                   d.pop('log', None)
                   if args.v:
                       print(d)
                   producer.send('thug-completed-analyses', d)
               else:
                   producer.send('thug_failure', { 'url_scanned': jsloc.origin, 'exit_status': status, 
                                                   'when': now, 'user_agent_used': user_agent } )
           except Exception as e:
               if args.v:
                   print(str(e))
               producer.send('thug_failure', { 'url_scanned': jsloc.origin, 'exit_status': -1, # -1 == exception eg. timeout
                                                   'when': now, 'user_agent_used': user_agent, 'msg': str(e) })
