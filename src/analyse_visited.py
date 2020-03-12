#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from dataclasses import dataclass
import json
from datetime import datetime
import os
import argparse
from utils.models import JavascriptArtefact
import subprocess
import pymongo
import logging
from tempfile import NamedTemporaryFile

a = argparse.ArgumentParser(description="Extract features from each javascript in visited topic and dump into analysis-results topic")
a.add_argument("--mongo-host", help="Hostname/IP with mongo instance [pi1]", type=str, default="pi1")
a.add_argument("--mongo-port", help="TCP/IP port for mongo instance [27017]", type=int, default=27017)
a.add_argument("--db", help="Mongo database to populate with JS data [au_js]", type=str, default="au_js")
a.add_argument("--visited", help="Kafka topic to get visited JS summary [visited]", type=str, default="visited")
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=int, default=1000000000)
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [javascript-analysis]", type=str, default='javascript-analysis')
a.add_argument("--v", help="Debug verbosely", action="store_true")
args = a.parse_args()

consumer = KafkaConsumer(args.visited, bootstrap_servers=args.bootstrap, group_id=args.group, #auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
mongo = pymongo.MongoClient(args.mongo_host, args.mongo_port)
db = mongo[args.db]
logger = logging.getLogger(__name__)

def get_script(artefact):
   # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
   d = { 'sha256': artefact.sha256.strip(), 'md5': artefact.md5.strip(), 'size_bytes': artefact.size_bytes } 
   if artefact.inline:
       js = db.snippets.find_one(d)
       if js:
           return js.get(u'code')
   else:
       js = db.scripts.find_one(d)
       if js:
           return js.get(u'code')
   # oops... something failed so we log it and keep going with the next message
   logger.warning("Failed to find JS in database for {}".format(artefact))
   return None 

def analyse_script(js, url):
   # save code to a file
   tmpfile = NamedTemporaryFile(delete=False) 
   tmpfile.write(js)   
   tmpfile.close()

   # save to file and run extract-features.jar to identify the javascript features
   process = subprocess.run(["/usr/bin/java", "-jar", "/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar", tmpfile.name, url], capture_output=True)

   # turn process stdout into something we can save
   ret = None
   if process.returncode == 0:
       ret = json.loads(process.stdout)
   else:
       logger.warning("Failed to extract features for {}".format(url))
       producer.send("feature-extraction-failures", { 'url': url , 'when': str(datetime.utcnow()) })
   # cleanup
   os.unlink(tmpfile.name)
   return ret

cnt = 0    
for message in consumer:
    d = message.value
    d['content_type'] = d['content-type']
    del d['content-type']
    jsr = JavascriptArtefact(**d)

    # eg.  {'url': 'https://alga.asn.au/', 'size_bytes': 294, 'inline': True, 'content-type': 'text/html; charset=UTF-8', 'when': '2020-02-06 02:51:46.016314', 'sha256': 'c38bd5db9472fa920517c48dc9ca7c556204af4dee76951c79fec645f5a9283a', 'md5': '4714b9a46307758a7272ecc666bc88a7'}
    if 'javascript' in jsr.content_type:
        # verbose?
        if args.v:
            print(jsr)

        # obtain the JS from MongoDB
        js = get_script(jsr)
        if js:
             results = analyse_script(js, jsr.url)
             if results:
                 producer.send('analysis-results', results)
    cnt += 1
    if cnt > args.n:
        break
consumer.commit()
exit(0)
