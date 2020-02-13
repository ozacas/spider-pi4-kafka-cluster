#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
import os
import subprocess
import pymongo
import logging
from tempfile import NamedTemporaryFile
consumer = KafkaConsumer('visited', bootstrap_servers='kafka1', group_id='javascript-analysis', auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), max_poll_interval_ms=30000000) # crank max poll to ensure no kafkapython timeout
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
mongo = pymongo.MongoClient('pi1', 27017)
db = mongo.au_js
logger = logging.getLogger(__name__)

def get_script(url, sha256, md5, inline_script):
   # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
   d = { 'sha256': sha256.strip(), 'md5': md5.strip() } 
   if inline_script:
       js = db.snippets.find_one(d)
       if js:
           return js.get(u'code')
   else:
       js = db.scripts.find_one(d)
       if js:
           return js.get(u'code')
   # oops... something failed so we log it and keep going with the next message
   logger.warning("Failed to find JS in database for {} (inline = {})".format(url, inline_script))
   return None 

def analyse_script(js, url):
   # save code to a file
   tmpfile = NamedTemporaryFile(delete=False) 
   tmpfile.write(js)   

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
    
for message in consumer:
    d = message.value 
    # eg.  {'url': 'https://alga.asn.au/', 'size_bytes': 294, 'inline': True, 'content-type': 'text/html; charset=UTF-8', 'when': '2020-02-06 02:51:46.016314', 'sha256': 'c38bd5db9472fa920517c48dc9ca7c556204af4dee76951c79fec645f5a9283a', 'md5': '4714b9a46307758a7272ecc666bc88a7'}
    if 'javascript' in d.get('content-type'):
        # obtain the JS from MongoDB
        js = get_script(d.get('url'), d.get('sha256'), d.get('md5'), d.get('inline'))
        if js:
             results = analyse_script(js, d.get('url'))
             if results:
                 producer.send('analysis-results', results)

exit(0)
