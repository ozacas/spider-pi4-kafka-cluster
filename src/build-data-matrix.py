#!/usr/bin/python3
import json
import csv
import sys
import signal
import pylru
import argparse
from kafka import KafkaConsumer
from utils.models import FeatureVector, JavascriptArtefact, Password
from utils.features import analyse_script
from dataclasses import fields
import pymongo

a = argparse.ArgumentParser(description="Read analysis results feature vector topic and generate TSV file at stdout")
a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [Inf]", type=int, default=1000000000)
a.add_argument("--topic", help="Read analysis results from specified topic [analysis-results]", type=str, default="analysis-results") # NB: can only be this topic
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [None]", type=str, default=None)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--start", help="Consume from earliest|latest message available in artefacts topic [earliest]", type=str, default='earliest')
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="Username to authenticate to MongoDB (read only access required)", type=str, required=True)
a.add_argument("--password", help="Password to authenticate to MongoDB", type=Password, default=Password.DEFAULT) 
args = a.parse_args()

consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, auto_offset_reset=args.start,
                          consumer_timeout_ms=1000, group_id=args.group,
                          value_deserializer=lambda m: json.loads(m.decode('utf-8'))) # dont set group id, we dont want to keep an offset for this program
mongo = pymongo.MongoClient(args.db, args.port, username=args.user, password=str(args.password))
db = mongo[args.dbname]
headers = None

def cleanup(*args):
    global mongo
    global consumer
    if len(args):
        print("Ctrl+C pressed. Terminating...")
    consumer.close()
    mongo.close()
    sys.exit(0)
 
def resolve_feature_vector(db, message):
   url = message.get('id')
   d = { 'first_url': url }
   # 1. find url _id
   ret = db.urls.find_one( { 'url': url }) # TODO FIXME: do we care which timestamp of the url we get? nah... not for now
   if not ret:
       raise Exception("Could not find URL in MongoDB: {}".format(url))
   id = ret.get('_id')

   # 2. lookup url_id in script_urls collection
   ret = db.script_url.find_one({ 'url_id': id }) 
   if not ret:
       raise Exception("Could not find script_url in MongoDB: {} {}".format(id, url))
   script_id = ret.get('script')
 
   # 3. lookup script_id in scripts collection
   ret = db.scripts.find_one({ '_id': script_id })
   if ret:
       d.update({ 'sha256': ret.get('sha256'), 'md5': ret.get('md5'), 'size_bytes': ret.get('size_bytes') })
   else:
       raise Exception("Could not find script {} {}".format(script_id, url))
   code = ret.get('code')

   # 4. finally we want to avoid re-computing the feature vector (slow) so we look it up in Mongo
   ret = db.statements_by_count.find_one({ 'url': url })
   if ret:
       d.update(**ret) 
       d.pop('_id', None)
       d.pop('url', None)
   else:
       sha256 = message.get('sha256')
       md5 = message.get('md5')
       jsr = JavascriptArtefact(url=url, sha256=sha256, md5=md5)
       ret = analyse_script(code, jsr, None)
       d.update(**ret['statements_by_count'])

   return d

def print_fv_as_csv(fv):
   global headers
   if not headers:
       headers = [f.name for f in fields(fv)]
       print('\t'.join(headers)) # dump CSV header if required

   row = [str(getattr(fv, f, '')) for f in headers]
   print('\t'.join(row))

def eviction(hash, fv):
   print_fv_as_csv(fv)

cache = pylru.lrucache(10 * 1000, eviction)
signal.signal(signal.SIGINT, cleanup)
for message in consumer:
   if 'id' in message.value:
      try:
          d = resolve_feature_vector(db, message.value) # need to get the feature vector (if any) from MongoDB since its not in the kafka message (old format)
      except Exception as e:
          continue
   else:
      d = { 'sha256': message.value.get('sha256'), 'md5': message.value.get('md5'), 
            'first_url': message.value.get('url'), 'size_bytes': message.value.get('size_bytes') }
      d.update(**message.value['statements_by_count'])

   if 'sha256' in d:
       hash = d['sha256']
       if hash in cache:
          fv = cache[hash]
          fv.n += 1
          cache[hash] = fv  # update LRU to avoid evicting this entry 
       else: 
          cache[hash] = FeatureVector(**d, n=1)

for k,fv in cache.items():
    print_fv_as_csv(fv)

cleanup()
