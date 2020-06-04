#!/usr/bin/python3
import os
import sys
import json
import argparse
import pymongo
from typing import Iterable
from kafka import KafkaConsumer, KafkaProducer
from utils.models import JavascriptArtefact
from utils.misc import *
from utils.io import next_artefact
from utils.features import find_script, analyse_script, calculate_vector

a = argparse.ArgumentParser(description="Examine perfect hits (based on AST and/or Function dist) looking for literals of interest")
add_kafka_arguments(a, 
                    consumer=True, # ensure we can read from a topic
                    producer=True, # and save to a topic
                    default_from='javascript-artefact-control-results', 
                    default_group='algo-changed-literals', 
                    default_to='javascript-artefacts-suspicious')
add_mongo_arguments(a, default_access="read-write", default_user='rw')
add_extractor_arguments(a)
add_debug_arguments(a)
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.dbuser, password=str(args.dbpassword))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.consume_from, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=10000,
                         value_deserializer=json_value_deserializer())
producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=json_value_serializer())

def cleanup(*args):
    global consumer
    global mongo
    if len(args):
        print("Now cleaning up and terminating... please wait.")
    else:
        print("Finished analysis. Shutting down...")
    consumer.close()
    mongo.close()
    rm_pidfile('pid.identify.perfect.changed.literals')
    sys.exit(0)

setup_signals(cleanup)
save_pidfile('pid.identify.perfect.changed.literals')

def looks_suspicious(set: Iterable[str]):
   partial_words = ['json', 'submit', 'eval', 'https://', 'http://', 'encode', 'decode', 'URI', 'network', 'dns']
   key_words = ['get', 'post', 'fromCharCode', 'xmlHttpRequest', 'ajax', 'charCodeAt', 'createElement', 'insertBefore', 'appendChild']
   matched = 0
   for s in set:
      if any(x in s.lower() for x in partial_words):
          matched += 1
      if len(s) > 512:
          matched += 1
      if any(x == s.lower() for x in key_words):
          matched += 1
   return matched >= 2

n_suspicious = n_failed = 0
for good_hit in next_artefact(consumer, args.n, lambda v: v['ast_dist'] < 0.01 or v['function_dist'] < 0.01, verbose=args.v):
   # ok so we have a hit with near perfect AST syntax to a control OR function calls which are also a perfect match to controls
   # but what about literal strings in the code? 
   print(good_hit)
   lv_control = db.javascript_controls.find_one({ 'origin': good_hit['control_url'] }).get('literals_by_count')
   script, _ = find_script(db, good_hit['origin_url'])
   if script and lv_control:
       content = script.get('code')
       if not content:
           n_failed += 1
           print("WARNING: no code for {}".format(good_hit['origin_url']))
           continue
       jsr = JavascriptArtefact(url=good_hit['origin_url'], sha256=script.get('sha256'), md5=script.get('md5'), size_bytes=script.get('size_bytes'))
       ret, failed, stderr = analyse_script(content, jsr, java=args.java, feature_extractor=args.extractor)
       d = json.loads(ret.decode('utf-8'))
       if not failed:
           ks = set(lv_control.keys())
           lv_origin = d.get('literals_by_count', {})
           origin_literals_not_in_control = set(lv_origin.keys()).difference(lv_control.keys())
           v1, lv_control_sum = calculate_vector(lv_control, feature_names=ks) 
           if len(origin_literals_not_in_control) > 0 and looks_suspicious(origin_literals_not_in_control):
               print("*" * 40)
               print(good_hit)
               print([k[0:200] for k in origin_literals_not_in_control]) # dont print more than first 200 chars per changed literal
               good_hit.update({ 'unexpected_literals': list(origin_iterals_not_in_control) })
               producer.send('javascript-artefacts-suspicious', good_hit)
               n_suspicious += 1
       else:
           n_failed += 1
   else:
       n_failed += 1

print("Saw {} suspicious artefacts, {} failed to be analysed.".format(n_suspicious, n_failed))
cleanup()
