#!/usr/bin/python3
import os
import sys
import json
import argparse
import pymongo
from typing import Iterable
from kafka import KafkaConsumer, KafkaProducer
from utils.models import Password, JavascriptArtefact
from utils.misc import setup_signals, rm_pidfile, save_pidfile
from utils.features import find_script, analyse_script, calculate_vector

a = argparse.ArgumentParser(description="Examine perfect hits (based on AST and/or Function dist) looking for literals of interest")
a.add_argument("--topic", help="Kafka topic to read good hits from ETL publishing [javascript-artefact-control-results]", type=str, default='javascript-artefact-control-results')
a.add_argument("--group", help="Use specified kafka consumer group to remember where we left off [algo-changed-literals]", type=str, default="algo-changed-literals")
a.add_argument("--start", help="Consume from earliest|latest message available in control results topic [latest]", type=str, default='latest')
a.add_argument("--bootstrap", help="Kafka bootstrap servers [kafka1]", type=str, default="kafka1")
a.add_argument("--n", help="Read no more than N records from kafka [infinite]", type=float, default=float('Inf'))
a.add_argument("--to", help="Report suspicious artefacts to specified topic [javascript-artefacts-suspicious]", type=str, default="javascript-artefacts-suspicious")
a.add_argument("--db", help="Mongo host/ip to save to [pi1]", type=str, default="pi1")
a.add_argument("--port", help="TCP port to access mongo db [27017]", type=int, default=27017)
a.add_argument("--dbname", help="Name on mongo DB to access [au_js]", type=str, default="au_js")
a.add_argument("--user", help="MongoDB RBAC username to use (read-only access required)", type=str, required=True)
a.add_argument("--password", help="MongoDB password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--java", help="Path to JVM executable [/usr/bin/java]", type=str, default="/usr/bin/java")
a.add_argument("--extractor", help="Path to feature extractor JAR", type=str, default="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar")
args = a.parse_args()

mongo = pymongo.MongoClient(args.db, args.port, 
                            username=args.user, password=str(args.password))
db = mongo[args.dbname]

consumer = KafkaConsumer(args.topic, bootstrap_servers=args.bootstrap, group_id=args.group, 
                         auto_offset_reset=args.start, consumer_timeout_ms=10000,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

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

def next_artefact(consumer, max, verbose):
    n = 0
    for message in consumer:
        v = message.value
        if v['ast_dist'] < 0.01 or v['function_dist'] < 0.01: # only yield good hits - this program is otherwise not interested
            yield v
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records.".format(n))
        if n >= max:
            break

setup_signals(cleanup)
save_pidfile('pid.identify.perfect.changed.literals')

def looks_suspicious(set: Iterable[str]):
   partial_words = ['json', 'submit', 'eval', 'https://', 'http://', 'encode', 'decode', 'URI', 'network', 'dns']
   key_words = ['get', 'post', 'xmlHttpRequest', 'ajax', 'charCodeAt', 'createElement', 'insertBefore', 'appendChild']
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
for good_hit in next_artefact(consumer, args.n, args.v):
   # ok so we have a hit with near perfect AST syntax to a control AND function calls which are also a perfect match to controls
   # but what about literal strings in the code? 
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
       if args.v:
           print(jsr)
       if not failed:
           ks = set(lv_control.keys())
           lv_origin = ret.get('literals_by_count', {})
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
