# -*- coding: utf-8 -*-
import json
import argparse
import socket
import pymongo
from collections import namedtuple
from bson.binary import Binary
from kafka import KafkaConsumer, KafkaProducer
from utils.misc import *
from utils.io import next_artefact, save_script, artefact_tuple


def save_artefact(db, producer, artefact, root, to):
   path = "{}/{}".format(root, artefact.path)
   with open(path, 'rb') as fp:
        d = save_script(db, artefact, Binary(fp.read()))
        # d already contains 'sha256', 'md5' and 'size_bytes' so this update will make it compatible with the JavascriptArtefact dataclass
        d.update({ 'url': artefact.url, 
                   'inline': False, 
                   'content-type': 'text/javascript', 
                   'when': artefact.when, 
                   'origin': artefact.origin })
        producer.send(to, d)

def save_batch(db, batch_of_artefacts, producer, root, fail_on_error=False, to=None, verbose=False): # eg. root='/data/kafkaspider2'
   # finally process each record via mongo
   cnt = 0
   for artefact in batch_of_artefacts:
       try:
           if verbose:
               print(artefact)
           save_artefact(db, producer, artefact, root, to)
           cnt += 1
           if cnt % 10000 == 0:
               print("Processed {} records.".format(cnt))
       except Exception as e:
           if fail_on_error:
              raise(e)
           print(e)
           
if __name__ == "__main__":
    a = argparse.ArgumentParser(description="Process JS artefact topic records and filesystem JS into specified mongo host")
    a.add_argument("--root", help="Root of scrapy file data directory which spider has populated", type=str, required=True)
    a.add_argument("--fail", help="Fail on first error", action='store_true')
    a.add_argument("--artefacts", help="Kafka topic to read JS artefact records from eg. javascript-artefacts2", type=str, required=True)
    add_mongo_arguments(a, default_access="read-write")
    add_kafka_arguments(a, 
                        consumer=True, 
                        producer=True, 
                        default_from="javascript-artefacts-16", 
                        default_group=None,
                        default_to="visited")
    add_debug_arguments(a)
    args = a.parse_args() 

    gid = args.group
    if gid and len(gid) < 1: # empty string on command is translated to no group
        gid = None 
    if args.v:
        print("Using kafka consumer group ID: {}".format(gid))
        print("Using kakfa bootstrap servers: {}".format(args.bootstrap))
        print("Saving artefacts to kafka topic: {}".format(args.to))
        print("Reading artefacts from: topic={} root={}".format(args.artefacts, args.root))
        print("Accessing mongo DB at {}:{}".format(args.db, args.port))
        if args.fail:
            print("Terminating on first error.")

    mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
    db = mongo[args.dbname]
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
    consumer = KafkaConsumer(args.consume_from, value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                             consumer_timeout_ms=10000, bootstrap_servers=args.bootstrap, group_id=gid, auto_offset_reset=args.start) 

    save_pidfile('pid.upload.artefacts')
    my_hostname = socket.gethostname()
    print("Loading records matching {} from kafka topic".format(my_hostname))
    type = artefact_tuple()
    batch = sorted([type(**r) for r in next_artefact(consumer, args.n, lambda v: v['host'] == my_hostname and not v['origin'] is None)])
    print("Loaded {} records.".format(len(batch)))
    save_batch(db, batch, producer, args.root, fail_on_error=args.fail, to=args.to, verbose=args.v)
    rm_pidfile('pid.upload.artefacts')
