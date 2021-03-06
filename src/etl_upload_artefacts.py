# -*- coding: utf-8 -*-
import json
import argparse
import socket
import pymongo
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from utils.misc import *
from utils.models import DownloadArtefact
from utils.io import next_artefact, save_artefact, batch

def save_batch(db, batch_of_artefacts, producer, root, fail_on_error=False, to=None, verbose=False, defensive=False): # eg. root='/data/kafkaspider2'
   # finally process each record via mongo
   cnt = n_cached = 0
   for artefact in batch_of_artefacts:
       try:
           ret, was_cached = save_artefact(db, artefact, root, defensive=defensive)
           # NB: ensure all records with a given hash are in same partition for the downstream consumer to optimise cache access, although the 
           # risk is that this is uneven allocation (but hopefully there are enough hashes to be ok)
           producer.send(to, ret, key=ret.get('sha256').encode('utf-8')) 
           cnt += 1
           if was_cached: # script already present in database?
              n_cached += 1
           if verbose:
               print(artefact)
           if cnt % 10000 == 0:
               print("Processed {} records.".format(cnt))
       except Exception as e:
           if fail_on_error:
              raise(e)
           print(e)
   print("Uploaded {} artefacts, {} scripts already present in database\n".format(cnt, n_cached))
           
if __name__ == "__main__":
    a = argparse.ArgumentParser(description="Process JS artefact topic records and filesystem JS into specified mongo host")
    a.add_argument("--root", help="Root of scrapy file data directory which spider has populated", type=str, required=True)
    a.add_argument("--fail", help="Fail on first error", action='store_true')
    a.add_argument("--artefacts", help="Kafka topic to read JS artefact records from eg. javascript-artefacts2", type=str, required=True)
    a.add_argument("--defensive", help="Validate objects to/from the database for correctness (SLOW) [False]", action="store_true")
    add_mongo_arguments(a, default_access="read-write", default_user="rw")
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
    producer = KafkaProducer(value_serializer=json_value_serializer(), bootstrap_servers=args.bootstrap)
    consumer = KafkaConsumer(args.consume_from, value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                             consumer_timeout_ms=10000, bootstrap_servers=args.bootstrap, group_id=gid, auto_offset_reset=args.start) 

    my_hostname = socket.gethostname()
    print("Loading records matching {} from kafka topic".format(my_hostname))
    total = 0
    for b in batch(next_artefact(consumer, args.n, lambda v: v['host'] == my_hostname and not v['origin'] is None, verbose=args.v), n=10000):
        artefacts = sorted([DownloadArtefact(**r) for r in b])
        print("Loaded {} artefacts.".format(len(artefacts)))
        save_batch(db, artefacts, producer, args.root, fail_on_error=args.fail, to=args.to, verbose=args.v, defensive=args.defensive)
        print("Uploaded {} artefacts in batch. {}".format(len(artefacts), str(datetime.utcnow())))
        total += len(artefacts)

    print("Uploaded {} artefacts in total.".format(total))
    mongo.close()
    consumer.close()
    exit(0)
