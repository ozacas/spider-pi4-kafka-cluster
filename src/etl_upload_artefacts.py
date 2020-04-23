# -*- coding: utf-8 -*-
import os
import json
import hashlib
import argparse
import pylru
import socket
import pymongo
from bson.binary import Binary
from kafka import KafkaConsumer, KafkaProducer
from utils.misc import *
from utils.io import next_artefact

class SaveToMongo:

    def __init__(self, *args, **kwargs):
        self.db = kwargs.get('db')
        self.producer = kwargs.get('producer')
        self.consumer = kwargs.get('consumer')
        self.n = kwargs.get('n')
        self.debug = kwargs.get('debug')

    def save_url(self, artefact):
         result = self.db.urls.insert_one({ 'url': artefact.url, 'last_visited': artefact.when, 'origin': artefact.origin })
         return result.inserted_id

    def save_script(self, artefact, script, content_type=None):
        # NB: we work hard here to avoid mongo calls which will slow down the spider

        # compute hashes to search for
        db = self.db
        sha256 = hashlib.sha256(script).hexdigest()
        md5 = hashlib.md5(script).hexdigest()
        if md5 != artefact.checksum:
            raise ValueError("Expected MD5 and MD5 hash do not match: {} {} != {}".format(url, md5, artefact.checksum))

        # check to see if in mongo already
        url_id = self.save_url(artefact)

        script_len = len(script)
        key = { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len }  
        value = key.copy() # NB: shallow copy is sufficient for this application
        value.update({ 'code': script })

        s = db.scripts.find_one_and_update(key, { '$set': value }, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
        db.script_url.insert_one( { 'url_id': url_id, 'script': s.get(u'_id') })

        return key

    def save_artefact(self, artefact, root, to):
        path = "{}/{}".format(root, artefact.path)
        with open(path, 'rb') as fp:
             d = self.save_script(artefact, Binary(fp.read()))
             d.update({ 'url': artefact.url, 'inline': False, 'content-type': 'text/javascript', 'when': artefact.when, 'origin': artefact.origin })
             self.producer.send(to, d)

    def run(self, root, my_hostname=None, fail_on_error=False, to=None): # eg. root='/data/kafkaspider2'
        print("Loading records matching {} from kafka topic".format(my_hostname))
        l = [JavascriptArtefact(**r) for r in self.next_artefact(self.consumer, self.n, 
                                           filter_cb=lambda m: m.value['host'] == my_hostname and not m.value['origin'] is None)]
        print("Loaded {} records.".format(len(l)))

        # sort by checksum and then by sha1 hash to speed mongo queries (maybe)
        l = sorted(l)
        print("Sorted {} records.".format(len(l)))
        # finally process each record via mongo
        cnt = 0
        verbose = self.debug
        for artefact in l:
            try:
                if verbose:
                    print(artefact)
                self.save_artefact(artefact, root, to)
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
        print("Accessing mongo DB at {}:{}".format(args.host, args.port))
        if args.fail:
            print("Terminating on first error.")

    mongo = pymongo.MongoClient(args.db, args.port, username=args.dbuser, password=str(args.dbpassword))
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=args.bootstrap)
    consumer = KafkaConsumer(args.consume_from, value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
               consumer_timeout_ms=10000, bootstrap_servers=args.bootstrap, group_id=gid, auto_offset_reset=args.start) 

    save_pidfile('pid.upload.artefacts')
    s = SaveToMongo(db=mongo[args.db], n=args.n, gid=gid, debug=args.v, consumer=consumer, producer=producer)  
    s.run(args.root, my_hostname=socket.gethostname(), fail_on_error=args.fail, to=args.to)
    rm_pidfile('pid.upload.artefacts')
