# -*- coding: utf-8 -*-
import json
import hashlib
import pymongo
import argparse
import pylru
import socket
from collections import namedtuple
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

class SaveToMongo(object):

    def __init__(self, *args, **kwargs):
        self.mongo =  pymongo.MongoClient(kwargs.get('mongo_host', kwargs.get('mongo_port')))
        self.db = self.mongo[kwargs.get('mongo_db')]
        self.visited_topic = kwargs.get('visited_topic')
        self.artefact_topic = kwargs.get('artefact_topic')
        bs = kwargs.get('bootstrap_kafka_servers')
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=bs)
        self.consumer = KafkaConsumer(self.artefact_topic, value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest', consumer_timeout_ms=10000, bootstrap_servers=bs) # no group

    def save_url(self, url, now):
         result = self.db.urls.insert_one({ 'url': url, 'last_visited': now })
         return result.inserted_id

    def save_script(self, url, script, content_type=None, expected_md5=None):
        # NB: we work hard here to avoid mongo calls which will slow down the spider

        # compute hashes to search for
        sha256 = hashlib.sha256(script).hexdigest()
        md5 = hashlib.md5(script).hexdigest()
        if expected_md5 and md5 != expected_md5:
            raise ValueError("Expected MD5 and MD5 hash do not match: {} {} != {}".format(url, md5, expected_md5))

        # check to see if in mongo already
        now = datetime.utcnow()
        url_id = self.save_url(url, now)

        script_len = len(script)
        s = self.db.scripts.find_one_and_update({ 'sha256': sha256, 'md5': md5, 'size_bytes': script_len }, 
                  { '$set': { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len, 'code': script }}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
        self.db.script_url.insert_one( { 'url_id': url_id, 'script': s.get(u'_id') })

        # finally update the kafka visited queue
        self.producer.send(self.visited_topic, { 'url': url, 'size_bytes': script_len, 'inline': False,
                                           'content-type': content_type, 'when': str(now), 
					   'sha256': sha256, 'md5': md5 })
    def process_tuple(self, tuple, root):
        path = "{}/{}".format(root, tuple.path)
        with open(path, 'rb') as fp:
            script = fp.read()
            self.save_script(tuple.url, script, content_type="text/javascript", expected_md5=tuple.checksum)

    def run(self, root, my_hostname=None, fail_on_error=False): # eg. root='/data/kafkaspider2'
        cnt = 0
        record = namedtuple('JavascriptArtefact', 'url origin host checksum path when')
        l = []  # list of records
        print("Loading records from kafka topic: {}".format(self.artefact_topic))
        for msg in self.consumer:
            d = msg.value
            if d['host'] == my_hostname and not d['origin'] is None:
                  r = record(**d)
                  l.append(r) 
        print("Loaded {} records.".format(len(l)))
        # sort by checksum and then by sha1 hash to speed mongo access
        record.__lt__ = lambda self, other: self.checksum < other.checksum
        l = sorted(l)
        print("Sorted {} records.".format(len(l)))
        # finally process each record via mongo
        cnt = 0
        for tuple in l:
            try:
                self.process_tuple(tuple, root)
                cnt += 1
                if (cnt % 10000 == 0):
                    print("Processed {} records.".format(cnt))
            except Exception as e:
                if fail_on_error:
                     raise(e)
                print(e)
            
if __name__ == "__main__":
    a = argparse.ArgumentParser(description="Process JS artefact topic records and filesystem JS into specified mongo host")
    a.add_argument("--mongo-host", help="Hostname/IP with mongo instance", type=str, default="pi1")
    a.add_argument("--mongo-port", help="TCP/IP port for mongo instance", type=int, default=27017)
    a.add_argument("--db", help="Mongo database to populate with JS data from kafkaspider", type=str, default="au_js")
    a.add_argument("--fail", help="Fail on first error", action='store_true')
    a.add_argument("--visited", help="Kafka topic to get visited JS summary", type=str, default="visited")
    a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1")
    a.add_argument("--root", help="Root of scrapy file data directory which spider has populated", type=str, required=True)
    a.add_argument("--artefacts", help="Kafka topic to read JS artefact records from eg. javascript-artefacts2", type=str, required=True)
    args = a.parse_args() 
    print("Added JS artefact summary to {} topic.".format(args.visited))
    s = SaveToMongo(mongo_host=args.mongo_host, mongo_port=args.mongo_port, mongo_db=args.db, 
                    visited_topic=args.visited, artefact_topic=args.artefacts, bootstrap_kafka_servers=args.bootstrap)
    s.run(args.root, my_hostname=socket.gethostname(), fail_on_error=args.fail)
