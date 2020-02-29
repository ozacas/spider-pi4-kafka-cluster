# -*- coding: utf-8 -*-
import json
import hashlib
import pymongo
import argparse
import pylru
import socket
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

class SaveToMongo(object):

    def __init__(self, *args, **kwargs):
        self.mongo =  pymongo.MongoClient(kwargs.get('mongo_host', kwargs.get('mongo_port')))
        self.db = self.mongo[kwargs.get('mongo_db')]
        self.visited_topic = kwargs.get('visited', 'visited')
        self.artefact_topic = kwargs.get('javascript-artefacts', 'javascript-artefacts')
        bs = kwargs.get('bootstrap_kafka_servers', 'kafka1')
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=bs)
        self.consumer = KafkaConsumer(self.artefact_topic, value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest', consumer_timeout_ms=10000, bootstrap_servers=bs) # no group

    def save_url(self, url, now):
         result = self.db.urls.insert_one({ 'url': url, 'last_visited': now })
         return result.get(u'_id')

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
 
    def run(self, root, my_hostname=None): # eg. root='/data/kafkaspider2'
        cnt = 0
        for msg in self.consumer:
            d = msg.value
            if d['host'] == my_hostname:
                path = "{}/{}".format(root, d['path'])
                try:
                    with open(path, 'rb') as fp:
                        script = fp.read()
                        self.save_script(d['url'], script, content_type="text/javascript", expected_md5=d['checksum'])
                    cnt += 1
                    if cnt % 1000 == 0:
                        print("Processed {} messages from {}".format(cnt, self.artefact_topic))
                except Exception as e:
                    print(e)
              
if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--mongo-host", help="Hostname/IP with mongo instance", type=str, default="pi1")
    a.add_argument("--mongo-port", help="TCP/IP port for mongo instance", type=int, default=27017)
    a.add_argument("--db", help="Mongo database to populate with JS data from kafkaspider", type=str, default="au_js")
    a.add_argument("--topic", help="Kafka topic to get visited JS summary", type=str, default="visited")
    a.add_argument("--bootstrap", help="Kafka bootstrap servers", type=str, default="kafka1,kafka2")
    a.add_argument("--root", help="Root of scrapy file data directory which spider has populated", type=str, required=True)
    args = a.parse_args() 
    s = SaveToMongo(mongo_host=args.mongo_host, mongo_port=args.mongo_port, mongo_db=args.db, visited_topic=args.topic, bootstrap_kafka_servers=args.bootstrap)
    s.run(args.root, my_hostname=socket.gethostname())
