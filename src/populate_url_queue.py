#!/usr/bin/python3

from kafka import KafkaProducer
import csv
import json
import uuid

producer = KafkaProducer(bootstrap_servers='kafka2', value_serializer=json_value_serializer())

with open('/home/acas/data/filtered_au_websites.csv', 'r') as csv:
     done = 0
     for line in csv:
        url = "https://" + line.rstrip()
        uuid_bytes = uuid.uuid1().bytes
        future = producer.send('url_queue', key=uuid_bytes, value={'url': url}) # NB: async!
        done = done + 1
        if done % 1000 == 0:
            print("Processed {} URLs.".format(done))

# ensure all pending sends are done
producer.flush()

# obtain metrics on delivery
print(json.dumps(producer.metrics(), sort_keys=True, indent=4))
