#!/usr/bin/python3

from kafka import KafkaProducer
import csv
import json

producer = KafkaProducer(bootstrap_servers='kafka1', 
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

with open('/home/acas/data/filtered_au_websites.csv', 'r') as csv:
     for line in csv:
        url = "https://" + line.rstrip()
        future = producer.send('url_queue', {'url': url}) # NB: async!

# ensure all pending sends are done
producer.flush()

# obtain metrics on delivery
print(json.dumps(producer.metrics(), sort_keys=True, indent=4)
