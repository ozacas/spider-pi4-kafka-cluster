#!/usr/bin/python3

from kafka import KafkaClient

client = KafkaClient(bootstrap_servers='kafka1')
client.poll(timeout_ms=1000)
topics = list(client.cluster.topics(exclude_internal_topics=True))
print(topics)
