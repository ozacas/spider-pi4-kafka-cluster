#!/usr/bin/python3
from kafka import KafkaConsumer
import re
import json
from utils.models import PageStats
from urllib.parse import urlparse, urljoin

consumer = KafkaConsumer('html-page-stats', group_id='topic-grep', value_deserializer=lambda m : json.loads(m.decode('utf-8')), bootstrap_servers='kafka1')

for message in consumer:
    ps = PageStats(**message.value)
    origin_host = urlparse(ps.url)
    for script in ps.scripts.split():
        j = urljoin(ps.url, script)
        up = urlparse(j)
        host = up.hostname
        if host is None:
            print("Bad script URL {}".format(script, ps.url))
            pass
        elif host.endswith('.com') or host.endswith('.au') or host.endswith('.net') or host.endswith('.org'):
            pass
        elif host != origin_host.hostname:
            print(script, ps.url)

