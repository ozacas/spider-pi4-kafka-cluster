from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import os
import uuid
from urllib.parse import urlparse
from datetime import datetime

# consume from url_queue topic
consumer = KafkaConsumer('url_queue', value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
                          consumer_timeout_ms=10*1000, bootstrap_servers='kafka1',
                          group_id="site_populator", client_id="site_populator_{}_{}".format(os.uname()[1], os.getpid()),
			  auto_offset_reset='earliest') # want to consume messages posted before this program started
producer = KafkaProducer(bootstrap_servers='kafka1', value_serializer=lambda m: json.dumps(m).encode('utf-8'))

done = 0
for message in consumer:
   # attempt to contact host
   u = message.value.get('url').rstrip()
   when = datetime.now()
   s = 0
   try:
       req = requests.get(u, timeout=30)
       s = req.status_code
   except Exception as e:
       producer.send('request_failed_queue', { 'url': u, 'msg': str(e) })
       continue

   done += 1
   if done % 1000 == 0:
       print("Processed {} URLs.".format(done))

   if s >= 200 and s < 400:
           u2 = u
           urls_traversed = [r.url for r in req.history if r.status_code >= 300 and r.status_code < 400]
           if len(urls_traversed) > 0:
               u2 = urls_traversed[-1]

           parts = urlparse(u2)
           # only successful contacts are placed into the canonical_url_queue, with the 
           # recommended url to use based on whether http redirects are issued 
           uuid_bytes = uuid.uuid1().bytes
           producer.send('canonical_sites', key=uuid_bytes,
                 value={ 'original_url': u, 
                   'canonical_url': u2, 
                   'canonical_domain': parts.hostname,
                   'when': when.strftime("%d-%b-%Y %H:%M:%S%z"), 
                   'http_status': s })
           # ensure thug processes this url ie. the url is scanned with a view to sub-pages being scanned
           producer.send('4thug', { 'url': u2 })
   else:
           producer.send('failed_url_queue', { 'url': u, 'http_status': s })

producer.flush()
print(json.dumps(producer.metrics(), sort_keys=True, indent=4))
