from kafka import KafkaConsumer, KafkaProducer
import requests
from urllib.parse import urlparse

# consume from url_queue topic
consumer = KafkaConsumer('url_queue', value_deserializer=lambda m: json.loads(m.decode('utf-8')), iter_timeout=10)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))

for message in consumer:
   # attempt to contact host
   u = message.get('url')
   when = datetime.now()
   req = requests.get(u)
   s = req.status_code

   if s >= 200 and s < 400:
           u2 = u
           urls_traversed = [r.url for r in req.history if r.status_code >= 300 and r.status_code < 400]
           if len(urls_traversed) > 0:
               u2 = urls_traversed[-1]

           parts = urlparse(u2)
           # only successful contacts are placed into the canonical_url_queue, with the 
           # recommended url to use based on whether http redirects are issued 
           producer.send('canonical_sites', 
                 { 'original_url': u, 
                   'canonical_url': u2, 
                   'canonical_domain': parts.hostname,
                   'when': when.strftime("%d-%b-%Y %H:%M:%S%z"), 
                   'http_status': s })
           # ensure thug processes this url ie. the url is scanned with a view to sub-pages being scanned
           producer.send('4thug', { 'url': u2 })
    else:
           producer.send('failed_url_queue', { 'url': u, 'http_status': s })
