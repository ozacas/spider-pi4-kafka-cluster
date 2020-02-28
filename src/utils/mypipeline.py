from scrapy import Request
from scrapy.pipelines.files import FilesPipeline
from kafka import KafkaProducer
import json
import socket
from datetime import datetime

class MyFilesPipeline(FilesPipeline):
   producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
 
   def get_media_requests(self, item, info):
      for url in item['file_urls']:
          yield Request(url, callback=self.done, errback=self.errback, dont_filter=True, cb_kwargs={'origin_url': item['origin'] })

   def done(self, response, origin_url=None):
      response.update({ 'host': socket.gethostname(), 
                        'when': str(datetime.utcnow()), 
                        'origin': origin_url })
      self.producer.send('javascript-artefacts2', d)
      return response

   def errback(self, failure, origin_url=None, item=None):
      d = { 'origin': origin_url,
            'when': str(datetime.utcnow()),
            'url': failure.request.url,
            'host': socket.gethostname() }
      self.producer.send('javascript-download-failure', d)
