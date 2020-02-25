from scrapy.pipelines.files import FilesPipeline
from kafka import KafkaProducer
import json
import socket
from datetime import datetime

class MyFilesPipeline(FilesPipeline):
   producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')

   def item_completed(self, results, item, info):
      item = super().item_completed(results, item, info)
      # submit completion to kafka queue (only if item successfully saved)
      t = results[0]
      if t[0]:
          d = t[1] 
          d.update({ 'host': socket.gethostname(), 'when': str(datetime.utcnow()), 'origin': item.get('origin', None) })
          self.producer.send('javascript-artefacts', d)
      else:
          # failed to download JS - indicator of malicious-ness???? so we need to send it to kafka
          d = dict(item)
          d.update({ 'when': str(datetime.utcnow()) })
          self.producer.send('javascript-download-failure', d)
      return item
