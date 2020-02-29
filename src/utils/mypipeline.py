from scrapy import Request
from scrapy.pipelines.files import FilesPipeline
from scrapy.utils.project import get_project_settings
from kafka import KafkaProducer
import json
import socket
from datetime import datetime

class MyFilesPipeline(FilesPipeline):
   producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
   settings = get_project_settings()

   def get_media_requests(self, item, info):
      for url in item['file_urls']:
          yield Request(url, dont_filter=True)

   def item_completed(self, results, item, info):
      item = super().item_completed(results, item, info)
      # submit completion to kafka queue (only if item successfully saved)
      t = results[0]
      if t[0]:
          self.success(t[1])
      else:
          # failed to download JS - indicator of malicious-ness???? so we need to send it to kafka
          self.error(item)
      return item

   def success(self, response):
      d.update({ 'host': socket.gethostname(), 'when': str(datetime.utcnow()), 'origin': item.get('origin', None) })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_ARTEFACTS_TOPIC'), d)

   def error(self, failure):
      d = dict(failure)
      d.update({ 'when': str(datetime.utcnow()), 'host': socket.gethostname() })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_FAILURE_TOPIC'), d)
