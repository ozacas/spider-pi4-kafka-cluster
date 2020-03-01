from scrapy import Request
from scrapy.pipelines.files import FilesPipeline, FileException
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
          self.success(t[1], item)
      else:
          # failed to download JS - indicator of malicious-ness???? so we need to send it to kafka
          self.error(item)
      return item

   def media_failure(self, failure, request, info):
      if not isinstance(failure.value, IgnoreRequest):
          d = { 'url': request.url, 'failure': dict(failure), 'when': str(datetime.utcnow()) }
          self.producer.send(self.settings.get('FILES_PIPELINE_FAILURE_TOPIC'), d)
      raise FileException    

   def success(self, response, item):
      response.update({ 'host': socket.gethostname(), 'when': str(datetime.utcnow()), 'origin': item.get('origin', None) })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_ARTEFACTS_TOPIC'), response)

   def error(self, failure):
      d = dict(failure)
      d.update({ 'when': str(datetime.utcnow()), 'host': socket.gethostname() })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_FAILURE_TOPIC'), d)
