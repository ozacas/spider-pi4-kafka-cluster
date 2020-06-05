from scrapy import Request
from scrapy.pipelines.files import FilesPipeline, FileException
from scrapy.utils.project import get_project_settings
from scrapy.utils.request import referer_str
from kafka import KafkaProducer
import json
import socket
import pylru
from datetime import datetime

class MyFilesPipeline(FilesPipeline):
   producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
   settings = get_project_settings()
   bad_sites = pylru.lrucache(50)

   def get_media_requests(self, item, info):
      for url in item['file_urls']:
          up = urlparse(url)
          host = up.hostname
          if not host in self.bad_sites or self.bad_sites[host] < 10:
              yield Request(url, dont_filter=True, headers={ 'Referer': item['origin'] })

   def bad_site(self, url):
      up = urlparse(url)
      host = up.hostname
      if host is not None:
          if not host in self.bad_sites:
              self.bad_sites[host] = 0
          self.bad_sites[host] += 1

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

   def media_failed(self, failure, request, info):
      if not isinstance(failure.value, IgnoreRequest):
          d = { 'url': request.url, 'failure': dict(failure), 'when': str(datetime.utcnow()), 'origin': referer_str(request) }
          self.bad_site(d['url'])
          self.producer.send(self.settings.get('FILES_PIPELINE_FAILURE_TOPIC'), d)
      raise FileException    

   def media_downloaded(self, response, request, info):
      try:
          return super().media_downloaded(response, request, info)
      except FileException as fe:
          failure = { 'url': response.url, 'origin': referer_str(request), 'reason': str(fe), 'http-status': response.status }
          self.bad_site(failure['url'])
          self.error(failure)
          raise fe

   def success(self, response, item):
      response.update({ 'host': socket.gethostname(), 'when': str(datetime.utcnow()), 'origin': item.get('origin', None) })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_ARTEFACTS_TOPIC'), response)

   def error(self, failure):
      d = dict(failure)
      d.update({ 'when': str(datetime.utcnow()), 'host': socket.gethostname() })
      self.producer.send(self.settings.get('FILES_DOWNLOAD_FAILURE_TOPIC'), d)
