
import re
from urllib.parse import urlparse, urljoin
from .AustraliaGeoLocator import AustraliaGeoLocator
from datetime import datetime

#[2020-01-22 13:39:45] [MongoDB] Analysis ID: 5e27b5f1c2da48806667697e
analysis_regex = re.compile("^\[(.*?)\]\s+\[MongoDB\]\s+Analysis\s+ID:\s+([a-z0-9]+)$")

script_src_regex = re.compile("^\[(.*?)\]\s+\[script src redirection\]\s+(\S+)\s\-\>\s(\S+)\s*$")
link_href_regex  = re.compile("^\[(.*?)\]\s+\[link redirection\]\s+(\S+)\s\-\>\s(\S+)$")
# eg. [2020-01-22 13:42:17] [HTTP] URL: https://fonts.googleapis.com/css?family=Open+Sans:300,400,700 (Content-type: text/css; charset =utf-8, MD5: d2570265994455a6b680c3bf861bd52b)
url_regex = re.compile("^\[(.*?)\]\s+\[HTTP\]\s+URL\:\s+(\S+)\s*\(Content\-type\:\s+(\S+);\s+charset=(\S+),\s+MD5\:\s+([a-z0-9]+)\)\s*$")
anchor_regex = re.compile('^\[(.*?)\]\s+<a\s+href="([^"]+?)"\s*>.*$')

class ThugLogParser(object):
   def __init__(self, producer, context={}, geo2_db_location=None, mongo=None):
      self.producer = producer
      self.au_locator = AustraliaGeoLocator(db_location=geo2_db_location)
      self.context = context
      self.mongo = mongo
      pass
   
   def is_au(self, src_url):
      if src_url is None or len(src_url) < 1 or src_url == '#':
          return False
      up = urlparse(src_url)
      ret = self.au_locator.is_au(up.hostname)
      return ret
 
   def is_already_seen(self, url):
      if self.mongo is None:
          return False
      db = self.mongo.thug
      urls = db.urls
      if urls:
          return urls.count_documents({ 'url': url }) > 0
      return False
 
   def parse(self, filename):
      self.scripts = list()
      self.urls = list()
      self.anchors = set() # NB: these wont be visited by thug in the log, but instead intended for further traversal
      self.metadata = { }
      analysis_id = ''
      when = ''
      with open(filename, 'r') as fp:
          for line in fp:
             m = script_src_regex.match(line)
             if m:
                 self.scripts.append({ 'when': m.group(1), 'origin_url': m.group(2), 'src': m.group(3)})
             else:
                 m = link_href_regex.match(line)
                 if m:
                     self.urls.append({ 'when': m.group(1), 'origin_url': m.group(2), 'href': m.group(3)})
                 else:
                      m = url_regex.match(line)
                      if m:
                          self.metadata[m.group(2)] = { 'when': m.group(1), 'charset': m.group(4), 'md5': m.group(5), 
                                                        'url': m.group(2), 'content-type': m.group(3) }
             m = anchor_regex.match(line)
             if m:
                 href = m.group(2)
                 if '://' not in href or not href.startswith('//'):
                     href = urljoin(self.context.get('url_scanned'), href)
                 self.anchors.add(href)
             m = analysis_regex.match(line)
             if m:
                 analysis_id = m.group(2)
                 when = m.group(1)


      # do not stop processing the thug queue on error...
      try:
          au_anchors = list([ u for u in self.anchors if self.is_au(u) ])
          for au in au_anchors:
              if not self.is_already_seen(au):
                  self.producer.send('4thug', { 'url': au })
              else:
                  self.producer.send('thug-already-seen', { 'url': au, 'now': str(datetime.utcnow()) })
          self.context.update({ 'analysis_id': analysis_id, 'more_pages_to_visit': len(au_anchors), 'started_at': when })
          self.producer.send('thug-completed-analyses', self.context)
      except Exception as e:
          print(e)
