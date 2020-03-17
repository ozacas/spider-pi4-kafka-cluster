
import re
from urllib.parse import urlparse, urljoin
from datetime import datetime
from dataclasses import asdict
from utils.models import ThugLog

#[2020-01-22 13:39:45] [MongoDB] Analysis ID: 5e27b5f1c2da48806667697e
analysis_regex = re.compile("^\[(.*?)\]\s+\[MongoDB\]\s+Analysis\s+ID:\s+([a-z0-9]+)$")

script_src_regex = re.compile("^\[(.*?)\]\s+\[script src redirection\]\s+(\S+)\s\-\>\s(\S+)\s*$")
link_href_regex  = re.compile("^\[(.*?)\]\s+\[link redirection\]\s+(\S+)\s\-\>\s(\S+)$")
# eg. [2020-01-22 13:42:17] [HTTP] URL: https://fonts.googleapis.com/css?family=Open+Sans:300,400,700 (Content-type: text/css; charset =utf-8, MD5: d2570265994455a6b680c3bf861bd52b)
url_regex = re.compile("^\[(.*?)\]\s+\[HTTP\]\s+URL\:\s+(\S+)\s*\(Content\-type\:\s+(\S+);\s+charset=(\S+),\s+MD5\:\s+([a-z0-9]+)\)\s*$")
anchor_regex = re.compile('^\[(.*?)\]\s+<a\s+href="([^"]+?)"\s*>.*$')

class ThugLogParser(object):
   def __init__(self, **kwargs):
      self.au_locator = kwargs.get('au_locator')
      self.db = kwargs.get('db')
      self.user_agent = kwargs.get('user_agent')
      self.origin = kwargs.get('origin')
   
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
      content = ''
      with open(filename, 'r') as fp:
          content = fp.read()
 
      concat_scripts = ''
      countries = set()
      for line in content:
          m = script_src_regex.match(line)
          if m:  
              u  = urljoin(self.origin, m.group(3))
              up = urlparse(u)
              ip = self.au_locator.as_ip(up.hostname)
              country = self.au_locator.country_code(ip)
              if country is None:
                  country = ''
              countries.add(country)
              concat_scripts.append(u+' ')

      rec = ThugLog(origin=self.origin, user_agent=self.user_agent, scripts=concat_scripts, 
                    script_countries=' '.join(countries), log=content, when=str(datetime.utcnow()))
      self.db.thug_log.insert_one(asdict(rec)) 
