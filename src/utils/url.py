from urllib.parse import urlparse

def as_priority(url, up=None):
   priority = 1
   penalty = 0
   if up is None:
       up = urlparse(url)
   if up.path != '/':
       penalty = up.path.count('/')  # penalty of up to three depending on depth of page
   if penalty > 3:
       penalty = 3
   qlen = len(up.query)
   if qlen > 0 and qlen < 20:
      penalty = penalty + 1
   elif qlen > 0:
      penalty = penalty + 2
   if 'anu.edu.au' in up.hostname:
      penalty = penalty + 5
   if len(url) > 200:
      penalty = penalty + 2
   elif len(url) > 100:
      penalty = penalty + 1
   return priority + penalty

