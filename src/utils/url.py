from urllib.parse import urlparse

def as_priority(url, up):
   priority = 1
   penalty = 0
   if up is None:
       up = urlparse(url)
   if up.path != '/':
       penalty = up.path.count('/')  # penalty of up to three depending on depth of page
   if penalty > 3:
       penalty = 3
   if up.path.lower().endswith((".mp3", ".avi", ".pdf", ".png", ".vob", ".mpeg", ".mpg", ".mp4", ".jpeg", ".jpg")):
       penalty = penalty + 2
   qlen = len(up.query)
   if qlen > 0 and qlen < 20:
      penalty = penalty + 1
   elif qlen > 0:
      penalty = penalty + 2
   ulen = len(url)
   if ulen > 150: # silly state-carrying URLs eg. SAML sign-on are not a priority right now
      penalty = penalty + 2
   elif ulen > 80:
      penalty = penalty + 1
   return priority + penalty

