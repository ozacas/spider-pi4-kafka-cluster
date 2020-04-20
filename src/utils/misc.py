import signal
import os
from utils.models import Password
from urllib.parse import urlparse

def setup_signals(callback):
   for s in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
       signal.signal(s, callback)

def rm_pidfile(suffix, root="/home/acas"):
   # NB: must not raise an exception if not found (which can happen with independent system activity)
   try: 
       os.unlink("{}/{}".format(root, suffix))
   except Exception as e:
       print("WARNING: rm_pidfile({}, {}): {}".format(suffix, root, str(e)))

def save_pidfile(suffix, root="/home/acas"):
   with open("{}/{}".format(root, suffix), 'w+') as fp:
       fp.write(str(os.getpid()))

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

def add_extractor_arguments(args, default_extractor="/home/acas/src/extract-features.jar"):
   args.add_argument("--java", 
                     help="Path to JVM executable [/usr/bin/java]", 
                     type=str, default="/usr/bin/java")
   args.add_argument("--extractor", 
                      help="Path to feature extractor JAR [{}]".format(default_extractor), 
                      type=str, 
                      default=default_extractor)

def add_mongo_arguments(args, default_host='pi1', default_port=27017, default_db='au_js', default_access='read-only'):
   args.add_argument("--db", 
                     help="Mongo host/ip to save to [{}]".format(default_host), 
                     type=str, default=default_host)
   args.add_argument("--port", 
                     help="TCP port to access mongo db [{}]".format(str(default_port)), 
                     type=int, default=default_port)
   args.add_argument("--dbname", 
                     help="Name on mongo DB to access [{}]".format(default_db), 
                     type=str, default=default_db)
   args.add_argument("--dbuser", 
                     help="MongoDB RBAC username to use ({} access required)".format(default_access), type=str, required=True)
   args.add_argument("--dbpassword", 
                     help="MongoDB password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)

def add_kafka_arguments(args, producer=False, consumer=True, default_from='', default_group='', default_to=''):
   args.add_argument("--bootstrap", 
                  help="Kafka bootstrap servers [kafka1]", 
                  type=str, default="kafka1")
   if consumer:
       args.add_argument("--consume-from", 
                         help="Kafka topic to read input data from [{}]".format(default_from), 
                         type=str, default=default_from)
       args.add_argument("--group", 
                      help="Use specified kafka consumer group to remember where we left off [{}]".format(default_group), 
                      type=str, default=default_group)
       args.add_argument("--start", 
                      help="Consume from earliest|latest message available in from topic [latest]", 
                      type=str, default='latest', choices=('earliest', 'latest'))
   if producer:
       args.add_argument("--to", help="Output records to specified topic [{}]".format(default_to), type=str, default=default_to)

def add_debug_arguments(args):
   args.add_argument("--v", help="Debug verbosely", action="store_true")
