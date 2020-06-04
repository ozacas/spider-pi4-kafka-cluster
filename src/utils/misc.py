import signal
import os
import random
import json
from utils.models import Password
from urllib.parse import urlparse

def json_value_serializer():
    """
    Returns a callable which serialises the supplied value with correct handling of UTF-8 encoded literals in artefacts
    """
    return lambda v: json.dumps(v, ensure_ascii=False, separators=(',', ':')).encode('utf-8')

def json_value_deserializer():
    """
    Returns a callable for correctly decoding a message which has been serialized via json_value_serializer() (or compatible)
    """
    return lambda v: json.loads(v)  # v can be bytes with python 3.6 (assumed utf8 et al.)

def random_user_agent(search_paths=None):
    """
    Examines search_paths which are directories of thug personalities and return a random userAgent string contained therein.
    Useful for those systems with thug installed.
    """
    if search_paths is None:
        search_paths = [ '/etc/thug/personalities', '/home/acas/src/thug/thug/DOM/personalities']
    ua = None
    agents = []
    random.seed()
    for dir in filter(lambda dir: os.path.exists(dir), search_paths):
        #print(dir)
        for dir, subdirs, files in os.walk(dir):
            json_files = ["{}/{}".format(dir, f) for f in files if f.endswith(".json")]
            for jsf in json_files:
                with open(jsf, 'r') as fp:
                    rec = json.load(fp)
                    agents.append(rec.get('userAgent'))

    assert len(agents) > 0
    ret = random.choice(agents)
    assert len(ret) > 0
    return ret

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

def as_priority(up): # up == result from urlparse for a given url
   assert up is not None
   priority = 1
   penalty = 0
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
   ulen = len(up.geturl())
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

def add_mongo_arguments(args, default_host='pi1', default_port=27017, default_db='au_js', default_access='read-only', default_user=None):
   args.add_argument("--db", 
                     help="Mongo host/ip to save to [{}]".format(default_host), 
                     type=str, default=default_host)
   args.add_argument("--port", 
                     help="TCP port to access mongo db [{}]".format(str(default_port)), 
                     type=int, default=default_port)
   args.add_argument("--dbname", 
                     help="Name on mongo DB to access [{}]".format(default_db), 
                     type=str, default=default_db)
   dict_args = { 'help': "MongoDB RBAC username to use ({} access required) [{}]".format(default_access, default_user),
                 'type': str,
                 'required': True 
               }
   if default_user is not None:
       dict_args.pop('required', None)
       dict_args.update({ 'default': default_user })
   args.add_argument("--dbuser", **dict_args)
   args.add_argument("--dbpassword", 
                     help="MongoDB password for user (prompted if not supplied)", type=Password, default=Password.DEFAULT)

def add_kafka_arguments(args, producer=False, consumer=True, default_from='', default_group='', default_to=''):
   args.add_argument("--bootstrap", 
                  help="Kafka bootstrap servers [kafka2]", 
                  type=str, default="kafka2")
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
       args.add_argument("--n",
                      help="Consume no more than N records [Inf]",
                      type=float, default=float('Inf'))
   if producer:
       args.add_argument("--to", help="Output records to specified topic [{}]".format(default_to), type=str, default=default_to)

def add_debug_arguments(args):
   args.add_argument("--v", help="Debug verbosely", action="store_true")
