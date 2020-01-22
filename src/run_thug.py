#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from subprocess import Popen, wait, PIPE
from utils import ThugLogParser
from urllib.parse import urlparse
import os
import random

consumer = KafkaConsumer('4thug', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))
host = os.uname()[1]

ua = [ "winxpie60", "winxpie61", "winxpie70", "winxpie80", "winxpchrome20",
        "winxpfirefox12", "winxpsafari5", "win2kie60", "win2kie80",
        "win7ie80", "win7ie90", "win7ie100", "win7chrome20",
        "win7chrome40", "win7chrome45", "win7chrome49", "win7firefox3",
        "win7safari5", "win10ie110", "osx10chrome19", "osx10safari5",
        "linuxchrome26", "linuxchrome30", "linuxchrome44", "linuxchrome54",
        "linuxfirefox19", "linuxfirefox40", "galaxy2chrome18", "galaxy2chrome25",
        "galaxy2chrome29", "nexuschrome18", "ipadchrome33", "ipadchrome37",
        "ipadchrome38", "ipadchrome39", "ipadchrome45", "ipadchrome46",
        "ipadchrome47", "ipadsafari7", "ipadsafari8", "ipadsafari9" ]

def determine_coverage(url):
    # url has no path ie. homepage - do extensive scan to get started
    u = urlparse(url)
    if u.path is None or len(u.path) == 0 or u.path == '/':
       return (500, "--extensive")
     
    # default is to fetch a maximum of 100 objects on the requested page and not follow anchors
    return (100, "")

for message in consumer:
        url = message.get('url') 
        max_objects, extensive = determine_coverage(url)
        # thug will produce 1) mongodb output 2) log file

        # We process the log here... and push worthy stuff into the relevant queues
        with tempfile.NamedTemporaryFile() as fp:
           user_agent = random.choice(ua)
           with Popen(["/usr/bin/thug",
                       "--json-logging",      # elasticsearch logging????
                       "--delay=5000",        # be polite
                       "--useragent={}".format(user_agent), # choose random user agent from supported list to maximise coverage
                       "--features-logging",  # ensure JS snippets are recorded in mongo
                       "--no-javaplugin",     # disable functionality we dont need
                       "-t", "{}".format(max_objects),           # max 100 requests per url from kafka
                       "{}".format(extensive),      # extensive search ie. follow anchors?
                       url
                ], stderr=fp) as proc:
               status = proc.wait() 
               producer.send('thug_completion', { 'pid': os.getpid(), 'hostname': host, 'when': now, 'status_code': status, "url_scanned": url, "user_agent_used":  user_agent })
               if status in [0,1]: # thug succeed?
                   ThugLogParser(producer).parse(tmp_filename) # will send messages based on log
               else:
                   producer.send('thug_failure', { 'url_scanned': url, 'exit_status': status, 'when': now, "user_agent_used": user_agent } )
