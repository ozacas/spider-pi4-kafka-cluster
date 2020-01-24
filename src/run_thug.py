#!/usr/bin/python3
from kafka import KafkaConsumer, KafkaProducer
from subprocess import Popen
from utils.ThugLogParser import ThugLogParser
from urllib.parse import urlparse
from datetime import datetime
import os
import tempfile
import json
import random
import pymongo

consumer = KafkaConsumer('4thug', bootstrap_servers='kafka1', auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers='kafka1')
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

max_objects = 100 # does not include other links
for message in consumer:
        url = message.value.get('url') 
        # thug will produce 1) mongodb output 2) log file
        now = str(datetime.utcnow())
        # We process the log here... and push worthy stuff into the relevant queues
        with tempfile.NamedTemporaryFile() as fp:
           user_agent = random.choice(ua)     # use a random UA for each url fetched to try to maximise return of suspicious objects over time
           with Popen(["/usr/bin/thug",
                       "--json-logging",      # elasticsearch logging????
                       "--delay=5000",        # be polite
                       "--useragent={}".format(user_agent), # choose random user agent from supported list to maximise coverage
                       "--features-logging",  # ensure JS snippets are recorded in mongo
                       "--no-javaplugin",     # disable functionality we dont need
                       "--no-shockwave", 
                       "--no-adobepdf",
                       "--no-silverlight",
                       "--no-honeyagent",
                       "-t{}".format(max_objects),           # max 100 requests per url from kafka
                       "--verbose",           # log level == INFO so we can get sub-resources to fetch
                       url
                ], stderr=fp) as proc:
               try:
                   status = proc.wait(timeout=3600) # max 1 hour for thug
                   if status == 0 or status == 1: # thug succeed?
                       # will send messages based on log
                       ThugLogParser(producer, context={ 'thug_pid': os.getpid(), 'thug_host': host, 
                                                     'when': now, 'thug_exit_status': status, 'ended': str(datetime.utcnow()),
                                                     'url_scanned': url, 'user_agent_used': user_agent},
                                 geo2_db_location="/home/acas/data/GeoLite2-City_20200114/GeoLite2-City.mmdb",
                                 mongo=pymongo.MongoClient('192.168.1.80')).parse(fp.name) 
                   else:
                       producer.send('thug_failure', { 'url_scanned': url, 'exit_status': status, 
                                                   'when': now, 'user_agent_used': user_agent } )
               except Exception as e:
                       producer.send('thug_failure', { 'url_scanned': url, 'exit_status': -1, # -1 == exception eg. timeout
                                                   'when': now, 'user_agent_used': user_agent, 'msg': str(e) })
