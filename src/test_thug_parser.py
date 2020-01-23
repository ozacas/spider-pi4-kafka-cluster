#!/usr/bin/python3

from utils.ThugLogParser import ThugLogParser

class MockProducer(object):
   def send(*args, **kwargs):
      print(args)
      print(kwargs)

ThugLogParser(MockProducer(), context={ 'url': 'foo' }, 
              geo2_db_location="/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb").parse("../data/example9.log")
