#!/usr/bin/python3

from utils.ThugLogParser import ThugLogParser

class MockProducer(object):
   def send(*args, **kwargs):
      print(args)
      print(kwargs)

ThugLogParser(MockProducer(), context={ 'url': 'foo' }).parse("../data/example9.log")
