#!/usr/bin/python3
import pymongo
from unittest.mock import Mock
from utils.geo import AustraliaGeoLocator
from utils.thug_parse import ThugLogParser

def test_thug_parser(pytestconfig):
    mongo = { }
    mongo['thug'] = Mock()
    ThugLogParser(context={ 'url': 'foo' }, 
              au_locator=AustraliaGeoLocator(),
              mongo=mongo).parse("{}/data/example10.log".format(pytestconfig.rootdir))
    m = mongo['thug'] 
    assert len(m.method_calls) == 1
    name, args, kwargs = m.method_calls[0] 
    assert name == "thug_log.insert_one" 
    assert len(args) == 1
    assert len(kwargs) == 0
