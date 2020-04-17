#!/usr/bin/python3
import pymongo
import mock
from utils.geo import AustraliaGeoLocator
from utils.thug_parse import ThugLogParser

def test_thug_parser(pytestconfig):
    mongo = { }
    mongo['thug'] = mock.Mock()
    ThugLogParser(context={ 'url': 'foo' }, 
              au_locator=AustraliaGeoLocator(),
              mongo=mongo).parse("{}/data/example10.log".format(pytestconfig.rootdir))
