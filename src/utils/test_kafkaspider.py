#!/usr/bin/python3
import pytest
import mock
from kafkaspider import KafkaSpiderMixin
from urllib.parse import urlparse

class CongestBatchStub(KafkaSpiderMixin):
    def __init__(self, *args, **kwargs):
        self.congested_batch = { } # must be an iterable
        self.logger = mock.Mock()

class PenaliseStub(KafkaSpiderMixin):
    def __init__(self, *args, **kwargs):
        self.recent_sites = {}
       
class RecentCrawlStub(PenaliseStub):
    def __init__(self, *args, **kwargs):
        super().__init__() 
        self.settings = { 'LRU_MAX_PAGES_PER_SITE': 10 }

class SuitableStub(KafkaSpiderMixin):
    recent_cache = {}

    def phoney_reject(self, up):
        return True # NB: true means reject the url
 
class OutsideAuStub(KafkaSpiderMixin):
    au_locator = mock.Mock()

def test_is_congested_batch():
    stub = CongestBatchStub()
    up = urlparse('mailto:blah@blah.blah.com')
    assert stub.is_congested_batch(up) == True

    up = urlparse('https://www.google.com')
    assert stub.is_congested_batch(up) == False
    assert stub.congested_batch == { 'www.google.com': 1 }

def test_penalise():
    stub = PenaliseStub()
    for host in ['www.google.com', 'www.blah.blah', 'www.google.com']:
         stub.penalise(host) # default penalty must be one
    assert stub.recent_sites == { 'www.google.com': 2, 'www.blah.blah': 1 }

    ret = stub.penalise('www.google.com', penalty=2)
    assert ret == 4
    assert stub.recent_sites == { 'www.google.com': 4, 'www.blah.blah': 1 }

def test_is_recently_crawled():
    stub = RecentCrawlStub()
    assert not stub.is_recently_crawled(urlparse('https://www.google.com'))

    for i in range(12):
         up = urlparse('http://www.google.com')
         stub.penalise(up.hostname) # NB: pretend we have visited the page
         ret = stub.is_recently_crawled(up)
         if i < 10:
            assert not ret
         else:
            assert ret 

    assert stub.recent_sites == { 'www.google.com': 12 } 

def test_is_outside_au():
    stub = OutsideAuStub()
    # 1. if a host ends with .au - it is considered in-scope (no geo performed as performance win)
    assert not stub.is_outside_au(urlparse('http://blah.blah.com.au')) and len(stub.au_locator.method_calls) == 0

    # 2. if we need to geo, ie. not .AU then it must match the expected...
    stub.au_locator.is_au.return_value = False
    assert stub.is_outside_au(urlparse('http://blah.blah.com'))

def test_is_suitable():
    stub = SuitableStub()
    #   def is_suitable(self, url, stats=None, rejection_criteria=None):
    assert stub.is_suitable('https://www.google.com', rejection_criteria=()) 

    stats = {}
    assert stub.is_suitable('https://www.google.com', stats=stats, rejection_criteria=[])
    assert stats == {}

    # must raise since 'phony_reject' is not a valid key in stats
    with pytest.raises(KeyError):
        ret = stub.is_suitable('https://www.google.com', stats=stats, rejection_criteria=[('phony_reject', stub.phoney_reject)]) 

    stats['phony_reject'] = 22
    assert stub.is_suitable('https://www.google.com', stats=stats, rejection_criteria=[('phony_reject', stub.phoney_reject)]) == False
    assert stats == { 'phony_reject': 23 } 

    
