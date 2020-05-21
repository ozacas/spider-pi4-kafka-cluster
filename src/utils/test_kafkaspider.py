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
