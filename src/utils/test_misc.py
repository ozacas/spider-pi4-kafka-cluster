#!/usr/bin/python3

import pytest
import json
from utils.misc import *
from urllib.parse import urlparse

def test_as_priority():
   pri = as_priority(urlparse('https://www.google.com'))
   assert pri is not None and pri <= 2

def test_json_utf8_clean():
   a = { '\u0022': 10, 'B\u0654': 20 }
   encoder = json_value_serializer()
   s = encoder(a)
   assert s.__class__ == bytes
   assert len(s) == 18   # ensure minimal whitespace is correctly performed
   decoder = json_value_deserializer()
   s2 = decoder(s)
   assert s2.__class__ == dict
   assert s2 == a
   assert s2['"'] == 10

def test_random_user_agent():
   ua = random_user_agent()
   assert ua is not None and len(ua) > 0

def test_javascript_only():
   cb = javascript_only()
   d = { 'content_type': 'application/rubbish', 'size_bytes': 2000 }
   assert not cb(d) 
   d = { 'content-type': 'application/javascript', 'size_bytes': 2000 }  # javascript_only() MUST support both content-type and content_type
   assert cb(d) 
   d = { 'content_type': 'text/javascript', 'size_bytes': 3000 }
   assert cb(d)
   d = { 'content_type': 'text/javascript', 'size_bytes': 1499 }  # must be false since bytes < 1500
   assert not cb(d)
