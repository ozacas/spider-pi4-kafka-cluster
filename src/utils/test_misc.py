#!/usr/bin/python3

import pytest
from utils.misc import as_priority
from urllib.parse import urlparse

def test_as_priority():
   pri = as_priority(urlparse('https://www.google.com'))
   assert pri is not None and pri <= 2

