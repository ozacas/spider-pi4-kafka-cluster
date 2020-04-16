#!/usr/bin/python3

import pytest
from utils.misc import as_priority
from urllib.parse import urlparse

def test_as_priority():
   pri = as_priority('https://www.google.com', None)
   assert pri is not None and pri <= 3

   # avoid re-parsing url's if we can
   up = urlparse('https://www.google.com')
   t2 = as_priority('https://www.google.com', up)
   assert t2 is not None and t2 == pri
