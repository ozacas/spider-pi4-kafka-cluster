#!/usr/bin/python3
import pytest
from utils.cdn import CDNJS, JSDelivr

def test_live_cdnjs():
    provider = CDNJS()
    # fetch(self, family, variant, version, ignore_i18n=True, provider=None):
    results = list(provider.fetch('jquery', None, '2.0.1'))
    assert len(results) == 2 # min.js and non-minified
    results = list(provider.fetch('jquery', 'min', '2.0.1', provider='cdnjs'))
    assert len(results) == 1 # now only min.js
    assert results[0] == ('https://cdnjs.cloudflare.com/ajax/libs/jquery/2.0.1/jquery.min.js', 'jquery', 'min', '2.0.1', 'cdnjs')

def test_live_jsdelivr():
    provider = JSDelivr()
    results = list(provider.fetch('jquery', None, '2.1.0', provider='jsdelivr'))
    assert len(results) == 93 
    # eg. ('https://cdn.jsdelivr.net/npm/jquery@2.1.0/dist/cdn/jquery-2.1.0.js', 'jquery', None, '2.1.0', 'jsdelivr')
    for url, family, variant, version, provider in results:
        assert url.startswith('https://cdn.jsdelivr.net/npm/jquery@2.1.0/')
        assert family == 'jquery'
        assert variant is None
        assert version == '2.1.0'
        assert provider == 'jsdelivr'
