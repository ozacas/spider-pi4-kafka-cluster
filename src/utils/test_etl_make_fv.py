#!/usr/bin/python3
import pytest
import mock
from utils.models import JavascriptArtefact
from etl_make_fv import report_failure, save_to_kafka, iterate

def test_report_failure():
    producer = mock.Mock()
    artefact = JavascriptArtefact(url='XXX', content_type='text/javascript', size_bytes=33, sha256='affb', md5='affc')
    report_failure(producer, artefact, 'reason=hello world')
    assert len(producer.method_calls) == 1
    name, args, kwargs = producer.method_calls[0]
    assert name == 'send'
    assert len(args) == 2
    assert args[0] == 'feature-extraction-failures'
    d = args[1].copy()
    assert 'when' in d
    assert isinstance(d['when'], str) and len(d['when']) > 10 # FIXME... stronger test... ?
    d.pop('when', None)
    expected_results = { 'url': 'XXX', 'sha256': 'affb', 'md5': 'affc', 'inline': False, 
                         'content_type': 'text/javascript', 'size_bytes': 33, 'origin': None, 
                         'reason': 'reason=hello world'}
    assert d == expected_results

def test_send_to_kafka():
   # 1. check incorrect input raises an error
   producer = mock.Mock()
   with pytest.raises(AssertionError):
       save_to_kafka(producer, {}, to='analysis-results')
   with pytest.raises(AssertionError):
       save_to_kafka(producer, { '_id': 'must not be present in input' }, to='analysis-results')

   # 2. check correct input is handled correctly
   results = { 'js_id': 'XXX' }
   save_to_kafka(producer, results, to='analysis-results')
   assert len(producer.method_calls) == 1
   name, args, kwargs = producer.method_calls[0]
   assert name == 'send'
   assert len(args) == 2
   assert args[0] == 'analysis-results'
   assert args[1] == results

def test_iterate():
   m1 = mock.Mock()
   m1.value = { 'content-type': 'text/javascript', 'url': 'http://blah.blah/...', 'sha256': 'XXX', 'md5': 'YYY', 'inline': False }
   m2 = mock.Mock()
   m2.value = { 'content-type': 'application/json' }
   consumer = [ m1, m2 ]
                
   ret = list(iterate(consumer, 2, [], verbose=False))
   assert len(ret) == 1  # only text/javascript is to be returned
   assert isinstance(ret[0], JavascriptArtefact)
   assert ret[0].url == 'http://blah.blah/...'
   assert ret[0].sha256 == 'XXX'
   assert ret[0].md5 == 'YYY'
   assert not ret[0].inline 
