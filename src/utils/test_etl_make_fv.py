#!/usr/bin/python3
import pytest
import mock
from utils.models import JavascriptArtefact
from etl_make_fv import report_failure

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
    expected_results = {'url': 'XXX', 'sha256': 'affb', 'md5': 'affc', 'inline': False, 'content_type': 'text/javascript', 'size_bytes': 33, 'origin': None, 'reason': 'reason=hello world'}
    assert d == expected_results

def test_send_to_kafka():
    pass
