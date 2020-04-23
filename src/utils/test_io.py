from unittest.mock import Mock
from dataclasses import dataclass
from typing import Dict
import pytest
from utils.io import *

def test_save_ast_vector():
   db = Mock()
   jsr = JavascriptArtefact(url="http://...foo.js", origin="http://....somepage.html", sha256='XXX', md5='XXX')
   save_ast_vector(db, jsr, { 'v1': 2, 'v2': 9 }, js_id="object_id_blah_blah")
   assert len(db.method_calls) == 1
   name, args, kwargs = db.method_calls[0]
   assert name == "statements_by_count.insert_one"
   assert len(args) == 1
   assert kwargs == {}
   assert args[0] == { 'js_id': 'object_id_blah_blah', 'url': 'http://...foo.js', 'origin': 'http://....somepage.html', 'v1': 2, 'v2': 9 }

def test_save_call_vector():
   db = Mock()
   jsr = JavascriptArtefact(url='1.js', origin='2.html', sha256='XXX', md5='XXX')
   save_call_vector(db, jsr, { 'eval': 2, 'parseJSON': 1 }, js_id="123eff33")
   assert len(db.method_calls) == 1
   name, args, kwargs = db.method_calls[0]
   assert kwargs == {}
   assert len(args) == 1
   assert args[0] == {'js_id': '123eff33', 'url': '1.js', 'origin': '2.html', 'calls': {'eval': 2, 'parseJSON': 1}}
   assert name == "count_by_function.insert_one"

@dataclass
class Junk:
   value: Dict
   crap: int = 0

def test_next_artefact():
   # test that no filter works as expected
   j = Junk(value={ 'ast_dist': 10.0, 'function_dist': 10.0}, crap=22)
   d = [ j ]
   results = list(next_artefact(d, filter_cb=None))
   assert len(results) == 1
   assert results[0] == j.value

   # test that filtering work as expected
   j2 = Junk(value={ 'ast_dist': 0.0, 'function_dist': 0.02}, crap=99) 
   d = [ j, j2 ]
   results = list(next_artefact(d, filter_cb=lambda v: v['ast_dist'] < 0.01 or v['function_dist'] < 0.01))
   assert len(results) == 1
   assert results[0] == j2.value

def test_artefact_tuple():
   type = artefact_tuple()
   t = type(url='http://blahblah', path='foo/bar/baz.js', checksum='XXX', host='pi5', origin='http://some.page/here.html', when='2019-20...')
   assert t.when is not None and isinstance(t.when, str)
   assert t.url == 'http://blahblah'
   assert t.path == 'foo/bar/baz.js'
   assert t.checksum == 'XXX'
   assert t.host == 'pi5'
   assert t.origin == 'http://some.page/here.html'

def test_save_url():
   tuple = artefact_tuple()
   t = tuple(url='http://www.blah.blah/foo.js', path='full/123yu4u43yu4y43.js',
             checksum='1a...', host='pi2', origin='http://www.blah.blah/index.html', when='2019-10-10')
   m = Mock() 
   ret = save_url(m, t)
   assert ret is not None # TODO FIXME: better test than this...
   assert len(m.method_calls) == 1
   name, args, kwargs = m.method_calls[0]
   assert name == 'urls.insert_one'
   assert len(args) == 1
   assert args[0] == {'url': 'http://www.blah.blah/foo.js', 'last_visited': '2019-10-10', 'origin': 'http://www.blah.blah/index.html'}
   assert len(kwargs) == 0

def test_save_script():
   m = Mock()
   tuple = artefact_tuple()
   # checksum corresponds to the rubbish code below...
   jsr = tuple(url='X', path='full/foo.js', checksum='f8f4392b9ce13de380ecdbe256030807', host='pi1', origin='foo.html', when='2020-04-20')
   ret = save_script(m, jsr, 'console.write.(blah blah)'.encode())
   assert ret is not None
   assert isinstance(ret, dict)
   assert 'sha256' in ret
   assert 'md5' in ret
   assert 'size_bytes' in ret 
