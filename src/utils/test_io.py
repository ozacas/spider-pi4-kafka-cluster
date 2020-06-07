from unittest.mock import Mock
from dataclasses import dataclass
from typing import Dict
import pytest
from utils.models import DownloadArtefact
from utils.io import *

@dataclass
class Junk:
   value: Dict
   crap: int = 0

def test_next_artefact():
   # test that no filter works as expected
   j = Junk(value={ 'ast_dist': 10.0, 'function_dist': 10.0}, crap=22)
   d = [ j ]
   results = list(next_artefact(d, 10, None))
   assert len(results) == 1
   assert results[0] == j.value

   # test that filtering work as expected
   j2 = Junk(value={ 'ast_dist': 0.0, 'function_dist': 0.02}, crap=99) 
   d = [ j, j2 ]
   results = list(next_artefact(d, 10, lambda v: v['ast_dist'] < 0.01 or v['function_dist'] < 0.01))
   assert len(results) == 1
   assert results[0] == j2.value

def test_next_artefact_valid_correct_arg_passing():
   l = []
   # first valid calling
   results = list(next_artefact(l,  10, lambda v: True, verbose=True))
   assert len(list(results)) == 0
   results = list(next_artefact(l,  10, lambda v: False, verbose=False))
   assert len(list(results)) == 0
   l = [Junk(value={ 'ast_dist': 10.0, 'function_dist': 5.0}, crap=12)]
   results = list(next_artefact(l,  10, lambda v: True))
   assert len(list(results)) == 1
   results = list(next_artefact(l,  10, lambda v: False))
   assert len(list(results)) == 0

   # and now invalid must raise a TypeError
   with pytest.raises(TypeError):
       results = list(next_artefact(l, lambda v: False)) # max positional argument must be float
   with pytest.raises(TypeError):
       results = list(next_artefact(l, 10, False)) # third positional argument must be a callable
   with pytest.raises(TypeError):
       results = list(next_artefact(l, 10, False, lambda v: False)) # third positional argument must be a callable
   with pytest.raises(TypeError):
       results = list(next_artefact(l, 10)) # no default for third positional argument

def test_download_artefact(): # should really be in test_models... but historical legacy...
   t = DownloadArtefact(url='http://blahblah', path='foo/bar/baz.js', checksum='XXX', host='pi5', origin='http://some.page/here.html', when='2019-20...')
   assert t.when is not None and isinstance(t.when, str)
   assert t.url == 'http://blahblah'
   assert t.path == 'foo/bar/baz.js'
   assert t.checksum == 'XXX'
   assert t.host == 'pi5'
   assert t.origin == 'http://some.page/here.html'

def test_save_url():
   t = DownloadArtefact(url='http://www.blah.blah/foo.js', path='full/123yu4u43yu4y43.js',
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

def test_save_script_correct_checksum():
   m = Mock()
   m.scripts.find_one_and_update.return_value = { '_id': "abc123" }

   # checksum corresponds to the rubbish code below...
   jsr = DownloadArtefact(url='X', path='full/foo.js', checksum='f8f4392b9ce13de380ecdbe256030807', host='pi1', origin='foo.html', when='2020-04-20')
   ret, js_id = save_script(m, jsr, 'console.write.(blah blah)'.encode())
   assert isinstance(ret, dict)
   assert js_id == "abc123"
   assert 'sha256' in ret
   assert 'md5' in ret
   assert 'size_bytes' in ret 

def test_save_script_incorrect_checksum():
   m = Mock()
   jsr = DownloadArtefact(url='X', path='full/foo.js', checksum='X' * 32, host='pi3', origin='crap.html', when='2020-04-25')
   with pytest.raises(ValueError):
       ret = save_script(m, jsr, 'rubbish.code.which.does.not.match.required.checksum'.encode())
   # mongo database must not have been updated
   assert(len(m.method_calls) == 0)

def test_batch():
   l = [1,2,3]
   # must batch correctly
   ret = list(batch(l, n=2))
   assert ret is not None
   assert ret[0] == (1,2,)
   assert ret[1] == (3,)

   # and also support iterables that have no __len__()
   i = iter([1,2,3])
   ret = list(batch(i, n=3))
   assert ret == [(1, 2, 3)]
