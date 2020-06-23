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
   # hash values must correspond to the script_bytes... or else the code-under-test will think the mock database is "corrupt"
   script_bytes = 'console.write.(blah blah)'.encode()
   m.scripts.find_one.return_value = { '_id': "abc123", 'sha256': 'd76a62e336adcdda817ad46287d08b616ccdbfd4e771b08cfeba864626433081', 'md5': 'f8f4392b9ce13de380ecdbe256030807', 'size_bytes': 25, 'code': script_bytes }
   m2 = Mock()
   m2.inserted_id = 'abc123ff'
   m.scripts.insert_one.return_value = m2

   # checksum corresponds to the rubbish code below...
   #[call.urls.insert_one({'url': 'X', 'last_visited': '2020-04-20', 'origin': 'foo.html'}),
   # call.scripts.find_one({'sha256': 'd76a62e336adcdda817ad46287d08b616ccdbfd4e771b08cfeba864626433081', 
   #                        'md5': 'f8f4392b9ce13de380ecdbe256030807', 'size_bytes': 25}),
   # call.scripts.insert_one({'sha256': 'd76a62e336adcdda817ad46287d08b616ccdbfd4e771b08cfeba864626433081', 
   #                          'md5': 'f8f4392b9ce13de380ecdbe256030807', 'size_bytes': 25, 'code': Binary(b'console.write.(blah blah)', 0)}),
   # call.script_url.insert_one({'url_id': <Mock name='mock.urls.insert_one().inserted_id' id='139913264531296'>, 'script': 'abc123ff'})]

   jsr = DownloadArtefact(url='X', path='full/foo.js', checksum='f8f4392b9ce13de380ecdbe256030807', host='pi1', origin='foo.html', when='2020-04-20')
   ret, js_id, was_cached = save_script(m, jsr, script_bytes)
   assert isinstance(ret, dict)
   assert js_id == "abc123"  # since it is "already in the database"
   assert 'sha256' in ret
   assert 'md5' in ret
   assert 'size_bytes' in ret 
   print(m.method_calls)
   assert len(m.method_calls) == 3 # db.urls.insert_one(), db.scripts.find_one(), db.script_url.insert_one()

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
