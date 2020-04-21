from unittest.mock import Mock
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
