#!/usr/bin/python3
import pytest
from utils.models import Password, JavascriptArtefact

def test_password_env(monkeypatch):
    monkeypatch.setenv("PASSWORD", "foobarbaz")
    result = str(Password(Password.DEFAULT))
    assert result == "foobarbaz"
    result = str(Password("foobaz")) 
    assert result == "foobaz"

def test_artefact_model():
    # usual constructor parameters for most apps
    jsr1 = JavascriptArtefact(sha256="XYZ", md5="XXX", size_bytes=279, url="http://foo/bar/baz")
    assert jsr1.sha256 == "XYZ"
    assert jsr1.md5 == "XXX"
    assert jsr1.size_bytes == 279
    assert jsr1.url == "http://foo/bar/baz" 
    assert jsr1.origin is None # if origin is not specified it must be None
    assert jsr1.inline == False
    assert jsr1.content_type == "text/javascript"
    assert isinstance(jsr1.when, str) and len(jsr1.when) > 10

    # but etl_upload requires compatibility with 'checksum' instead of md5
    jsr2 = JavascriptArtefact(sha256="JJJ", checksum="XXX", size_bytes=122999, url=None, origin="http://bar/baz")
    assert jsr2.md5 == "XXX"
    assert jsr2.size_bytes == 122999
    assert jsr2.origin == "http://bar/baz"

    # and that sorting is correctly done via the md5 field
    j3 = JavascriptArtefact(md5='333', url=None, sha256='') 
    j9 = JavascriptArtefact(md5='999', url=None, sha256='')
    j1 = JavascriptArtefact(md5='111', url=None, sha256='') 
    l = [j3, j9, j1]
    assert sorted(l) == [j1, j3, j9]
