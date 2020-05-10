#!/usr/bin/python3
import pytest
from utils.models import Password, JavascriptArtefact, BestControl

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

def test_best_control():
    bc1 = BestControl(origin_url='XXX', control_url='YYY', ast_dist=1.0, function_dist=1.5, sha256_matched=False, diff_functions='parseJSON runAJAX')
    assert bc1.origin_url == 'XXX'
    assert bc1.control_url == 'YYY'
    assert bc1.ast_dist == pytest.approx(1.0)
    assert bc1.function_dist == pytest.approx(1.5)
    assert not bc1.sha256_matched
    assert bc1.diff_functions == 'parseJSON runAJAX' 
    assert bc1.origin_js_id is None
    assert bc1.cited_on is None

    bc2 = BestControl(origin_url='AAA', control_url='BBB', ast_dist=10.0, function_dist=1.5, sha256_matched=False, diff_functions='a b c')
    assert bc1.is_better(bc2)
    assert not bc2.is_better(bc1)
    assert bc1.dist_prod() < bc2.dist_prod() and pytest.approx(bc1.dist_prod(), 1.5)
