#!/usr/bin/python3
import pytest
from utils.models import Password, JavascriptArtefact, BestControl

def test_password_env(monkeypatch):
    monkeypatch.setenv("PASSWORD", "foobarbaz")
    result = str(Password(Password.DEFAULT))
    assert result == "foobarbaz"
    result = str(Password("foobaz")) 
    assert result == "foobaz"

def test_javascript_artefact():
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
    bc1 = BestControl(origin_url='XXX', control_url='YYY', literal_dist=0.0, ast_dist=1.0, function_dist=1.5, sha256_matched=False, diff_functions='parseJSON runAJAX')
    assert bc1.origin_url == 'XXX'
    assert bc1.control_url == 'YYY'
    assert bc1.ast_dist == pytest.approx(1.0)
    assert bc1.function_dist == pytest.approx(1.5)
    assert not bc1.sha256_matched
    assert bc1.diff_functions == 'parseJSON runAJAX' 
    assert bc1.origin_js_id is None
    assert bc1.cited_on is None
    assert pytest.approx(bc1.literal_dist, -1.0)
    assert bc1.is_good_hit()

    bc2 = BestControl(origin_url='AAA', control_url='BBB', ast_dist=10.0, function_dist=1.5, literal_dist=0.0, sha256_matched=False, diff_functions='a b c')
    assert bc1.is_better(bc2)
    assert not bc2.is_better(bc1)
    assert bc1.distance() < bc2.distance() and pytest.approx(bc1.distance(), 1.5)

    bc3 = BestControl(origin_url='AAA', control_url='BBB', ast_dist=43.0, function_dist=3.0, literal_dist=9.7, sha256_matched=False, diff_functions='aaa bbb')
    assert not bc3.is_good_hit()

    bc2.literal_dist = -1
    assert not bc2.good_hit_as_tuple() == (False, 'bad_literal_dist')

def test_best_control_infinity_handling():
    bc1 = BestControl(literal_dist=float('Inf'), ast_dist=float('Inf'), function_dist=float('Inf'), 
                      control_url='', origin_url='', sha256_matched=False, diff_functions='')
    bc2 = BestControl(literal_dist=0.0, ast_dist=0.0, function_dist=0.0,
                      control_url='XXX', origin_url='YYY', sha256_matched=True, diff_functions='')

    assert bc2.is_better(bc1)
    assert not bc1.is_better(bc2)

def test_best_control_max_distance():
    bc2 = BestControl(literal_dist=7.0, ast_dist=12.0, function_dist=3.0, diff_functions='                  ', # need 12 to disable original criteria
                      control_url='XXX', origin_url='YYY', sha256_matched=True)
    ok, reason = bc2.good_hit_as_tuple(max_distance=100.0) # since distprod is (12+3) * (3+7) == 150
    assert ok and reason == 'good_two_smallest_distances' # must fail distance() test
    assert bc2.good_hit_as_tuple(max_distance=200.0) == (True, 'dist_lt_200.0')

    bc2.function_dist=22
    assert not bc2.is_good_hit()
