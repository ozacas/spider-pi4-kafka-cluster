#!/usr/bin/python3
import pytest
import mock
from utils.models import BestControl
from etl_publish_poor_hits import iterate

def test_iterate():
   # check that key iterator works as advertised 
   # eg. {
   #   "control_url": "https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.8.13/jquery-ui.min.js",
   #   "origin_url": "https://ajax.googleapis.com/ajax/libs/jqueryui/1.8.12/jquery-ui.min.js",
   #   "sha256_matched": false,
   #   "ast_dist": 54.7265931700485,
   #   "function_dist": 1.7048137729212494,
   #   "diff_functions": "x p attr map splice b mouseover mousedown appendTo ...",
   #   "cited_on": "https://www.linc.tas.gov.au/family-history/Pages/default.aspx" }
   m1 = mock.Mock()
   m1.value = { 'ast_dist': 49.9, 'sha256_matched': False, 'control_url': 'XXX', 'origin_url': 'YYY', 
                'function_dist': 0.01, 'diff_functions': 'a b c', 'cited_on': '1' }
   m2 = mock.Mock()
   m2.value = { 'ast_dist': 50.01, 'sha256_matched': False, 'control_url': 'rubbish', 'origin_url': 'crap', 
                'function_dist': 0.01, 'diff_functions': 'd e f', 'cited_on': '2' }
   consumer = [ m1, m2 ] 
   ret = list(iterate(consumer, 2, False, 50.0))  # must proceed without exception
   assert len(ret) == 1
   assert isinstance(ret[0], BestControl)
   assert ret[0].origin_js_id is None
   assert ret[0].ast_dist > 50.0
   assert not ret[0].sha256_matched
   assert ret[0].control_url == 'rubbish'
   assert ret[0].origin_url == 'crap'
   assert ret[0].function_dist < 0.02
   assert ret[0].diff_functions == 'd e f'
