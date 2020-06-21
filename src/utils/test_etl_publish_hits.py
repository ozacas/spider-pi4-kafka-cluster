#!/usr/bin/python3
import pytest
import mock
from etl_publish_hits import process_hit
from utils.models import BestControl

def test_process_hit_success():
   db = mock.Mock()
   producer = mock.Mock()
   control_url = 'https://cdn.com/path/to/artefact.js' 
   bc = BestControl(control_url=control_url, 
                    origin_url='https://some.web.site/somewhere.js', 
                    cited_on='https://some.web.site/somewhere.html', origin_js_id='12345',
                    sha256_matched=True, diff_functions='', ast_dist=0.0, literal_dist=0.0, function_dist=0.0)
   all_controls = { control_url: { 'control_url': control_url, 'literals_by_count': { 'a': 1 } }}
   db.analysis_content.find_one.return_value = { 'analysis_bytes': b'{"calls_by_count":{}}' }
   process_hit(db, all_controls, bc, producer, 'distance_lt_200.0', stats={}) 
   #expected_find_call = mock.call.analysis_content.find_one({'js_id': '12345' })
   expected_insert_call = mock.call.etl_hits.insert_one({'control_url': 'https://cdn.com/path/to/artefact.js', 
                                                        'origin_url': 'https://some.web.site/somewhere.js', 'sha256_matched': True, 
                                                        'ast_dist': 0.0, 'function_dist': 0.0, 'cited_on': 'https://some.web.site/somewhere.html', 
                                                        'literal_dist': 0.0, 'xref': None, 'literals_not_in_control': -1, 
                                                        'literals_not_in_origin': -1, 'n_diff_literals': -1, 'diff_literals': '', 
                                                        'origin_host': 'some.web.site', 'origin_has_query': False, 'origin_port': 443, 
                                                        'origin_js_id': '12345',
                                                        'origin_scheme': 'https', 'origin_path': '/somewhere.js', 'cited_on_host': 'some.web.site', 
                                                        'cited_on_has_query': False, 'cited_on_port': 443, 'cited_on_scheme': 'https', 
                                                        'cited_on_path': '/somewhere.html', 'control_family': None, 'diff_functions': []})
   assert len(db.method_calls) == 1
   assert db.method_calls == [expected_insert_call]
   assert producer.method_calls == [mock.call.send('etl-good-hits', 
                                       {'control_url': 'https://cdn.com/path/to/artefact.js', 
                                        'origin_url': 'https://some.web.site/somewhere.js', 'sha256_matched': True, 
                                        'ast_dist': 0.0, 'function_dist': 0.0, 'cited_on': 'https://some.web.site/somewhere.html', 
                                        'origin_js_id': None, 'literal_dist': 0.0, 'xref': None, 'literals_not_in_control': -1, 
                                        'literals_not_in_origin': -1, 'n_diff_literals': -1, 'diff_literals': '', 
                                        'origin_js_id': '12345', 
                                        'origin_host': 'some.web.site', 'origin_has_query': False, 'origin_port': 443, 
                                        'origin_scheme': 'https', 'origin_path': '/somewhere.js', 'cited_on_host': 'some.web.site', 
                                        'cited_on_has_query': False, 'cited_on_port': 443, 'cited_on_scheme': 'https', 
                                        'cited_on_path': '/somewhere.html', 'control_family': None, 'diff_functions': []})]

