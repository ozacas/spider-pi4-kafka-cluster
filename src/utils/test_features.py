#!/usr/bin/python3
import pytest
from utils.features import *
from utils.models import JavascriptArtefact, JavascriptVectorSummary

def test_safe_for_mongo():
   assert safe_for_mongo({ 'a': 0, '$': 1 }) == { 'a': 0, 'F$': 1 }
   assert safe_for_mongo({ '_id': 7 }) == { 'F_id': 7 }

def test_as_url_fields():
   assert as_url_fields('https://www.google.com') == { 'has_query': False, 'host': 'www.google.com', 'path': '', 'port': 443,  'scheme': 'https' }
   assert as_url_fields('https://www.google.com/map', prefix='g') == { 'g_has_query': False, 'g_host': 'www.google.com', 'g_path': '/map', 'g_port': 443, 'g_scheme': 'https' }

def test_compute_distance():
   assert compute_distance([1.0, 3.0, 9.0], [1.0, 3.0, 9.0]) < 0.0000001
   assert compute_distance([1.0, 3.0], [1.0, 3.0], short_vector_penalty=True) < 0.0001
   assert compute_distance([112, 33], [99, 12]) >= 246.98
   dist = compute_distance([112, 33], [99, 12], short_vector_penalty=False)
   assert dist >= 24.6981 and dist <= 24.6982

def test_compute_function_dist():
   a = { "foo": 3, "bar": 7, "baz": 1 }
   b = { "crud": 20, "foo": 3, "bar": 7 }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist >= 10.01 and dist <= 10.02 
   assert sorted(diff_functions) == ['baz', 'crud']
   a = { "tmp": 1 }
   b = { "tmp": 2 }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist > 0.0
   assert diff_functions == ['tmp']

def test_compute_ast_vector():
   d = { "ArrayLiteral": 10, "Assignment": 7, "AstRoot": 1 }
   tuple = calculate_ast_vector(d) 
   assert tuple == ([10, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 18) 
   assert compute_distance(tuple[0], tuple[0]) <= 0.001

def test_analyse_script(pytestconfig):
   testjs = "{}/src/test-javascript/banners.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
      jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256='XXX', md5='XXX')
      json, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
      assert not failed
      assert 'when' in json and isinstance(json['when'], str) and len(json['when']) > 0
      json.pop('when', None)
      assert json == {'statements_by_count': {'StringLiteral': 32, 'VariableInitializer': 1, 'KeywordLiteral': 2, 'AstRoot': 1, 'ObjectLiteral': 2, 'ObjectProperty': 12, 'Name': 14, 'NumberLiteral': 5, 'ArrayLiteral': 9, 'VariableDeclaration': 1, 'NewExpression': 1}, 
'url': 'file:/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/test-javascript/banners.js', 
'sha256': 'XXX', 
'md5': 'XXX', 
'inline': False, 
'content_type': 'text/javascript', 
'size_bytes': 0, 
'origin': None, 
'calls_by_count': {}, 
'literals_by_count': {'0': 1, '220': 1, '//www.spec-net.com.au/banners/200129_Rollashield.jpg': 1, 'https://www.rollashieldshutters.com.au/bushfire-shutters/': 1, '470': 1, '500': 1, '//www.spec-net.com.au/banners/200129_GCP-Applied-Technologies.jpg': 1, '5000': 1, 'fadeshow1': 1, 'https://gcpat.com.au/en-gb/news/blog': 1, '//www.spec-net.com.au/banners/200129_RMS.jpg': 1, '//www.spec-net.com.au/banners/200129_Tornex-Door-Systems.jpg': 1, 'auto': 1, 'https://www.spec-net.com.au/press/0120/dpp_290120/Elegant-Bathroom-Radiators-AGAVE-by-dPP-Hydronic-Heating': 1, 'https://www.rmsmarble.com/vetrazzo-slabs/': 1, '//www.spec-net.com.au/banners/200129_dPP-Hydronic-Heating.jpg': 1, 'Elegant Bathroom Radiators - AGAVE by dPP Hydronic Heating': 1, 'GCP Preprufe Plus Seamless Protection of Underground Structures': 1, 'A Gorgeous Staircase Featuring AWIS Wrought Iron Components': 1, 'Electrical Operated, Triple Glazed Roof Hatches by Gorter Hatches': 1, 'https://www.gortergroup.com/au/products/roof-hatches/rhtg-glazed.html': 1, '//www.spec-net.com.au/banners/200129_AWIS.jpg': 1, 'Handcrafted Recycled Glass Benchtops - Vetrazzo by RMS': 1, 'Single & Double Sliding Track Doors from Tornex Door Systems': 1, '//www.spec-net.com.au/banners/200205_Gorter-Hatches.jpg': 1, 'always': 1, 'https://www.tornex.com.au/': 1, 'AS3959-2009 Compliant BAL FZ Bushfire Shutters from Rollashield': 1, 'http://artisticwroughtiron.com.au/': 1}} 

def test_analyse_script_2(pytestconfig):
   testjs = "{}/src/test-javascript/fieldRequiredWhenNotAfterGoLiveValidation.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
       jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256="XXX", md5="XXX")
       json, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
       assert not failed

       assert json['statements_by_count'] == {"FunctionNode":2,"StringLiteral":13,"VariableInitializer":3,"Scope":1,"KeywordLiteral":3,"AstRoot":1,"Assignment":2,"IfStatement":1,"Block":2,"InfixExpression":10,"ExpressionStatement":4,"PropertyGet":14,"ReturnStatement":2,"UnaryExpression":1,"Name":37,"NumberLiteral":2,"ArrayLiteral":1,"VariableDeclaration":3,"FunctionCall":9,"ElementGet":2,"ParenthesizedExpression":3}
       assert json['calls_by_count'] == {"val":1,"F$":3,"addMethod":1,"get":1,"attr":1,"split":1,"add":1}
       assert json['literals_by_count'] == {" ":1,"0":2,"#IsAfterGoLive":1,"INPUT":1,"requiredwhennotaftergolivevalidation":1,"True":1,"class":1,"testrequiredwhennotaftergolivevalidation":3,"SELECT":1}

def test_analyse_script_failure(pytestconfig):
   # mozilla rhino cant handle all JS... so check that failure path is as expected
   testjs = "{}/src/test-javascript/google-analytics.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
      jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256="XXX", md5="XXX")
      json, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
      assert failed
      assert "missing ; after for-loop initializer" in stderr.decode('utf-8')

def test_find_feasible_controls():
   #def find_feasible_controls(desired_sum, controls_to_search, max_distance=100.0)
   all_controls = [ ({'origin': 'good' }, 100, [1, 2, 3, 94]), ({ 'origin': 'also good' }, 90, [90]) ]
   ret = list(find_feasible_controls(100, all_controls))
   assert len(ret) == 2

def test_find_best_control(pytestconfig):
   all_controls = [({'_id': '5e8919aece9013c5bcde5338', 'family': 'WordPress/WordPress', 'origin': 'https://cdn.jsdelivr.net/gh/WordPress/WordPress@5.2.5//wp-includes/js/json2.min.js', 'calls_by_count': {'f': 5, 'charCodeAt': 2, 'toString': 2, 'test': 3, 'getUTCMinutes': 1, 'apply': 1, 'valueOf': 2, 'getUTCDate': 1, 'walk': 2, 'String': 3, 'getUTCMonth': 1, 'join': 4, 'isFinite': 2, 'getUTCFullYear': 1, 'push': 2, 'eval': 1, 'toJSON': 1, 'replace': 5, 'quote': 3, 'call': 4, 'getUTCSeconds': 1, 'slice': 2, 'str': 4, 'getUTCHours': 1}, 'content_type': 'text/javascript', 'inline': False, 'md5': '35d899a81986173f44f9bbe686cc583c', 'provider': 'jsdelivr', 'release': '5.2.5', 'sha256': '6c16b51a66747d60a59bf985bbb77f40922eabb1d7401d1564da78ec025e65e5', 'size_bytes': 3133, 'statements_by_count': {'FunctionNode': 11, 'StringLiteral': 90, 'VariableInitializer': 23, 'Scope': 1, 'KeywordLiteral': 9, 'AstRoot': 1, 'RegExpLiteral': 6, 'Assignment': 40, 'IfStatement': 7, 'ConditionalExpression': 13, 'ThrowStatement': 2, 'Block': 11, 'SwitchStatement': 1, 'ObjectLiteral': 4, 'ObjectProperty': 9, 'InfixExpression': 117, 'ExpressionStatement': 10, 'PropertyGet': 62, 'ReturnStatement': 15, 'ForLoop': 3, 'SwitchCase': 5, 'UnaryExpression': 23, 'ForInLoop': 2, 'Name': 305, 'NumberLiteral': 19, 'ArrayLiteral': 1, 'VariableDeclaration': 7, 'NewExpression': 2, 'FunctionCall': 55, 'ElementGet': 8, 'ParenthesizedExpression': 16}, 'url': 'https://cdn.jsdelivr.net/gh/WordPress/WordPress@5.2.5//wp-includes/js/json2.min.js', 'variant': '5.2.5', 'when': '2020-04-20 11:27:28.952014'}, 878, [1, 40, 1, 11, 0, 0, 13, 0, 0, 8, 0, 0, 10, 2, 3, 55, 11, 7, 117, 9, 0, 0, 305, 2, 19, 4, 9, 16, 62, 6, 15, 1, 90, 5, 1, 2, 0, 23, 7, 23, 0, 0, 0, 0])]
   # def find_best_control(input_features, controls_to_search, max_distance=100.0, db=None, debug=False)
   js_file = "{}/src/test-javascript/json2_4.9.2.min.js".format(pytestconfig.rootdir)
   with open(js_file, 'rb') as fp:
       content = fp.read()
       jsr = JavascriptArtefact(url=js_file, sha256=hashlib.sha256(content).hexdigest(),
                                md5=hashlib.md5(content).hexdigest(), size_bytes=len(content))
       input_features, failed, stderr = analyse_script(content, jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
       assert not failed
       best_control, next_best_control = find_best_control(input_features, all_controls, db=None) # db=None means no mongo hash match
       # NB: with only 1 control in the test, next_best must be None
       assert next_best_control.ast_dist == float('Inf')
       assert best_control.ast_dist <= 0.0000001
       c, c_ast_sum, c_ast_vector = all_controls[0]
       assert best_control.control_url == c['origin']
       assert best_control.sha256_matched == False     # due to no db specified
