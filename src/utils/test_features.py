#!/usr/bin/python3
import pytest
import mock
from utils.features import *
from bson.objectid import ObjectId
from datetime import datetime
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
   # test zero length vectors (needed as literal vectors may be empty)
   dist = compute_distance([], [], short_vector_penalty=True) 
   assert pytest.approx(dist, 0.0)

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

@pytest.fixture(scope="module")
def analyse_script_expected_results(pytestconfig):
   return load_data("{}/src/utils/fixtures/analyse_script_expected_results.json".format(pytestconfig.rootdir)) 

def test_analyse_script(pytestconfig, analyse_script_expected_results):
   testjs = "{}/src/test-javascript/banners.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
      jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256='XXX', md5='XXX')
      json, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
      assert not failed
      assert 'when' in json and isinstance(json['when'], str) and len(json['when']) > 0
      json.pop('when', None)
      assert json == analyse_script_expected_results

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

def load_data(filename):
   with open(filename, 'rb') as fp:
       return eval(fp.read().decode('utf-8'))

@pytest.fixture(scope="module")
def all_controls(pytestconfig):
   return load_data("{}/src/utils/fixtures/controls.json".format(pytestconfig.rootdir))

def test_find_best_control(pytestconfig, all_controls):
   # def find_best_control(input_features, controls_to_search, max_distance=100.0, db=None, debug=False)
   js_file = "{}/src/test-javascript/json2_4.9.2.min.js".format(pytestconfig.rootdir)
   with open(js_file, 'rb') as fp:
       content = fp.read()
       jsr = JavascriptArtefact(url=js_file, sha256=hashlib.sha256(content).hexdigest(),
                                md5=hashlib.md5(content).hexdigest(), size_bytes=len(content))
       input_features, failed, stderr = analyse_script(content, jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
       assert not failed
       best_control, next_best_control = find_best_control(input_features, all_controls, db=None) # db=None means no mongo hash match
# EXPECTED RESULTS:
#BestControl(control_url='https://cdn.jsdelivr.net/gh/WordPress/WordPress@5.2.5//wp-includes/js/json2.min.js', origin_url='/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/test-javascript/json2_4.9.2.min.js', sha256_matched=False, ast_dist=0.0, function_dist=0.0, diff_functions='', cited_on=None)
#BestControl(control_url='', origin_url='/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/test-javascript/json2_4.9.2.min.js', sha256_matched=False, ast_dist=inf, function_dist=inf, diff_functions='', cited_on=None)

       # NB: with only 1 control in the test, next_best must have infinite distances
       assert next_best_control.ast_dist == float('Inf')
       assert next_best_control.function_dist == float('Inf')
       assert best_control.ast_dist <= 0.0000001
       assert best_control.function_dist <= 0.000001
       assert pytest.approx(best_control.literal_dist, -1.0)
       assert pytest.approx(next_best_control.literal_dist, -1.0)

       c, c_ast_sum, c_ast_vector = all_controls[0]
       assert best_control.control_url == c['origin']
       assert best_control.sha256_matched == False     # due to no db specified

def test_find_script():
   db = mock.Mock()
   db.urls = mock.Mock()
   db.script_url = mock.Mock()
   db.scripts = mock.Mock()
   u = 'https://claritycommunications.com.au/wp-content/plugins/woocommerce/assets/js/jquery-blockui/jquery.blockUI.min.js?ver=2.70' 
   db.urls.find_one.return_value = {'_id': ObjectId('5e3c8f2b11c568eb4f2766de'), 'url': u, 'last_visited': datetime(2020, 2, 29, 5, 50, 25, 182000)}
   db.script_url.find_one.return_value =  {'_id': ObjectId('5e3c8f2b744c6d2310443a55'), 'url_id': ObjectId('5e3c8f2b11c568eb4f2766de'), 'script': ObjectId('5e38ff5ebef5d2ec18b15038')}
   db.scripts.find_one.return_value = ({ '_id': 'expected' }, { 'url_id': 'real' })

   ret = find_script(db, u, debug=True, want_code=False)
   assert ret == (db.scripts.find_one.return_value, db.urls.find_one.return_value)

