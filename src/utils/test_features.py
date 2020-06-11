#!/usr/bin/python3
import pytest
import mock
import hashlib
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
   assert dist == pytest.approx(dist, 20.025)
   assert sorted(diff_functions) == ['baz', 'crud']
   a = { "tmp": 1 }
   b = { "tmp": 2 }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist > 0.0
   assert diff_functions == ['tmp']

def test_compute_function_dist_3():
   # enforce symettry and correct anonymous function calculation
   a = { "N/A": 1 }
   b = { }
   dist1, diff_fn1 = calc_function_dist(a, b)
   assert pytest.approx(dist1, 10.0) # since the short penalty max'es out at ten
   assert diff_fn1 == ['N/A']
   dist2, diff_fn2 = calc_function_dist(b, a)
   assert dist2 > 0.0
   assert diff_fn2 == ['N/A']
   assert pytest.approx(dist1, dist2) # must be the same distance for (b, a) as (a, b)

   # empty vectors are infinitely distant - since no available evidence
   assert calc_function_dist({}, {}) == (float('Inf'), [])

def test_compute_function_dist_2():
   # real world example
   a = { k: v for k,v in zip([i for i in 'abcdefghijklmnop'], [0, 1, 0, 0, 1, 1, 1, 2, 0, 1, 0, 0, 0, 1, 4]) }
   b = { k: v for k,v in zip([i for i in 'abcdefghijklmnop'], [1, 0, 1, 1, 1, 0, 0, 2, 1, 0, 1, 2, 3, 0, 4]) }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist == pytest.approx(19.18332)
   assert set(diff_functions) == set(['n', 'd', 'j', 'c', 'm', 'l', 'i', 'b', 'k', 'g', 'f', 'a'])

def test_calculate_literal_dist():
   hit = mock.Mock()
   hit.control_url = 'http://XXX' 
   db = mock.Mock()
   hit = { 'literals_by_count': { 'a': 1, 'b': 2 } }
   ret = calculate_literal_distance(hit['literals_by_count'], { 'a': 2, 'b': 2 })
   assert isinstance(ret, tuple)
   assert ret[0] == pytest.approx(10.0)
   assert ret[1] == 0
   assert ret[2] == 0
   assert ret[3] == ['a']

def test_compute_ast_vector():
   d = { "ArrayLiteral": 10, "Assignment": 7, "AstRoot": 1 }
   tuple = calculate_ast_vector(d) 
   assert tuple == ([10, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 18) 
   assert compute_distance(tuple[0], tuple[0]) <= 0.001

@pytest.fixture(scope="module")
def analyse_script_expected_results(pytestconfig):
   return load_data("{}/src/utils/fixtures/analyse_script_expected_results.json".format(pytestconfig.rootdir)) 

@pytest.fixture(scope="module")
def analyse_utf8_expected_results(pytestconfig):
   return load_data("{}/src/utils/fixtures/analyse_script_utf8_results.json".format(pytestconfig.rootdir))

def test_analyse_script(pytestconfig, analyse_script_expected_results):
   testjs = "{}/src/test-javascript/banners.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
      jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256='XXX', md5='XXX')
      byte_content, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
      assert not failed
      js = json.loads(byte_content.decode())
      js.pop('id', None) # not interested in this field anymore -- obsolete
      assert js == analyse_script_expected_results

def test_analyse_script_2(pytestconfig):
   testjs = "{}/src/test-javascript/fieldRequiredWhenNotAfterGoLiveValidation.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
       jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256="XXX", md5="XXX")
       byte_content, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
       assert not failed
       js = json.loads(byte_content.decode())
       assert js['statements_by_count'] == {"FunctionNode":2,"StringLiteral":13,"VariableInitializer":3,"Scope":1,"KeywordLiteral":3,"AstRoot":1,"Assignment":2,"IfStatement":1,"Block":2,"InfixExpression":10,"ExpressionStatement":4,"PropertyGet":14,"ReturnStatement":2,"UnaryExpression":1,"Name":37,"NumberLiteral":2,"ArrayLiteral":1,"VariableDeclaration":3,"FunctionCall":9,"ElementGet":2,"ParenthesizedExpression":3}
       assert js['calls_by_count'] == {"val":1,"F$":3,"addMethod":1,"get":1,"attr":1,"split":1,"add":1}
       assert js['literals_by_count'] == {" ":1,"0":2,"#IsAfterGoLive":1,"INPUT":1,"requiredwhennotaftergolivevalidation":1,"True":1,"class":1,"testrequiredwhennotaftergolivevalidation":3,"SELECT":1}

def test_analyse_script_utf8_handling(pytestconfig, analyse_utf8_expected_results):
   testjs = "{}/src/test-javascript/ca.js".format(pytestconfig.rootdir)
   with open(testjs, 'rb') as fp:
       jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256="XXX", md5="XXX")
       byte_content, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
       assert not failed
       js = json.loads(byte_content.decode())
       # must match results computed by java on the CLI...
       v1 = truncate_literals(js['literals_by_count'])
       v2 = truncate_literals(analyse_utf8_expected_results['literals_by_count'])
       assert len(v1) == len(v2)
       assert v1 == v2
       # and that calculate_literal_ distance() is also zero (ie. unicode problems are not screwing up)
       dist, n_not_in_origin, n_not_in_control, diff_lits = calculate_literal_distance(v1, v2, fail_if_difference=True)
       assert pytest.approx(dist, 0.0)
       assert n_not_in_origin == 0
       assert n_not_in_control == 0
       assert diff_lits == []

def test_analyse_script_failure(pytestconfig):
   # mozilla rhino cant handle all JS... so check that failure path is as expected
   testjs = "{}/src/test-javascript/google-analytics.js".format(pytestconfig.rootdir)
   with open(testjs, "rb") as fp:
      jsr = JavascriptArtefact(url="file:{}".format(testjs), origin=None, sha256="XXX", md5="XXX")
      json, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor="{}/src/extract-features.jar".format(pytestconfig.rootdir))
      assert failed
      assert "missing ; after for-loop initializer" in stderr

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
       db = mock.Mock()
       db.javascript_controls.find_one.return_value =  { 'literals_by_count': { 'blah': 0, 'blah': 0 } }
       d = json.loads(input_features.decode())
       d['js_id'] = 'XXXXXXXXXXXXXXXXXXXXXXXX'
       d['sha256'] = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
       d['byte_content_sha256'] = hashlib.sha256(input_features).hexdigest()
       best_control, next_best_control = find_best_control(d, all_controls, db=db, debug=True) 
# EXPECTED RESULTS:
# best_control = BestControl(control_url='https://cdn.jsdelivr.net/gh/WordPress/WordPress@5.2.5//wp-includes/js/json2.min.js', origin_url='/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/test-javascript/json2_4.9.2.min.js', sha256_matched=False, ast_dist=0.0, function_dist=0.0, diff_functions='', cited_on=None)
# next_best_control has a larger ast_dist due to a different version, but it has the same function calls so the dist is zero
       print(best_control)
       print(next_best_control)
       # NB: with only 1 control in the test, next_best must have infinite distances
       assert next_best_control.ast_dist == pytest.approx(7.54983)
       assert next_best_control.function_dist == pytest.approx(0.0)
       assert best_control.ast_dist <= 0.0000001
       assert best_control.function_dist <= 0.000001
       assert pytest.approx(best_control.literal_dist, -1.0)
       assert pytest.approx(next_best_control.literal_dist, -1.0)


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

def test_identify_subfamily():
   u = 'https://cdnjs.cloudflare.com/ajax/libs/jquery.payment/1.0.0/jquery.payment.min.js'
   u2 = 'https://cdnjs.cloudflare.com/ajax/libs/flexslider/2.7.2/jquery.flexslider-min.js'
   assert identify_control_subfamily(u) == (u, 'jquery-payment')
   assert identify_control_subfamily(u2) == (u2, 'jquery-flexslider')
   with pytest.raises(AssertionError):
       identify_control_subfamily(None)
