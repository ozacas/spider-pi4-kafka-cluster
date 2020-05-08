import subprocess
import json
import os
import math 
import hashlib
from dataclasses import asdict
from utils.models import BestControl
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

# comes from MongoDB (db.statements_by_count_keys collection) but for performance is listed here
ast_feature_list =  [ "ArrayLiteral", "Assignment", "AstRoot", "Block", "BreakStatement", "CatchClause", "ConditionalExpression",
        "ContinueStatement", "DoLoop", "ElementGet", "EmptyExpression", "EmptyStatement", "ExpressionStatement", "ForInLoop", "ForLoop",
        "FunctionCall", "FunctionNode", "IfStatement", "InfixExpression", "KeywordLiteral", "Label", "LabeledStatement", "Name",
        "NewExpression", "NumberLiteral", "ObjectLiteral", "ObjectProperty", "ParenthesizedExpression", "PropertyGet",
        "RegExpLiteral", "ReturnStatement", "Scope", "StringLiteral", "SwitchCase", "SwitchStatement", "ThrowStatement",
        "TryStatement", "UnaryExpression", "VariableDeclaration", "VariableInitializer", "WhileLoop", "WithStatement", "XmlLiteral", "XmlString" ]


def safe_for_mongo(items):
   """ 
   Returns a dict which has all keys made safe for insertion into MongoDB.
   This primarily means fields with a key starting with '$' are replaced with 'F$'
   """
   d = {}
   for k, v in items.items():
       if k.startswith('$') or k == '_id':
           k = "F"+k
       d[k] = v
   return d

def as_url_fields(url, prefix=''):
    """
    Returns a dict with keys prefixed by prefix representing component parts and attributes of the chosen url. 
    urlparse is used for parsing the url.
    """
    up = urlparse(url)
    d = {}
    if len(prefix) > 0:
        prefix = "{}_".format(prefix)
    d[prefix+'host'] = up.hostname
    d[prefix+'has_query'] = len(up.query) > 0
    if up.port:
        d[prefix+'port'] = up.port
    elif up.scheme == 'https':
        d[prefix+'port'] = 443
    elif up.scheme == 'http':
        d[prefix+'port'] = 80
    d[prefix+'scheme'] = up.scheme
    d[prefix+'path'] = up.path
    return d

def find_script(db, url, want_code=True):
   if db:
       # 1 lookup url
       url_id = db.urls.find_one({ 'url': url })
       if url_id:
            # 2. lookup script_url to find the script_id (TODO FIXME: control which spider'ed version is retrieved from the DB????)
            want_code_settings = {} if want_code else { 'code': 0 }
            ret = db.script_url.find_one({ 'url_id': url_id.get('_id') }, want_code_settings)
            if ret:
               # 3. ok, now we can get the script document to return the hash
               return (db.scripts.find_one({ '_id': ret.get('script') }), url_id)
   return (None, None)
 
def find_sha256_hash(db, url):
   """
   Similar to find_script(), but returns only the sha256 hexdigest (if found) 
   """
   script, url_id = find_script(db, url, want_code=False)
   if script:
       return (script.get('sha256'), url_id)
   return (None, None)

def get_script(db, artefact):
   # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
   d = { 'sha256': artefact.sha256.strip(), 'md5': artefact.md5.strip(), 'size_bytes': artefact.size_bytes } # ENSURE we set the right fields so Mongo uses the index
   collection = db.snippets if artefact.inline else db.scripts
   js = collection.find_one(d)
   return (js.get(u'code'), str(js.get('_id'))) if js else (None, None)

def analyse_script(js, jsr, java='/usr/bin/java', feature_extractor="/home/acas/src/extract-features.jar"):
   # save code to a file
   tmpfile = NamedTemporaryFile(delete=False)
   tmpfile.write(js)
   tmpfile.close()

   url = jsr.url
   # save to file and run extract-features.jar to identify the javascript features
   process = subprocess.run([java, "-jar", feature_extractor, tmpfile.name, url], capture_output=True)
   
   # turn process stdout into something we can save
   ret = None
   d = asdict(jsr)
   if process.returncode == 0:
       ret = json.loads(process.stdout)
       ret.update(d)       # will contain url key and NOW origin html page also (missing in early data)
       ret.pop('id', None) # remove duplicate url entry silently
       call_vector = safe_for_mongo(ret.pop('calls_by_count')) # NB: for correct analysis both the AU JS and control vectors must both be safe-for-mongo'ed consistently
       #print(call_vector)
       ret['calls_by_count'] = call_vector
       tmp = ret.pop('literals_by_count')
       literal_vector = safe_for_mongo({ k[0:200]: v  for k,v in tmp.items() }) # HACK TODO FIXME: we assume that the first 200 chars per literal is unique
       ret['literals_by_count'] = literal_vector

       # FALLTHRU

   # cleanup
   os.unlink(tmpfile.name)

   return (ret, process.returncode != 0, process.stderr)  # JSON (iff successful else None), failed (boolean), stderr capture

def compute_distance(v1, v2, short_vector_penalty=True):
   svp = 1.0
   if short_vector_penalty and len(v1) < 15:
       svp = 20.0 / len(v1)
   return math.dist(v1, v2) * svp

def calculate_ast_vector(d):
   global ast_feature_list
   return calculate_vector(d, feature_names=ast_feature_list)

def calculate_vector(features, feature_names=None):
   if feature_names is None:
       raise ValueError("must supply a list of vector feature names (set semantics)")

   ret = []
   sum = 0
   for f in feature_names:
       v = features.get(f, 0)
       sum += v
       ret.append(v)  # vector must always have the same length with the keys in a consistent order for comparison

   return (ret, sum)

def find_hash_match(db, input_features, control_url):
   if db:
       rec = db.javascript_controls.find_one({ 'origin': control_url })
       if rec is None:
           return False
       expected_sha256 = rec.get('sha256', None)    
       actual_sha256 = input_features.get('sha256', None)
       if actual_sha256 is None:
           # lookup db if sha256 is not in kafka message? nah, not for now... TODO FIXME
           url = input_features.get('id', input_features.get('url')) # url field was named 'id' field in legacy records
           actual_sha256, url_id = find_sha256_hash(db, url)
           #print(actual_sha256)
           # FALLTHRU 
       ret = expected_sha256 == actual_sha256
       return ret
   return False

def calc_function_dist(origin_calls, control_calls):
   # function distance is a little different: all function calls are used for distance, even if only present in one vector 
   o_calls = set(origin_calls.keys())
   c_calls = set(control_calls.keys())
   fns = c_calls.union(o_calls)
   vec1 = []
   vec2 = []
   #print("*** origin_calls {}".format(origin_calls))
   #print("*** control_calls {}".format(control_calls))
   diff_functions = []
   common = 0
   for key in fns:
       if key == 'N/A':  # ignore this for calculation for distance
          continue
       a = origin_calls.get(key, 0)
       b = control_calls.get(key, 0)
       vec1.append(a)
       vec2.append(b) 
       if a != b:
           diff_functions.append(key)
       else:
           common += 1 
   commonality_factor = 1 / common if common > 0 else 10 
   return (math.dist(vec1, vec2) * commonality_factor, diff_functions)

def find_feasible_controls(desired_sum, controls_to_search, max_distance=100.0):
   if desired_sum < 50: # vector too small for meaningful comparison? In other words, no feasible controls
       return []

   # only those controls within max_distance (either side) from the desired feature sum (from the AST vector under test) are considered feasible
   # all others need not be searched. This could be tighter.
   lb = desired_sum - max_distance
   ub = desired_sum + max_distance
   feasible_controls = [tuple for tuple in filter(lambda t: t[1] >= lb and t[1] <= ub, controls_to_search)]
   return feasible_controls

def find_best_control(input_features, controls_to_search, max_distance=100.0, db=None, debug=False):
   second_best_control = None
   best_distance = float('Inf')
   origin_url = input_features.get('url', input_features.get('id')) # LEGACY: url field used to be named id field
   cited_on = input_features.get('origin', None) # report owning HTML page also if possible (useful for data analysis)
   origin_js_id=input_features.get("js_id", None), # ensure we can find the script directly without URL lookup
   if isinstance(origin_js_id, tuple) or isinstance(origin_js_id, list): # BUG FIXME: should not be a tuple but is... where is that coming from??? so...
       origin_js_id = origin_js_id[0]
   assert isinstance(origin_js_id, str) or origin_js_id is None # check POST-CONDITION
   hash_match = False
   input_vector, total_sum = calculate_ast_vector(input_features['statements_by_count'])
   best_control = BestControl(control_url='', origin_url=origin_url, cited_on=cited_on,
                              sha256_matched=False, 
                              ast_dist=float('Inf'), function_dist=float('Inf'), diff_functions='',
                              origin_js_id=origin_js_id)
   control_function_calls = None

   feasible_controls = find_feasible_controls(total_sum, controls_to_search, max_distance=max_distance)
   for fc_tuple in feasible_controls:
       control, control_ast_sum, control_ast_vector = fc_tuple
       assert isinstance(control, dict)
       assert control_ast_sum > 0
       assert isinstance(control_ast_vector, list)
       control_url = control.get('origin')

       dist = math.dist(input_vector, control_ast_vector)
       if dist < best_distance and dist <= max_distance: # TODO FIXME: consider more than AST distance before determining "best" ???
           if debug:
               print("Got good distance {} for {}".format(dist, control_url)) 
           # compute what we can for now and if we can update it later we will. Otherwise the second_best control may have some fields not-computed
           call_dist, diff_functions = calc_function_dist(input_features['calls_by_count'], 
                                                          control['calls_by_count'])
           new_control = BestControl(control_url=control_url, # control artefact from CDN (ground truth)
                                      origin_url=origin_url, # JS at spidered site 
                                      origin_js_id=origin_js_id,
                                      cited_on=cited_on,   # HTML page that cited the origin JS
                                      sha256_matched=False, 
                                      ast_dist=dist, 
                                      function_dist=call_dist, 
                                      diff_functions=' '.join(diff_functions))
           # NB: look at product of two distances before deciding to update best_* - hopefully this results in a lower false positive rate 
           #     (with accidental ast hits) as the number of controls in the database increases
           prod = (dist * call_dist)
           if prod < 50.0 and prod > (best_control.ast_dist * best_control.function_dist):
               if second_best_control.ast_dist * second_best_control.function_dist > prod:
                   print("NOTE: improved second_best control")
                   second_best_control = new_control
               # NB: dont update best_* since we dont consider this hit a replacement for current best_control
           else: 
               second_best_control = best_control
               best_control = new_control
               best_distance = dist

               if dist < 0.00001:     # small distance means we can try for a hash match against control?
                   hash_match = find_hash_match(db, input_features, best_control.control_url)
                   best_control.sha256_matched = hash_match
                   break    # save time since we've likely found the best control
 
   return (best_control, second_best_control)
