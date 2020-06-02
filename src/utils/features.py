import subprocess
import json
import os
import re
import math 
import hashlib
import pylru
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

def find_script(db, url, want_code=True, debug=False):
   # TODO FIXME BUG: which version of url do we want to find... poor atm!
   if db:
       # 1 lookup url
       url_id = db.urls.find_one({ 'url': url })
       if debug:
           print(url_id)
       if url_id:
            # 2. lookup script_url to find the script_id (TODO FIXME: control which spider'ed version is retrieved from the DB????)
            ret = db.script_url.find_one({ 'url_id': url_id.get('_id') })
            if debug:
                print(ret)
            if ret:
               # 3. ok, now we can get the script document to return the hash
               script_doc = db.scripts.find_one({ '_id': ret.get('script') }, { 'code': 1 if want_code else 0 }) 
               if debug:
                   print(script_doc)
               return (script_doc, url_id)
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
   return (js.get(u'code'), str(js.get('_id'))) if js is not None else (None, None)

def analyse_script(js, jsr, java='/usr/bin/java', feature_extractor="/home/acas/src/extract-features.jar"):
   if isinstance(js, bytes):
       # save code to a file
       tmpfile = NamedTemporaryFile(delete=False)
       tmpfile.write(js)
       tmpfile.close()
       fname = tmpfile.name
   else:
       fname = js
       tmpfile = None

   url = jsr.url
   # save to file and run extract-features.jar to identify the javascript features
   process = subprocess.run([java, "-jar", feature_extractor, fname, url], capture_output=True)
   
   # turn process stdout into something we can save
   ret = None
   d = asdict(jsr)
   if process.returncode == 0:
       assert process.stdout.__class__ == bytes
       ret = json.loads(process.stdout.decode('utf-8'))
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
   if tmpfile is not None:
       os.unlink(tmpfile.name)

   return (ret, process.returncode != 0, process.stderr)  # JSON (iff successful else None), failed (boolean), stderr capture

def compute_distance(v1, v2, short_vector_penalty=True):
   svp = 1.0
   denom = len(v1) 
   if denom == 0:
      denom = 1
   if short_vector_penalty and denom < 15:
      svp = 20.0 / denom
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
   # we regard zero calls as no-evidence and thus safest to consider the distance infinite. Some may regard it as zero. Its rare for the control artefacts
   # to have no function calls, so we return the less noisy option for now.
   if len(fns) == 0:
       return (float('Inf'), [])
 
   vec1 = []
   vec2 = []
   #print("*** origin_calls {}".format(origin_calls))
   #print("*** control_calls {}".format(control_calls))
   diff_functions = []
   common = 0
   for key in fns:
       # anonymous calls have a key == 'N/A' so do we include them in the calc?
       #if key == 'N/A':  # ignore this for calculation for distance
       #   continue
       a = origin_calls.get(key, 0)
       b = control_calls.get(key, 0)
       vec1.append(a)
       vec2.append(b) 
       if a != b:
           diff_functions.append(key)
       else:
           common += 1 
   commonality_factor = (1 / common) * (len(vec1)-common) if common > 0 else 10 
   return (math.dist(vec1, vec2) * commonality_factor, diff_functions)

def literal_lookup(db, control_url, cache=None):
   assert db is not None
   assert isinstance(control_url, str) and len(control_url) > 0

   if cache is not None and control_url in cache:
       return cache[control_url].get('literals_by_count')

   # vector length is len of union of literals encountered in either vector. Count will differentiate.
   control_literals_doc = db.javascript_controls.find_one({ 'origin': control_url }) # NB: includes literal vector
   if cache is not None:
       cache[control_url] = control_literals_doc

   assert control_literals_doc is not None # should not happen as it means control has been deleted??? maybe bad io???
   control_literals = control_literals_doc.get('literals_by_count')
   assert control_literals is not None
 
   return control_literals

def calculate_literal_distance(control_literals, origin_literals, debug=False):
   assert isinstance(control_literals, dict)
   assert isinstance(origin_literals, dict)

   features = list(set(control_literals.keys()).union(origin_literals.keys())) # use list() to ensure traversal order is predictable
   v1, control_sum = calculate_vector(control_literals, features)
   v2, origin_sum = calculate_vector(origin_literals, features)
   #print(v1)
   #print(v2)
   assert len(v1) == len(v2)
   assert control_sum >= 0
   assert origin_sum >= 0
   
   literals_not_in_origin = set(control_literals.keys()).difference(origin_literals.keys())
   literals_not_in_control = set(origin_literals.keys()).difference(control_literals.keys())
   diff_literals = [lit for lit in features if control_literals.get(lit, 0) != origin_literals.get(lit, 0)]
   t = (
         compute_distance(v1, v2, short_vector_penalty=True),
         len(literals_not_in_origin),
         len(literals_not_in_control),
         diff_literals
       )
   if debug:
      print(control_literals.keys())
      print(len(v1))
      print(control_sum)
      print(origin_sum)
      print(t)
   return t

def find_feasible_controls(desired_sum, controls_to_search, max_distance=100.0):
   if desired_sum < 50: # vector too small for meaningful comparison? In other words, no feasible controls
       return []

   # only those controls within max_distance (either side) from the desired feature sum (from the AST vector under test) are considered feasible
   # all others need not be searched. This could be tighter.
   lb = desired_sum - max_distance
   ub = desired_sum + max_distance
   feasible_controls = [tuple for tuple in filter(lambda t: t[1] >= lb and t[1] <= ub, controls_to_search)]
   return feasible_controls

def encoded_literals(literals_to_encode):
   encoded_literals = map(lambda v: v.replace(',', '%2C'), literals_to_encode)
   return ','.join(encoded_literals)

def find_best_control(input_features, controls_to_search, max_distance=100.0, db=None, debug=False, control_cache=None):
   best_distance = float('Inf')
   origin_url = input_features.get('url', input_features.get('id')) # LEGACY: url field used to be named id field
   cited_on = input_features.get('origin', None)     # report owning HTML page also if possible (useful for data analysis)
   origin_js_id = input_features.get("js_id", None)  # ensure we can find the script directly without URL lookup
   if isinstance(origin_js_id, tuple) or isinstance(origin_js_id, list): # BUG FIXME: should not be a tuple but is... where is that coming from??? so...
       origin_js_id = origin_js_id[0]
   assert isinstance(origin_js_id, str) or origin_js_id is None # check POST-CONDITION
   hash_match = False
   input_vector, total_sum = calculate_ast_vector(input_features['statements_by_count'])
   best_control = BestControl(control_url='', origin_url=origin_url, cited_on=cited_on,
                              sha256_matched=False, 
                              ast_dist=float('Inf'), 
                              function_dist=float('Inf'), 
                              literal_dist=0.0,
                              diff_functions='',
                              origin_js_id=origin_js_id)
   second_best_control = None

   feasible_controls = find_feasible_controls(total_sum, controls_to_search, max_distance=max_distance)
   for fc_tuple in feasible_controls:
       control, control_ast_sum, control_ast_vector = fc_tuple
       assert isinstance(control, dict)
       assert control_ast_sum > 0
       assert isinstance(control_ast_vector, list)
       control_url = control.get('origin')

       ast_dist = math.dist(input_vector, control_ast_vector)
       # compute what we can for now and if we can update it later we will. Otherwise the second_best control may have some fields not-computed
       call_dist, diff_functions = calc_function_dist(input_features['calls_by_count'], control['calls_by_count'])
       new_dist = ast_dist * call_dist
       if new_dist < best_distance and new_dist <= max_distance: 
           if debug:
               print(input_features['calls_by_count'])
               print(control['calls_by_count']) 
               print("Got good distance {} for {} (was {}, max={})".format(new_dist, control_url, best_distance, max_distance)) 
           new_control = BestControl(control_url=control_url, # control artefact from CDN (ground truth)
                                      origin_url=origin_url, # JS at spidered site 
                                      origin_js_id=origin_js_id,
                                      cited_on=cited_on,
                                      sha256_matched=False, 
                                      ast_dist=ast_dist, 
                                      function_dist=call_dist, 
                                      literal_dist=0.0,
                                      diff_functions=' '.join(diff_functions))

           # NB: look at product of two distances before deciding to update best_* - hopefully this results in a lower false positive rate 
           #     (with accidental ast hits) as the number of controls in the database increases
           if best_control.is_better(new_control):
               if second_best_control is None or second_best_control.dist_prod() > new_control.dist_prod():
                   #print("NOTE: improved second_best control")
                   second_best_control = new_control
               # NB: dont update best_* since we dont consider this hit a replacement for current best_control
           else: 
               best_distance = new_dist
               second_best_control = best_control
               best_control = new_control

               if ast_dist < 0.00001:     # small distance means we can try for a hash match against control?
                   hash_match = find_hash_match(db, input_features, best_control.control_url)
                   best_control.sha256_matched = hash_match
                   break    # save time since we've likely found the best control

   if 'literals_by_count' in input_features:
       lv_origin = input_features.get('literals_by_count')
       # NB: cannot use dist_prod() here since we havent initialised all the {ast,literal,function}_dist fields yet...
       half_dist = max_distance / 2  # only quality hits have literals computed since it can be expensive to store
       if best_control.ast_dist * best_control.function_dist < half_dist:
           bc = best_control
           control_literals = literal_lookup(db, bc.control_url, cache=control_cache)
           t = calculate_literal_distance(control_literals, lv_origin)
           assert isinstance(t, tuple) 
           bc.literal_dist, bc.literals_not_in_origin, bc.literals_not_in_control, diff_literals = t
           bc.n_diff_literals = len(diff_literals)
           bc.diff_literals = encoded_literals(diff_literals)
           if bc.sha256_matched:
               # ABANDON ALL HOPE YE WHO ENTER HERE: this should be an assertion, but there appears to be corruption of artefacts somewhere... so 
               # we just issue a warning until we find the problem
               #assert bc.literal_dist <= 0.0000001   # these cannot be non-zero if we had a hash match!
               #assert bc.n_diff_literals == 0
               if bc.literal_dist > 0.0:
                   print("WARNING: literal vector distance should be zero - hash match - corrupted artefact???")
                   print(bc)
 
       sbc = second_best_control
       if sbc is not None and sbc.ast_dist * sbc.function_dist < half_dist:
           control_literals = literal_lookup(db, sbc.control_url, cache=control_cache)
           t = calculate_literal_distance(control_literals, lv_origin)
           sbc.literal_dist, sbc.literals_not_in_origin, sbc.literals_not_in_control, diff_literals = t
           sbc.n_diff_literals = len(diff_literals)
           sbc.diff_literals = encoded_literals(diff_literals) 
   else:
       print("WARNING: literal vector not available in input features")
   return (best_control, second_best_control)

def identify_control_subfamily(cntl_url):
   """
   When controls are added we must identify the subfamily eg. password-strength-meter rather than wordpress. This is so we
   can reasonably accurately determine the site probability of DE functions related to (say) password-strength-meter. Without
   a subfamily we cant accurately do that, so many DE functions will have their significance over-estimated. All controls must have a subfamily
   when put into the database.
   """
   assert isinstance(cntl_url, str)
   subfamily = None
   last_slash_idx = cntl_url.rfind('/')
   assert last_slash_idx > 0
   subfamily = cntl_url[last_slash_idx+1:] 
   if subfamily.endswith(".min.js") or subfamily.endswith("-min.js"):
       subfamily = subfamily[:-6]
   if subfamily.endswith('.js'):
       subfamily = subfamily[:-3]
   subfamily = subfamily.rstrip('-_.') # cleanup punctuation
   subfamily = re.sub('[-_.#~/]+', '-', subfamily) 
   subfamily = re.sub('\b[a-f0-9]+$', '', subfamily) # remove hex digests at end if any
   subfamily = subfamily.lower()
   if 'woocommerce' in cntl_url:
       if len(subfamily) > 20:
           subfamily=subfamily[0:20]
       if 'woocommerce-admin' in cntl_url:
           subfamily = "woocommerce-admin-" + subfamily 
       else:
           subfamily = "woocommerce-" + subfamily 
      
   # some of the above rules may re-introduce punctuation at the end. Sooo...
   subfamily = subfamily.rstrip('-') 

   # and some final rules
   if subfamily.startswith("kendo-culture"):
      subfamily = "kendo-culture"
   if subfamily == "wp-tinymce" or subfamily == "tinymce":
      subfamily = "tiny-mce"
   if subfamily.endswith("-dev"):
      subfamily = subfamily[:-4]

   assert subfamily is not None # POST-CONDITION: must not be true or we will have problems accurately computing a probability  
   return (cntl_url, subfamily)
