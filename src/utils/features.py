import subprocess
import json
import os
from io import StringIO
import re
import math 
import hashlib
import pylru
from codecs import encode, decode
from bs4 import UnicodeDammit
import jsbeautifier.unpackers.packer as packer
from itertools import chain
from bson.objectid import ObjectId
from dataclasses import asdict
from utils.models import BestControl, JavascriptArtefact
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

def validate_feature_vectors(db, msg, java, extractor):
    """
    Returns True if the features in msg can be validated using only the Mongo-stored artefact (ie. not kafka) and re-calculating. Otherwise False
    Slow and expensive, so only called when --defensive is specified. Extensive checking to validate that key metrics are exactly what they should be so
    that data quality across the pipeline is high.
    """
    assert isinstance(msg, dict)
    assert 'url' in msg
    assert os.path.exists(java)
    assert os.path.exists(extractor)

    jsr = JavascriptArtefact(url=msg['url'], sha256=msg['sha256'], md5=msg['md5'], size_bytes=msg['size_bytes'])
    ret = db.scripts.find_one({ '_id': ObjectId(msg['js_id']) })
    if ret is None:
        raise ValueError("Could not locate script: {}".format(jsr.url))
    features, failed, stderr = analyse_script(ret.get('code'), jsr, java=java, feature_extractor=extractor)
    if failed:
        return False
    
    l = len(features['literals_by_count'])
    o = len(msg['literals_by_count'])
    print(l, " ", o)
    assert o == l
    assert features['statements_by_count'] == msg['statements_by_count']
    assert features['calls_by_count'] == msg['calls_by_count']
    return True
 
def find_sha256_hash(db, url):
   """
   Similar to find_script(), but returns only the sha256 hexdigest (if found) 
   """
   script, url_id = find_script(db, url, want_code=False)
   if script:
       return (script.get('sha256'), url_id)
   return (None, None)

def get_script(db, artefact):
   if isinstance(artefact, str):
       js = db.scripts.find_one({ '_id': ObjectId(artefact) })
   elif isinstance(artefact, JavascriptArtefact) and len(artefact.js_id) > 0:
       ret = get_script(db, artefact.js_id)
       assert ret[1] == artefact.js_id    # verify we found the right artefact?
       return ret
   else:
       # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
       d = { 'sha256': artefact.sha256.strip(), 
             'md5': artefact.md5.strip(), 
             'size_bytes': artefact.size_bytes } # ENSURE we set the right fields so Mongo uses the index
       collection = db.snippets if artefact.inline else db.scripts
       js = collection.find_one(d)

   if js is None:
       return (None, None)
   id = str(js.get('_id'))
   return (js.get(u'code'), id)

def fast_decode(byte_content):
   """
   We assume that most AU artefacts will be utf-8 encoded (for speed). If that errors, we go thru a slow process but more universal process to decode....
   """
   assert isinstance(byte_content, bytes)
   try:
       source = byte_content.decode('utf-8', errors='strict')
       return source
   except:
       # https://stackoverflow.com/questions/436220/how-to-determine-the-encoding-of-text
       encoding = UnicodeDammit(byte_content)
       source = byte_content.decode(encoding.original_encoding, errors='strict')
       return source

def is_artefact_packed(filename, byte_content):
   """
   Returns a tuple (is_packed, unpacked_byte_content) if the artefact is packed using an algorithm
   similar to Dave Edward's p.a.c.k.e.r algorithm. This appears to generate syntax errors compounding feature analysis,
   but we try anyway...
   """
   # dont load the file again if we already have the byte-content...
   if byte_content is None:
       with open(filename, 'rb') as fp:
           byte_content = fp.read()

   try:
       source = fast_decode(byte_content)
       is_packed = packer.detect(source) 
       if is_packed:
           ret = packer.unpack(source)
           # TODO FIXME... yuk this is so silly! But we have to return bytes since extract-features.jar program expects that... 
           # fortunately most artefacts arent packed so this is not as expensive as it could be...
           result = decode(encode(ret, 'utf-8', 'backslashreplace'), 'unicode-escape').encode()
           assert isinstance(result, bytes)
           assert len(result) > 0
           return (True, result)
       # else 
       return (False, byte_content)
   except Exception as e:
       # we fail an artefact that is purportedly packed, but which cannot be unpacked...
       raise e

def save_temp_file(byte_content):
    assert isinstance(byte_content, bytes)
    assert len(byte_content) > 0

    tmpfile = NamedTemporaryFile(delete=False)
    tmpfile.write(byte_content)
    tmpfile.close()
    return (tmpfile, tmpfile.name)
    
def analyse_script(js, jsr, java='/usr/bin/java', feature_extractor="/home/acas/src/extract-features.jar"):
   is_bytes = isinstance(js, bytes)
   if is_bytes:
       tmpfile, fname = save_temp_file(js)
   else:
       fname = js
       tmpfile = None

   # TODO FIXME add support for packed JS here... with autounpacking (just changes the feature vectors but NOT the script content in Mongo)
   try:
       is_packed, unpacked_byte_content = is_artefact_packed(fname, js if is_bytes else None)
       if is_packed:
           if tmpfile is not None:
              os.unlink(tmpfile.name)
           assert len(unpacked_byte_content) > 0
           tmpfile, fname = save_temp_file(unpacked_byte_content)
           # FALLTHRU... although it will probably fail since unpacker is VERY unreliable... and often produces syntax errors
   except Exception as e:
       # if we get an exception, we assume that its bad packed artefact and reject the whole analysis
       print("WARNING: unpack failed for {} - ignored.".format(jsr))
       return (None, True, str(e)) # (no bytes, failed, stderr == str(e))

   # ensure we get lossless output using recommendations reported at: https://bugs.python.org/issue34618
   environ = os.environ.copy()
   environ['PYTHONIOENCODING'] = 'utf-8' 

   # save to file and run extract-features.jar to identify the javascript features
   process = subprocess.run([java, "-jar", feature_extractor, fname, jsr.url], env=environ, capture_output=True, encoding='utf-8', errors='strict')
   
   # turn process stdout into something we can save
   ret = process.stdout
   assert isinstance(ret, str)
   bytes_content = ret.encode('utf-8', errors='strict') # any errors will hopefully get caught, despite double-encoding
   assert isinstance(bytes_content, bytes)

   # cleanup
   if tmpfile is not None:
       os.unlink(tmpfile.name)

   return (bytes_content, process.returncode != 0, process.stderr)  # JSON (iff successful else None), failed (boolean), stderr capture

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

def calc_function_dist(origin_calls, control_calls):
   # function distance is a little different: all function calls are used for distance, even if only present in one vector 
   o_calls = set(origin_calls.keys())
   c_calls = set(control_calls.keys())
   fns = c_calls.union(o_calls)
   # we regard zero calls as no-evidence and thus safest to consider the distance infinite. Some may regard it as zero. Its rare for the control artefacts
   # to have no function calls, so we return the less noisy option for now.
   n_function_calls = len(fns)
   if n_function_calls == 0:
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
   svp = 20.0 / n_function_calls if n_function_calls < 15 else 1.0
   return (math.dist(vec1, vec2) * commonality_factor * svp, diff_functions)

def lookup_control_literals(db, control_url, debug=True):
   assert db is not None
   assert isinstance(control_url, str) and len(control_url) > 0

   # vector length is len of union of literals encountered in either vector. Count will differentiate.
   control_literals_doc = db.javascript_control_code.find_one({ 'origin': control_url }, {'code': 0}) # NB: includes literal vector
   assert control_literals_doc is not None # should not happen as it means control has been deleted??? maybe bad io???
   assert 'analysis_bytes' in control_literals_doc
   analysis_bytes = control_literals_doc.get('analysis_bytes')

   control_literals = json.loads(analysis_bytes.decode('utf-8')).get('literals_by_count')
   assert control_literals is not None
   return control_literals

def truncate_literals(literal_dict):
   assert isinstance(literal_dict, dict)
   return { k[0:200] : v for k,v in literal_dict.items() }

def calculate_literal_distance(control_literals, origin_literals, debug=False, fail_if_difference=False):
   assert isinstance(control_literals, dict)
   assert isinstance(origin_literals, dict)

   # NB: control_literals are not truncated to 200 chars, so we do it on-the-fly as performance is ok for now
   v1 = []
   v2 = []
   
   features = set()
   diff_literals = []
   n_not_in_origin = n_not_in_control = 0
   for k in origin_literals.keys():
       features.add(k)
       if not k in control_literals:  # case 1: k is not a literal present in the control 
          v1.append(origin_literals[k])
          v2.append(0)
          n_not_in_control += 1
          diff_literals.append(k)
          if fail_if_difference:
              raise ValueError("Found incorrect case1 difference: {}\n{}\n{}".format(k, control_literals, origin_literals))
       else:                          # case 2: k is present in both vectors, but not necessarily with the same count
          v1_count = origin_literals[k]
          v2_count = control_literals[k]
          v1.append(v1_count)
          v2.append(v2_count)
          if v1_count != v2_count:
              diff_literals.append(k) 
              if fail_if_difference:
                  raise ValueError("Found incorrect case 2 difference: {}\n{}\n{}".format(k, control_literals, origin_literals))
   for k in control_literals.keys():
       if not k in features:          # case 3: k is present in the control but not in origin vector
          features.add(k)
          v1.append(0)
          v2.append(control_literals[k])
          n_not_in_origin += 1
          diff_literals.append(k)
          if fail_if_difference:
              raise ValueError("Found incorrect case 3 difference: {}\n{}\n{}".format(k, control_literals, origin_literals))

   assert len(v1) == len(v2)       
   assert len(features) == len(v1)

   t = (
         compute_distance(v1, v2, short_vector_penalty=True),
         n_not_in_origin,
         n_not_in_control,
         diff_literals
       )

   if debug:
      print(v1)
      print(v2)
      print(t)
   return t

def find_feasible_controls(db, ast_desired_sum, function_call_sum, max_distance=150.0, control_cache=None, debug=False):
   assert ast_desired_sum >= 0
   assert function_call_sum >= 0

   if ast_desired_sum < 50: # vector too small for meaningful comparison? In other words, no feasible controls
       return []

   # only those controls within max_distance (either side) from the desired feature sum (from the AST vector under test) are considered feasible
   # all others need not be searched. This could be tighter.
   lb = ast_desired_sum - max_distance 
   ub = ast_desired_sum + max_distance
   # but we now also consider function call distance (only use half max_distance since function calls are less frequent than AST features)
   lb_fc = function_call_sum - (max_distance / 2)
   ub_fc = function_call_sum + (max_distance / 2)

   n = n_cached = 0
   ast_hits = db.javascript_controls_summary.find({ 'sum_of_ast_features': { '$gte': lb, '$lt': ub } }) 
   fcall_hits = db.javascript_controls_summary.find({ 'sum_of_function_calls': { '$gte': lb_fc, '$lt': ub_fc } })
   feasible_controls = []
   seen = set()
   for rec in chain(ast_hits, fcall_hits): 
       n += 1
       control_url = rec.get('origin')
       if control_url in seen:
           continue # de-dupe if function call hit was also hit by ast_hits
       seen.add(control_url)

       if control_cache is not None and control_url in control_cache:
           t = control_cache[control_url]
           feasible_controls.append(t)
           control_cache[control_url] = t # mark record as MRU ie. least likely to be evicted, since it is being hit 
           n_cached += 1
       else:
           ast_sum = rec.get('sum_of_ast_features')
           control_doc = db.javascript_controls.find_one({ 'origin': control_url }, { 'statements_by_count': 0, 'literals_by_count': 0, 'calls_by_count': 0 })
           assert isinstance(control_doc, dict)
           if 'do_not_load' in control_doc and control_doc['do_not_load']: # ignore those controls which are do_not_load == True
               continue
           vector_doc = db.javascript_control_code.find_one({ 'origin': control_url }, { 'code': 0 })
           assert vector_doc is not None
           assert 'analysis_bytes' in vector_doc
           vectors = json.loads(vector_doc.get('analysis_bytes'))
           ast_vector, calc_sum = calculate_ast_vector(vectors['statements_by_count'])
           assert calc_sum == ast_sum # database must match calculated or something is very wrong...
           t = (control_doc, ast_sum, ast_vector, vectors['calls_by_count']) 
           if control_cache is not None:
               control_cache[control_url] = t
           feasible_controls.append(t)

   if debug:
       print("find_feasible_controls():")
       print("ast range [{:.2f}, {:.2f}] function call range [{:.2f}, {:.2f}]".format(lb, ub, lb_fc, ub_fc))
       if n > 0:
           print("Considered {} controls - cache hit rate {:.2f}%".format(n, (float(n_cached) / n)*100.0))
       else:
           print("No feasible controls found.")

   return feasible_controls

def fix_literals(literals_to_encode):
   ret = map(lambda v: v.replace(',', '%2C'), literals_to_encode)
   return ','.join(ret)

def distance(origin_ast, control_ast, origin_calls, control_calls, debug=False, additive_weight=0.5, ast_weight=0.8):
   if debug:
       assert isinstance(origin_ast, list)
       assert isinstance(control_ast, list)
       assert isinstance(origin_calls, dict)
       assert isinstance(control_calls, dict)
       assert len(origin_ast) == len(control_ast)
   ast_dist = compute_distance(origin_ast, control_ast, short_vector_penalty=True) 
   assert ast_dist >= 0.0
   call_dist, diff_functions = calc_function_dist(origin_calls, control_calls)
   assert call_dist >= 0.0
   # the additive term is not taken entirely: only up to the additive weight fraction, since we must manage total distance to not reject legitimate hits
   # weight AST distance slightly less than 1 to not weight syntax too highly   
   return ((ast_dist * ast_weight * call_dist) + (ast_dist + call_dist)*additive_weight, ast_dist, call_dist, diff_functions)

def find_best_control(db, input_features, max_distance=200.0, debug=False, control_cache=None):
   """
   Search all controls with AST vector magnitudes within max_distance and find the best hit (lowest product of AST*call distance)
   against suitable controls. Does not currently use literal distance for the calculation. Could be improved.... returns up to two hits
   representing the best and next best hits (although the latter may be None).
   """
   assert db is not None
   origin_url = input_features.get('url', input_features.get('id')) # LEGACY: url field used to be named id field
   cited_on = input_features.get('origin', None)     # report owning HTML page also if possible (useful for data analysis)
   origin_js_id = input_features.get("js_id", None)  # ensure we can find the script directly without URL lookup
   if isinstance(origin_js_id, tuple) or isinstance(origin_js_id, list): # BUG FIXME: should not be a tuple but is... where is that coming from??? so...
       origin_js_id = origin_js_id[0]
   assert isinstance(origin_js_id, str) and len(origin_js_id) == 24

   best_distance = float('Inf')
   input_ast_vector, ast_sum = calculate_ast_vector(input_features['statements_by_count'])
   fcall_sum = sum(input_features['calls_by_count'].values())
   best_control = BestControl(control_url='', origin_url=origin_url, cited_on=cited_on,
                              sha256_matched=False, 
                              ast_dist=float('Inf'), 
                              function_dist=float('Inf'), 
                              literal_dist=0.0,
                              diff_functions='',
                              origin_js_id=origin_js_id)
   second_best_control = None

   # we open the distance to explore "near by" a little bit... but the scoring for these hits is unchanged
   if debug:
       print("find_best_control({})".format(origin_url))
   feasible_controls = find_feasible_controls(db, ast_sum, fcall_sum, debug=debug, control_cache=control_cache) 
   for fc_tuple in feasible_controls:
       control, control_ast_sum, control_ast_vector, control_call_vector = fc_tuple
       assert isinstance(control, dict)
       assert control_ast_sum > 0
       assert isinstance(control_ast_vector, list)
       control_url = control.get('origin')

       # compute what we can for now and if we can update it later we will. Otherwise the second_best control may have some fields not-computed
       new_distance, ast_dist, call_dist, diff_functions = distance(input_ast_vector, control_ast_vector, 
                                                                    input_features['calls_by_count'], control_call_vector, debug=debug)
       if new_distance < best_distance and new_distance <= max_distance:
           if debug:
               print("Got good distance {} for {} (was {}, max={})".format(new_distance, control_url, best_distance, max_distance)) 
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
           if best_control.is_better(new_control, max_distance=max_distance):
               second_dist = second_best_control.distance() if second_best_control is not None else 0.0
               if second_best_control is None or second_dist > new_control.distance():
                   if debug:
                       print("NOTE: improved second_best control was {} now is {}".format(second_best_control, new_control))
                   second_best_control = new_control
               # NB: dont update best_* since we dont consider this hit a replacement for current best_control
           else: 
               best_distance = new_distance
               second_best_control = best_control
               best_control = new_control

               if best_distance < 0.00001:     # small distance means we can try for a hash match against control?
                   assert control_url == best_control.control_url
                   hash_match = (control['sha256'] == input_features['sha256'])
                   best_control.sha256_matched = hash_match
                   break    # save time since we've likely found the best control but this may mean next_best_control is not second best in rare cases

   # NB: literal fields in best_control/next_best_control are updated elsewhere... not here
   return (best_control, second_best_control)

def update_literal_distance(db, hit: BestControl, ovec, fail_if_difference=False):
    assert hit is not None
    assert ovec is not None
    control_literal_vector = lookup_control_literals(db, hit.control_url, debug=fail_if_difference) if len(hit.control_url) > 0 else None
    if control_literal_vector is None:
        hit.literal_dist = -1.0
        hit.literals_not_in_origin = -1
        hit.literals_not_in_control = -1
        hit.n_diff_literals = -1
        hit.diff_literals = ''
        return
    v1 = truncate_literals(control_literal_vector)
    v2 = truncate_literals(ovec)
    try:
        t = calculate_literal_distance(v1, v2, fail_if_difference=fail_if_difference)
        hit.literal_dist, hit.literals_not_in_origin, hit.literals_not_in_control, diff_literals = t
        hit.n_diff_literals = len(diff_literals)
        hit.diff_literals = fix_literals(diff_literals)
    except ValueError as ve:
        raise ve

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
   mapping = ( ('admin-', 'admin'), ('chunk-googlesitekit-adminbar-', 'googlesitekit-adminbar'),
               ('chunk-googlesitekit-setup-wizard-', 'googlesitekit-setup-wizard'), ('chunk-googlesitekit-setup-wrapper-', 'googlesitekit-setup-wrapper'),
               ('date-', 'date'), ('editor', 'editor'), ('edit-', 'edit'), ('editor-', 'editor'), ('eland-tracker-', 'eland-tracker'),
               ('jquery-spservices-', 'jquery-spservices'), ('login-', 'login'), ('modernizr-', 'modernizr'), 
               ('moment-timezone-with-data-', 'moment-timezone-with-data'), ('chunk-googlesitekit-', 'googlesitekit'),
               ('admin-ajaxwatcher-', 'admin-ajaxwatcher'), ('authorizenet-accept-', 'authorizenet'),
               ('jquery-ui-timepicker-addon-', 'jquery-ui-timepicker-addon'), ('ui-bootstrap-', 'ui-bootstrap'),
               ('vendors-chunk-googlesitekit-adminbar-', 'googlesitekit-adminbar'),
               ('woocommerce-admin-', 'woocommerce-admin'), ('woocommerce-product-', 'woocommerce-product'),
   )

   for t in mapping:
       starts_with, replace_with = t
       if subfamily.startswith(starts_with):
           subfamily = replace_with
 
   assert subfamily is not None # POST-CONDITION: must not be true or we will have problems accurately computing a probability  
   assert isinstance(subfamily, str)
   return (cntl_url, subfamily)
