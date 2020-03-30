import subprocess
import json
import os
import math 
import hashlib
from dataclasses import asdict
from utils.models import BestControl
from tempfile import NamedTemporaryFile

# comes from MongoDB (db.statements_by_count_keys collection) but for performance is listed here
normalised_ast_features_list =  [ "ArrayLiteral", "Assignment", "AstRoot", "Block", "BreakStatement", "CatchClause", "ConditionalExpression",
        "ContinueStatement", "DoLoop", "ElementGet", "EmptyExpression", "EmptyStatement", "ExpressionStatement", "ForInLoop", "ForLoop",
        "FunctionCall", "FunctionNode", "IfStatement", "InfixExpression", "KeywordLiteral", "Label", "LabeledStatement", "Name",
        "NewExpression", "NumberLiteral", "ObjectLiteral", "ObjectProperty", "ParenthesizedExpression", "PropertyGet",
        "RegExpLiteral", "ReturnStatement", "Scope", "StringLiteral", "SwitchCase", "SwitchStatement", "ThrowStatement",
        "TryStatement", "UnaryExpression", "VariableDeclaration", "VariableInitializer", "WhileLoop", "WithStatement", "XmlLiteral", "XmlString" ]


def safe_for_mongo(function_vector):
   # returns a dict which has all keys made safe for insertion into mongoDB
   # since $ for fields is not permissable in mongo, we replace with F$ 
   d = {}
   for k, v in function_vector.items():
       if k.startswith('$'):
           k = "F"+k
       d[k] = v
   return d

def find_sha256_hash(db, url):
   if db:
       # 1 lookup url
       url_id = db.urls.find_one({ 'url': url })
       if url_id:
            # 2. lookup script_url to find the script_id
            ret = db.script_url.find_one({ 'url_id': url_id.get('_id') })
            if ret:
               # 3. ok, now we can get the script document to return the hash
               script = db.scripts.find_one({ '_id': ret.get('script') }) 
               if script: 
                  return (script.get('sha256'), url_id)
            # else FALLTHRU
   return (None, None)

def get_script(db, artefact, logger):
   # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
   d = { 'sha256': artefact.sha256.strip(), 'md5': artefact.md5.strip(), 'size_bytes': artefact.size_bytes }
   collection = db.snippets if artefact.inline else db.scripts
   js = collection.find_one(d)
   if js:
       return js.get(u'code')
   # oops... something failed so we log it and keep going with the next message
   if logger:
       logger.warning("Failed to find JS in database for {}".format(artefact))
   return None

def analyse_script(js, jsr, producer=None, java='/usr/bin/java', feature_extractor="/home/acas/src/extract-features.jar"):
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
       ret.update(d)       # will contain url key
       ret.pop('id', None) # remove duplicate url entry silently
       call_vector = safe_for_mongo(ret.pop('calls_by_count')) # NB: for correct analysis both the AU JS and control vectors must both be safe-for-mongo'ed consistently
       #print(call_vector)
       ret['calls_by_count'] = call_vector
       # FALLTHRU
   elif producer:
       producer.send("feature-extraction-failures", d)
       # FALLTHRU

   # cleanup
   os.unlink(tmpfile.name)

   return ret

def normalise_vector(ast_features):
   # ensure return vector is standardised in name order with all values filled in
   global normalised_ast_features_list
   ret = []
   sum = 0
   for f in normalised_ast_features_list:
       v = ast_features.get(f, 0)
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
   fns = set(origin_calls.keys())
   fns.update(control_calls.keys())
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

def find_best_control(input_features, controls, ignore_i18n=True, max_distance=100.0, db=None, debug=False): 
   best_control = None
   best_distance = float('Inf')
   hash_match = False
   input_vector, total_sum = normalise_vector(input_features['statements_by_count'])
   if total_sum > 50:  # ignore really small vectors which dont have enough features to enable meaningful comparison
       suitable_controls = filter(lambda c: not (ignore_i18n and '/i18n/' in c), controls)
       for control in suitable_controls:
           control_url = control.get('origin')
           control_vector, _ = normalise_vector(control['statements_by_count'])
       
           dist = math.dist(input_vector, control_vector)
           if dist < best_distance and dist <= max_distance:
               if debug:
                   print("Got good distance {} for {}".format(dist, control_url)) 
               best_control = control_url
               best_distance = dist
               control_function_calls = control['calls_by_count']
               if dist < 0.00001:     # try for a hash match against a control?
                   hash_match = find_hash_match(db, input_features, best_control)
                   break    # save time since we've likely found the best control
   else:
       print("WARNING: too small vector for meaningful control matching: {}".format(input_features))
 
   diff_functions = []
   function_dist = float('Inf') # NB: only computed if best_distance < max_distance as it can be quite expensive
   if best_distance < max_distance:
       function_dist, diff_functions = calc_function_dist(input_features['calls_by_count'], control_function_calls)

   return BestControl(control_url=best_control, 
                      origin_url=input_features.get('url', input_features.get('id')), # LEGACY: url field used to be named id field
                      sha256_matched=hash_match, 
                      ast_dist=best_distance, 
                      function_dist=function_dist, 
                      diff_functions=' '.join(diff_functions)) # functions for which there is a difference in call counts between control and input JS
