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

def get_script(db, artefact, logger):
   # if its an inline script it will be in db.snippets otherwise it will be in db.scripts - important to get it right!
   d = { 'sha256': artefact.sha256.strip(), 'md5': artefact.md5.strip(), 'size_bytes': artefact.size_bytes }
   if artefact.inline:
       js = db.snippets.find_one(d)
       if js:
           return js.get(u'code')
   else:
       js = db.scripts.find_one(d)
       if js:
           return js.get(u'code')
   # oops... something failed so we log it and keep going with the next message
   if logger:
       logger.warning("Failed to find JS in database for {}".format(artefact))
   return None

def analyse_script(js, jsr, producer=None, java='/usr/bin/java', feature_extractor="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar"):
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
       ret['calls_by_count'] = safe_for_mongo(ret.pop('calls_by_count'))
   elif producer:
       producer.send("feature-extraction-failures", d)

   # cleanup
   os.unlink(tmpfile.name)

   return ret

def normalise_vector(ast_features):
   # ensure return vector is standardised in name order with all values filled in
   global normalised_ast_features_list
   ret = []
   for f in normalised_ast_features_list:
       ret.append(ast_features.get(f, 0))  # vector must always have the same length with the keys in a consistent order for comparison

   return ret

def find_script(db, url):
   if db:
       # first lookup url
       url_id = db.urls.find_one({ 'url': url })
       #print(url_id)
       if url_id:
            # next lookup script_url to find the script_id
            ret = db.script_url.find_one({ 'url_id': url_id.get('_id') })
            #print(ret)
            if ret:
               script = db.scripts.find_one({ '_id': ret.get('script') }) 
               if script: 
                  return (script.get('sha256'), url_id)
            # FALLTHRU
   return (None, None)

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
           actual_sha256, url_id = find_script(db, url)
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

def find_best_control(input_features, controls, ignore_i18n=True, max_distance=100.0, db=None): 
   best_control = None
   best_distance = float('Inf')
   hash_match = False
   input_vector = normalise_vector(input_features['statements_by_count'])
   for control in controls:
       control_url = control.get('origin')
       if ignore_i18n and '/i18n/' in control_url:
           continue

       control_vector = normalise_vector(control['statements_by_count'])
       
       dist = math.dist(input_vector, control_vector)
       if dist < best_distance and dist <= max_distance:
            best_control = control_url
            best_distance = dist
            control_function_calls = control['calls_by_count']
            if dist < 0.01:     # try for a hash match against a control?
                hash_match = find_hash_match(db, input_features, best_control)
             

   diff_functions = []
   function_dist = float('Inf') # NB: only computed if best_distance < max_distance as it can be quite expensive
   if best_distance < max_distance:
       function_dist, diff_functions = calc_function_dist(input_features['calls_by_count'], control_function_calls)

   return BestControl(control_url=best_control, 
                      origin_url=input_features.get('url', input_features.get('id')), # LEGACY: url field used to be name id field
                      sha256_matched=hash_match, 
                      ast_dist=best_distance, 
                      function_dist=function_dist, 
                      diff_functions=' '.join(diff_functions)) # functions for which there is a difference in call counts between control and input JS
