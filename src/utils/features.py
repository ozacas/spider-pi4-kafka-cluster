import subprocess
import json
import os
import math 
import hashlib
from dataclasses import asdict
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

def find_hash_match(db, input_features, control_url):
   if db:
       rec = db.javascript_controls.find_one({ 'origin': control_url })
       if rec is None:
           print("Could not find control {}".format(control_url))
           return False
       expected_sha256 = rec.get('sha256', None)    
       actual_sha256 = input_features.get('sha256')
       # lookup db if sha256 is not in kafka message? nah, not for now... TODO FIXME
       return expected_sha256 != actual_sha256
   return False
 
def find_best_control(input_features, controls, ignore_i18n=True, max_distance=100.0, db=None): 
   best_control = None
   best_distance = float('Inf')
   hash_match = False
   input_vector = normalise_vector(input_features['statements_by_count'])
   #print(input_vector)
   for control in controls:
       if ignore_i18n and '/i18n/' in control['origin']:
           continue

       control_vector = normalise_vector(control['statements_by_count'])
       
       dist = math.dist(input_vector, control_vector)
       if dist < best_distance and dist <= max_distance:
            best_control = control['origin']
            best_distance = dist
            if dist < 0.01:     # try for a hash match against a control?
                hash_match = find_hash_match(db, input_features, best_control)

   return (best_control, best_distance, hash_match)
