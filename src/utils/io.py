from utils.features import safe_for_mongo
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from collections import namedtuple
from dataclasses import asdict
from datetime import datetime
import pymongo
import hashlib
import requests
from bson.binary import Binary
from itertools import chain, islice
from utils.features import analyse_script, calculate_ast_vector

def save_ast_vector(db, jsr: JavascriptArtefact, ast_vector, js_id: str=None):
   assert ast_vector is not None
   d = { "js_id": js_id, "url": jsr.url, "origin": jsr.origin }
   d.update(**ast_vector)  # ast_vector never needs to be made safe for Mongo, since its just mozilla rhino statement types for keys
   d.update({ "js_id": js_id })
   assert '_id' not in d.keys()
   db.statements_by_count.insert_one(d)

def get_function_call_vector(db, url):
    ret = db.count_by_function.find_one({ 'url': url })
    return ret

def save_call_vector(db, jsr: JavascriptArtefact, call_vector, js_id: str=None):
   assert call_vector is not None
   d = { "js_id": js_id, "url": jsr.url, "origin": jsr.origin }
   calls = safe_for_mongo(call_vector)
   d['calls'] = calls
   d.pop('_id', None) # BUG: FIXME -- sometimes it appears to be present, so... maybe topic has a bit of pollution during dev???
   db.count_by_function.insert_one(d)

def next_artefact(iterable, max: float, filter_cb: callable, verbose=False):
    n = 0
    for message in filter(lambda m: filter_cb is None or filter_cb(m.value), iterable):
        yield message.value
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records. {}".format(n, str(datetime.utcnow())))
        if n >= max:
            break

def artefact_tuple():
   ret = namedtuple('Artefact', 'url origin host checksum path when')
   ret.__lt__ = lambda self, other: self.checksum < other.checksum
   return ret

def save_url(db, artefact):
   result = db.urls.insert_one({ 'url': artefact.url, 'last_visited': artefact.when, 'origin': artefact.origin })
   return result.inserted_id

def save_script(db, artefact, script: bytes):
   # NB: we work hard here to avoid mongo calls which will cause performance problems (hashing too)

   # compute hashes to search for
   sha256 = hashlib.sha256(script).hexdigest()
   md5 = hashlib.md5(script).hexdigest()
   if md5 != artefact.checksum:
       raise ValueError("Expected MD5 and MD5 hash do not match: {} {} != {}".format(artefact.url, md5, artefact.checksum))

   # check to see if in mongo already
   url_id = save_url(db, artefact)

   script_len = len(script)
   key = { 'sha256': sha256, 'md5': md5, 'size_bytes': script_len }  
   value = key.copy() # NB: shallow copy is sufficient for this application
   value.update({ 'code': script })

   s = db.scripts.find_one_and_update(key, { '$set': value }, 
                                      upsert=True, 
                                      projection={ 'code': False },
                                      return_document=pymongo.ReturnDocument.AFTER)
   db.script_url.insert_one( { 'url_id': url_id, 'script': s.get(u'_id') })

   return key

# https://stackoverflow.com/questions/8290397/how-to-split-an-iterable-in-constant-size-chunks
# https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it/24527424
def batch(iterable, n=1000):
    iterable = iter(iterable)
    while True:
        x = tuple(islice(iterable, n))
        if not x:
            return
        yield x

def save_control(db, url, family, version, variant, force=False, refuse_hashes=None, provider='', java='/usr/bin/java', feature_extractor=None, content=None):
   """
   Update all control related data. Note callers must supply refuse_hashes (empty set) or an error will result

   Returns JavascriptArtefact representing control which has had its state updated into MongoDB
   """
   if content is None:
      resp = requests.get(url)
      if resp.status_code != 200:
          raise ValueError("Failed to fetch [{}] {}".format(resp.status_code, url))
      content = resp.content

   jsr = JavascriptArtefact(when=str(datetime.utcnow()), sha256=hashlib.sha256(content).hexdigest(),
                             md5 = hashlib.md5(content).hexdigest(), url=url,
                             inline=False, content_type='text/javascript', size_bytes=len(content))
   if jsr.size_bytes < 1000:
       print("Refusing artefact as too small to enable meaningful vector comparison: {}".format(jsr))
       return jsr

   if not force and jsr.sha256 in refuse_hashes:
       print("Refusing to update existing control as dupe: {}".format(jsr))
       return jsr

   ret, failed, stderr = analyse_script(content, jsr, java=java, feature_extractor=feature_extractor)
   if failed:
       raise ValueError('Could not analyse script {}'.format(jsr.url))
   ret.update({ 'family': family, 'release': version, 'variant': variant, 'origin': url, 'provider': provider })
   #print(ret)
   # NB: only one control per url/family pair (although in theory each CDN url is enough on its own)
   resp = db.javascript_controls.find_one_and_update({ 'origin': url, 'family': family },
                                                     { "$set": ret }, upsert=True)
   db.javascript_control_code.find_one_and_update({ 'origin': url },
                                                     { "$set": { 'origin': url, 'code': Binary(content),
                                                       "last_updated": jsr.when } }, upsert=True)

   vector, total_sum = calculate_ast_vector(ret['statements_by_count'])
   sum_of_function_calls = sum(ret['calls_by_count'].values())
   sum_of_literals = sum(ret['literals_by_count'].values())
   vs = JavascriptVectorSummary(origin=url, sum_of_ast_features=total_sum,
                                 sum_of_functions=sum_of_function_calls, sum_of_literals=sum_of_literals, last_updated=jsr.when)
   db.javascript_controls_summary.find_one_and_update({ 'origin': url }, { "$set": asdict(vs) }, upsert=True)
   return jsr

def load_controls(db, min_size=1500, literals_by_count=False):
   all_controls = []
   for control in db.javascript_controls.find(
                      { "size_bytes": { "$gte": min_size } }, 
                      { 'literals_by_count': literals_by_count }):             # dont load literal vector to save considerable memory
       ast_vector, ast_sum = calculate_ast_vector(control['statements_by_count'])
       tuple = (control, ast_sum, ast_vector)
#      print(tuple)
       all_controls.append(tuple)
   return all_controls
