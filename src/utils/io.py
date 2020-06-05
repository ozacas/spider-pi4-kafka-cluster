from utils.features import safe_for_mongo
from utils.models import JavascriptArtefact, JavascriptVectorSummary
from collections import namedtuple
from dataclasses import asdict
from datetime import datetime
from pymongo import ReturnDocument, ASCENDING
import hashlib
import requests
from bson.binary import Binary
from itertools import chain, islice
from utils.features import analyse_script, calculate_ast_vector, identify_control_subfamily

def save_artefact(db, producer, artefact: JavascriptArtefact, root, to, content=None, inline=False, content_type='text/javascript' ):
   assert db is not None
   assert producer is not None
   assert content is not None or (root is not None and artefact.path is not None)
   assert isinstance(to, str) and len(to) > 0
 
   if content is None: 
        path = "{}/{}".format(root, artefact.path)
        content = None
        with open(path, 'rb') as fp:
            content = fp.read()
   assert content is not None

   d, js_id = save_script(db, artefact, Binary(content))
   assert len(js_id) > 0
   assert 'sha256' in d
   assert 'md5' in d
   assert 'size_bytes' in d
   d.update({ 'url': artefact.url,
              'inline': inline,
              'content-type': content_type,
              'when': artefact.when,
              'origin': artefact.origin,
              'js_id': js_id })
   producer.send(to, d)

def save_analysis_content(db, jsr: JavascriptArtefact, bytes_content, js_id: str=None, ensure_indexes=False):
   assert bytes_content is not None
   assert len(js_id) > 0
   assert isinstance(bytes_content, bytes)

   if ensure_indexes:
       db.analysis_content.create_index([( 'js_id', ASCENDING ), ( 'byte_content_sha256', ASCENDING ) ], unique=True)

   d = asdict(jsr)
   expected_hash = hashlib.sha256(bytes_content).hexdigest()
   d.update({ "js_id": js_id, "analysis_bytes": bytes_content, "byte_content_sha256": expected_hash })
   db.analysis_content.find_one_and_update({ "js_id": js_id, 'byte_content_sha256': expected_hash }, { "$set": d }, upsert=True)

def next_artefact(iterable, max: float, filter_cb: callable, verbose=False):
    n = 0
    for message in filter(lambda m: filter_cb is None or filter_cb(m.value), iterable):
        yield message.value
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records. {}".format(n, str(datetime.utcnow())))
        if n >= max:
            break

def save_url(db, artefact):
   result = db.urls.insert_one({ 'url': artefact.url, 'last_visited': artefact.when, 'origin': artefact.origin })
   return result.inserted_id

def save_script(db, artefact: JavascriptArtefact, script: bytes):
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
                                      return_document=ReturnDocument.AFTER)
   js_id = s.get('_id')
   db.script_url.insert_one( { 'url_id': url_id, 'script': js_id })

   return (key, js_id)

# https://stackoverflow.com/questions/8290397/how-to-split-an-iterable-in-constant-size-chunks
# https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it/24527424
def batch(iterable, n=1000):
    iterable = iter(iterable)
    while True:
        x = tuple(islice(iterable, n))
        if not x:
            return
        yield x

def save_control(db, url, family, variant, version, force=False, refuse_hashes=None, provider='', java='/usr/bin/java', feature_extractor=None, content=None):
   """
   Update all control related data. Note callers must supply refuse_hashes (empty set) or an error will result

   Returns JavascriptArtefact representing control which has had its state updated into MongoDB
   """
   assert url is not None
   assert family is not None
   assert version is not None
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

   bytes_content, failed, stderr = analyse_script(content, jsr, java=java, feature_extractor=feature_extractor)
   if failed:
       raise ValueError('Could not analyse script {} - {}'.format(jsr.url, stderr))
   ret = json.loads(bytes_content.decode())
   ret.update({ 'family': family, 'release': version, 'variant': variant, 'origin': url, 'provider': provider, 'subfamily': identify_control_subfamily(jsr.url) })
   #print(ret)
   # NB: only one control per url/family pair (although in theory each CDN url is enough on its own)
   resp = db.javascript_controls.find_one_and_update({ 'origin': url, 'family': family },
                                                     { "$set": ret }, upsert=True)
   db.javascript_control_code.find_one_and_update({ 'origin': url },
                                                     { "$set": { 'origin': url, 'code': Binary(content), 
                                                       'analysis_bytes': bytes_content, 'analysis_vectors_sha256': hashlib.sha256(bytes_content).hexdigest(),
                                                       "last_updated": jsr.when } }, upsert=True)

   vector, total_sum = calculate_ast_vector(ret['statements_by_count'])
   assert total_sum >= 50   # vectors smaller than this are too small to match accurately - and may indicate an issue with the download/code
   sum_of_function_calls = sum(ret['calls_by_count'].values())
   sum_of_literals = sum(ret['literals_by_count'].values())
   vs = JavascriptVectorSummary(origin=url, sum_of_ast_features=total_sum,
                                 sum_of_functions=sum_of_function_calls, sum_of_literals=sum_of_literals, last_updated=jsr.when)
   db.javascript_controls_summary.find_one_and_update({ 'origin': url }, { "$set": asdict(vs) }, upsert=True)
   return jsr

def load_controls(db, min_size=1500, literals_by_count=False, verbose=False):
   all_controls = []
   for control in db.javascript_controls.find(
                      { "size_bytes": { "$gte": min_size } }, 
                      { 'literals_by_count': literals_by_count }):             # dont load literal vector to save considerable memory
       ast_vector, ast_sum = calculate_ast_vector(control['statements_by_count'])
       tuple = (control, ast_sum, ast_vector)
#       print(tuple)
       all_controls.append(tuple)
   if verbose:
       print("Loaded {} controls, each at least {} bytes".format(len(all_controls), min_size))
   return all_controls
