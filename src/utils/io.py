from dataclasses import asdict
from datetime import datetime
from pymongo import ReturnDocument, ASCENDING
import hashlib
import json
import requests
from bson.binary import Binary
from bson.objectid import ObjectId
from itertools import chain, islice
from utils.features import analyse_script, calculate_ast_vector, identify_control_subfamily, get_script
from utils.models import JavascriptArtefact, JavascriptVectorSummary, DownloadArtefact


def find_or_update_analysis_content(db, m, fail_iff_not_found=False, defensive=False,
                                           java=None, extractor=None, force=False):
    assert isinstance(m, dict)
    assert all(['js_id' in m, 'url' in m, 'sha256' in m, 'md5' in m, 'size_bytes' in m])

    js_id = m.get('js_id')
    assert len(js_id) > 0

    # NB: due to an error in processing, I had to throw away the db.analysis_content collection, so records may be missing. Sigh 8th June 2020
    if not force:
       byte_content_doc = db.analysis_content.find_one({ 'js_id': js_id })
       if fail_iff_not_found and byte_content_doc is None:   # prevent infinite recursion
           raise ValueError("No data for {}".format(js_id))
    else:
       byte_content_doc = None

    if byte_content_doc is None:
        code_bytes, js_id = get_script(db, js_id)
        assert code_bytes is not None
        jsr = JavascriptArtefact(url=m.get('url'), sha256=m.get('sha256'), md5=m.get('md5'), size_bytes=m.get('size_bytes'), js_id=js_id)
        vector_as_bytes, failed, stderr = analyse_script(code_bytes, jsr, java=java, feature_extractor=extractor)
        if failed:
            raise ValueError("Could not analyse artefact: js_id={}\n{}".format(js_id, stderr))
        save_analysis_content(db, jsr, vector_as_bytes)

        if defensive:
            # check that artefact hashes match the actual content 
            assert hashlib.sha256(code_bytes).hexdigest() == m.get('sha256')
        return find_or_update_analysis_content(db, m, fail_iff_not_found=True) # this time it should be found!

    assert 'analysis_bytes' in byte_content_doc
    byte_content = byte_content_doc.get('analysis_bytes')
    assert isinstance(byte_content, bytes)
    return json.loads(byte_content.decode())

def save_artefact(db, artefact, root, content=None, inline=False, content_type='text/javascript', defensive=False):
   """
   Saves the content to Mongo as specified by root/artefact.path and then the record of the save to Kafka (topic to) for downstream processing
   """
   assert db is not None
   assert content is not None or (root is not None and artefact.path is not None)
 
   if content is None: 
        path = "{}/{}".format(root, artefact.path)
        content = None
        with open(path, 'rb') as fp:
            content = fp.read()
            assert isinstance(content, bytes)

   d, js_id, was_cached = save_script(db, artefact, content)
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
   return (d, was_cached)

def save_analysis_content(db, jsr: JavascriptArtefact, bytes_content: bytes, ensure_indexes=False, iff_not_exists=False):
   assert bytes_content is not None
   js_id = jsr.js_id
   assert len(js_id) > 0
   assert isinstance(bytes_content, bytes) # to ensure correct behaviour

   if ensure_indexes:
       db.analysis_content.create_index([( 'js_id', ASCENDING ) ], unique=True)

   d = asdict(jsr)
   d.update({ "analysis_bytes": Binary(bytes_content), 'last_updated': datetime.utcnow() })

   # only perform and update if no existing analysis?  
   if iff_not_exists:
       # https://blog.serverdensity.com/checking-if-a-document-exists-mongodb-slow-findone-vs-find/ 
       # dont do the update below if the analysis content already exists...
       cursor = db.collection.find({'_id': ObjectId(js_id) }, {'_id': 1}).limit(1)
       if cursor is not None: # data exists, so we dont update
           return

   # lookup artefact by ID - altered content will get a new ID as long as its sha256 hash isnt already known
   ret = db.analysis_content.find_one_and_update({ "js_id": js_id }, { "$set": d }, upsert=True)

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
   assert result is not None
   return result.inserted_id

def save_script(db, artefact: DownloadArtefact, script: bytes):
   # NB: we work hard here to avoid mongo calls which will cause performance problems (hashing too)
   assert isinstance(artefact, DownloadArtefact)
   assert len(artefact.checksum) == 32  # length of an md5 hexdigest

   # 0. compute hashes to search for
   sum256 = hashlib.sha256(script).hexdigest()
   sum5 = hashlib.md5(script).hexdigest()
   if sum5 != artefact.checksum:
       raise ValueError("Expected MD5 and MD5 hash do not match: {} {} != {}".format(artefact.url, sum5, artefact.checksum))

   # 1. save the url for this artefact
   url_id = save_url(db, artefact)

   script_len = len(script)
   assert len(sum256) > 0
   assert len(sum5) > 0

   key = { 'sha256': sum256, 'md5': sum5, 'size_bytes': script_len }  
   value = key.copy() # NB: shallow copy is sufficient for this application
   value.update({ 'code': Binary(script) })

   # NB: to fix a data corruption bug from May 2020 - we implement this as follows:
   # 1) call find_one()
   # 2) if we find an existing record its sha256, md5 and size (in bytes) are validated
   #    a) if valid, we return this js_id
   #    b) if not valid, we create a new document and return that, reporting a corrupt ID
   # 3) if nothing is found in step (1) we insert a new document
   # In this way the corrupt records are left untouched and new code will just refer to clean records. A separate program will cleanup corrupt records.
   s = db.scripts.find_one(key)
   invalid_record = False
   is_cached = False
   if s is not None:
       assert '_id' in s
       assert 'code' in s
       code = s.get('code')
       #print("actual bytes={} md5={} sha256={}".format(script_len, sum5, sum256))
       on_disk256 = hashlib.sha256(code).hexdigest()
       on_disk5 = hashlib.md5(code).hexdigest()
       #print("expected bytes={} md5={} sha256={}".format(s.get('size_bytes'), on_disk5, on_disk256))
       invalid_record = any([ s.get('size_bytes') != script_len,
                              on_disk256 != sum256,
                              on_disk5 != sum5 ])
       if invalid_record:
           print("WARNING: corrupt db.scripts record seen: JS ID {} - will create new record for new data.".format(s['_id']))
           # NB: keep track of how often we do this, to spot problematic records which for some reason are being "fixed" multiple times...
           db.analysis_content.find_and_update_one({ 'js_id': s['_id'] }, { "$inc": { "corruption_fix_count": 1 } })
       else:
           #print("Using existing record for sha256={}: JS ID {}".format(sum256, s['_id']))
           is_cached = True
       # FALLTHRU...

   # 3. persist a new db.scripts record?
   if s is None or invalid_record:
       assert len(value['md5']) > 0
       assert len(value['sha256']) > 0
       assert value['size_bytes'] >= 0  # should we permit zero-byte scripts (yes... they do happen!)
       assert isinstance(value['code'], bytes)
       ret = db.scripts.insert_one(value)
       assert ret is not None 
       s = { '_id': ret.inserted_id } # convert into something that code below can use...
       # FALLTHRU since we've now inserted... and there is nothing to validate for newly inserted scripts (rare once you've done a lot of crawling)

   # 4. and finally ensure the url -> script record is present also
   js_id = str(s.get('_id'))
   ret = db.script_url.insert_one({ 'url_id': url_id, 'script': js_id })
   assert ret is not None 
   assert len(js_id) > 0
   return (key, js_id, is_cached)

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

   sha256 = hashlib.sha256(content).hexdigest()
   md5 = hashlib.md5(content).hexdigest()
   jsr = JavascriptArtefact(when=str(datetime.utcnow()), sha256=sha256, md5=md5 , url=url,
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
   cntrl_url, subfamily = identify_control_subfamily(jsr.url)
   ret.update({ 'family': family, 'release': version, 'variant': variant, 'origin': url, 'sha256': sha256, 'md5': md5, 'size_bytes': len(content),
                'do_not_load': False,  # all controls loaded by default except alpha/beta/release candidate
                'provider': provider, 'subfamily': subfamily })
   #print(ret)
   assert 'sha256' in ret
   assert 'md5' in ret
   assert 'size_bytes' in ret

   # NB: only one control per url/family pair (although in theory each CDN url is enough on its own)
   resp = db.javascript_controls.find_one_and_update({ 'origin': url, 'family': family },
                                                     { "$set": ret }, upsert=True)
   db.javascript_control_code.find_one_and_update({ 'origin': url },
                                                     { "$set": { 'origin': url, 'code': Binary(content), 
                                                       'analysis_bytes': bytes_content, 
                                                       "last_updated": jsr.when } }, upsert=True)
   update_control_summary(db, url, 
                          ret['statements_by_count'], 
                          ret['calls_by_count'],
                          ret['literals_by_count'])
   return jsr

def update_control_summary(db, url, ast_vector, function_call_vector, literal_vector, defensive=False):
   assert isinstance(url, str) and url.startswith("http")

   vector, total_sum = calculate_ast_vector(ast_vector)
   if defensive:
       assert total_sum >= 50   # vectors smaller than this are too small to match accurately - and may indicate an issue with the download/code
   sum_of_function_calls = sum(function_call_vector.values())
   sum_of_literals = sum(literal_vector.values())
   vs = JavascriptVectorSummary(origin=url, 
                                 sum_of_ast_features=total_sum,
                                 sum_of_functions=sum_of_function_calls, 
                                 sum_of_literals=sum_of_literals, 
                                 last_updated=str(datetime.utcnow()))
   ret = db.javascript_controls_summary.find_one_and_update({ 'origin': url }, { "$set": asdict(vs) }, upsert=True)

def load_controls(db, min_size=1500, all_vectors=False, verbose=False):
   # NB: vectors come from javascript_control_code where integrity is better implemented
   n = 0
   for control in db.javascript_controls.find({ "size_bytes": { "$gte": min_size }, "do_not_load": False }, { 'literals_by_count': 0, 'calls_by_count': 0 }):
       ast_vector, ast_sum = calculate_ast_vector(control['statements_by_count'])
       
       if all_vectors: 
           bytes_content_doc = db.javascript_control_code.find_one({ 'origin': control['origin'] })
           if bytes_content_doc is None:
               print("WARNING: could not load code for {}".format(control['origin']))
               continue
           assert bytes_content_doc is not None
           assert 'analysis_bytes' in bytes_content_doc
           vectors = json.loads(bytes_content_doc.get('analysis_bytes'))
           assert vectors is not None and 'statements_by_count' in vectors
           tuple = (control, ast_sum, ast_vector, vectors['calls_by_count'], vectors['literals_by_count'])
       else:
           tuple = (control, ast_sum, ast_vector)
       yield tuple
       n += 1

   if verbose:
       print("Loaded {} controls, each at least {} bytes".format(n, min_size))
