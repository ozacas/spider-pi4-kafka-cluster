from utils.features import safe_for_mongo
from utils.models import JavascriptArtefact
from collections import namedtuple
import pymongo
import hashlib

def save_ast_vector(db, jsr: JavascriptArtefact, ast_vector, js_id: str=None):
   assert ast_vector is not None
   d = { "js_id": js_id, "url": jsr.url, "origin": jsr.origin }
   d.update(**ast_vector)  # ast_vector never needs to be made safe for Mongo, since its just mozilla rhino statement types for keys
   d.update({ "js_id": js_id })
   assert '_id' not in d.keys()
   db.statements_by_count.insert_one(d)

def save_call_vector(db, jsr: JavascriptArtefact, call_vector, js_id: str=None):
   assert call_vector is not None
   d = { "js_id": js_id, "url": jsr.url, "origin": jsr.origin }
   calls = safe_for_mongo(call_vector)
   d['calls'] = calls
   d.pop('_id', None) # BUG: FIXME -- sometimes it appears to be present, so... maybe topic has a bit of pollution during dev???
   db.count_by_function.insert_one(d)

def next_artefact(iterable, max: float=float('Inf'), verbose: bool=False, filter_cb: callable=None):
    n = 0
    for message in filter(lambda m: filter_cb is None or filter_cb(m.value), iterable):
        v = message.value
        yield v
        n += 1
        if verbose and n % 10000 == 0:
            print("Processed {} records.".format(n))
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
                                      projection={ 'code': False, '_id': True },
                                      return_document=pymongo.ReturnDocument.AFTER)
   db.script_url.insert_one( { 'url_id': url_id, 'script': s.get(u'_id') })

   return key
