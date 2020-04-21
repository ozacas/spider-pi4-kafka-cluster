from utils.features import safe_for_mongo
from utils.models import JavascriptArtefact

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

