import django.db.models as m
from djongo.models import ObjectIdField
from djongo.models.json import JSONField

class JavascriptControl(m.Model):
   _id = ObjectIdField(db_column='_id')
   family = m.CharField(blank=False, null=False, max_length=100)
   origin = m.CharField(blank=False, null=False, max_length=1024)
   do_not_load = m.BooleanField()
   size_bytes = m.IntegerField()
   variant = m.CharField(max_length=100)
   release = m.CharField(max_length=100)
   content_type = m.CharField(max_length=100)
   provider = m.CharField(max_length=100)
   subfamily= m.CharField(max_length=255) 

   # responsibility for this model is up to etl_control_upload*.py so...
   class Meta:
       db_table = 'javascript_controls'
       managed = False

class BlacklistedDomain(m.Model):
   _id = ObjectIdField(db_column='_id')
   domain = m.CharField(blank=False, null=False, max_length=255)
   reason = m.CharField(blank=False, null=False, max_length=255)

   # responsibility for this model is not with Django so...
   class Meta:
      db_table = 'blacklisted_domains'
      managed = False

#{ "_id" : ObjectId("5e89571bce9013c5bcdfe59e"), "origin" : "https://cdnjs.cloudflare.com/ajax/libs/angular.js/2.0.0-beta.16/router.min.js", "last_updated" : "2020-06-12 20:37:01.214068", "sum_of_ast_features" : 19875, "sum_of_functions" : 1144, "sum_of_literals" : 976 }
class JavascriptControlSummary(m.Model):
   _id = ObjectIdField(db_column='_id')
   origin = m.CharField(blank=False, null=False, max_length=255) # lookup into JavascriptControl model, although do_not_load=True may be in effect
   last_updated = m.CharField(blank=False, null=False, max_length=64)
   sum_of_ast_features = m.IntegerField()
   sum_of_functions = m.IntegerField()
   sum_of_literals = m.IntegerField() 

   # responsibility for this model is up to etl_control_upload*.py so...
   class Meta:
       db_table = "javascript_controls_summary"
       managed = False

class Script(m.Model):
   _id = ObjectIdField(db_column='_id')
   sha256 = m.CharField(blank=False, null=False, max_length=128, editable=False)
   md5 = m.CharField(blank=False, null=False, max_length=64, editable=False)
   size_bytes = m.IntegerField(editable=False)
   #code = m.BinaryField()  # not editable by default

   # responsibility for this model is up to etl_upload_artefacts.py
   class Meta:
       db_table = "scripts"
       managed = False

class FeatureVector(m.Model):
   # { "_id" : ObjectId("5ee1f3b8a4b98699c0917c8f"), "js_id" : "5e3cff0baf4b1411a1429ff4", "byte_content_sha256" : "bae214050996634a9a61dae7e12a3afd3fed131db5d90a14abfa66b49dcf7eea", "content_type" : "text/javascript", "inline" : false, "last_updated" : ISODate("2020-06-22T06:37:34.977Z"), "md5" : "2882cbfe23dc5802cf598e7a20409304", "origin" : "https://clubs.canberra.edu.au/Account/Register", "sha256" : "ff0ada03194dbd93b98c88e54b6e1637c2b4b4458907401f869187d90efd08ba", "size_bytes" : 23828, "url" : "https://clubs.canberra.edu.au/Scripts/expressive.annotations.validate.js", "when" : "2020-06-21 22:42:10.323637" }
   _id = ObjectIdField(db_column='_id')
   js_id = m.ForeignKey(Script, on_delete=m.DO_NOTHING)
   content_type = m.CharField(max_length=100)
   inline = m.BooleanField()
   last_updated = m.DateTimeField()
   md5 = m.CharField(blank=False, null=False, max_length=64) 
   origin = m.CharField(max_length=1024)
   sha256 = m.CharField(blank=False, null=False, max_length=128)
   size_bytes = m.IntegerField()
   url = m.CharField(max_length=1024)
   when = m.CharField(max_length=64) 
   byte_content = m.BinaryField()

   class Meta:
      db_table = "analysis_content"
      managed = False
 
class VetAgainstControl(m.Model):
   # unlike ControlHit which is the "best" hit for a given control-origin pair, this is a point-in-time datapoint
   # { "_id" : ObjectId("5ecb5cbed7161ef4963d4498"), "origin_url" : "https://www.playfootball.com.au/sites/play/files/js/js_BaupD1b1RyIB49fG7a7PVMg8ZvbSJmHIlTWnZmKr9L8.js", "ast_dist" : Infinity, "cited_on" : "https://www.playfootball.com.au/?id=172", "control_url" : "", "diff_functions" : "", "diff_literals" : "", "function_dist" : Infinity, "literal_dist" : 0, "literals_not_in_control" : -1, "literals_not_in_origin" : -1, "n_diff_literals" : -1, "origin_js_id" : "5e8da771af53ffdf7ee0b88b", "sha256_matched" : false, "xref" : null, "origin_vectors_sha256" : "8edf3e27259d16c1430b28ee2ad609c0ebda25626d089f4fcc35e6e6911bee0c" }
   _id = ObjectIdField(db_column='_id')
   origin_url = m.CharField(max_length=1024)
   ast_dist = m.FloatField()
   literal_dist = m.FloatField()
   literals_not_in_control = m.IntegerField()
   literals_not_in_origin = m.IntegerField()
   n_diff_literals = m.IntegerField()
   origin_js_id = m.ForeignKey(Script, on_delete=m.DO_NOTHING)
   sha256_matched = m.BooleanField()
   xref = m.ForeignKey(FeatureVector, on_delete=m.DO_NOTHING)

   class Meta:
       db_table = "vet_against_control"
       managed = False

#{ "_id" : ObjectId("5ed89c9db200f2e3a8d64a42"), "control_url" : "https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.19/js/jquery.dataTables.min.js", "origin_url" : "https://cdn.datatables.net/1.10.19/js/jquery.dataTables.min.js", "sha256_matched" : true, "ast_dist" : 0, "function_dist" : 0, "cited_on" : "https://www.scenicrim.qld.gov.au/our-community/libraries", "origin_js_id" : "5e3c98be744c6d2310447e8c", "literal_dist" : 0, "xref" : "5ecb9b9cd7161ef4964bb995", "literals_not_in_control" : 0, "literals_not_in_origin" : 0, "n_diff_literals" : 0, "diff_literals" : "", "origin_vectors_sha256" : "38379043c0037e3587aaf463a2822964dbf10491489c5040fdc3db053823bae7", "origin_host" : "cdn.datatables.net", "origin_has_query" : false, "origin_port" : 443, "origin_scheme" : "https", "origin_path" : "/1.10.19/js/jquery.dataTables.min.js", "cited_on_host" : "www.scenicrim.qld.gov.au", "cited_on_has_query" : false, "cited_on_port" : 443, "cited_on_scheme" : "https", "cited_on_path" : "/our-community/libraries", "control_family" : "datatables", "diff_functions" : [ ] }
class ControlHit(m.Model):
   _id = ObjectIdField(db_column='_id')
   control_url = m.CharField(blank=False, null=False, max_length=1024)
   origin_url = m.CharField(blank=False, null=False, max_length=1024)
   sha256_matched = m.BooleanField()
   ast_dist = m.FloatField()
   function_dist = m.FloatField()
   literal_dist = m.FloatField()
   cited_on = m.CharField(blank=False, null=False, max_length=1024)
   origin_js_id = m.ForeignKey(Script, on_delete=m.DO_NOTHING)
   xref = m.ForeignKey(VetAgainstControl, on_delete=m.DO_NOTHING)
   literals_not_in_control = m.IntegerField()
   literals_not_in_origin = m.IntegerField()
   n_diff_literals = m.IntegerField()
   diff_literals = m.CharField(null=False, max_length=1000000)
   origin_host = m.CharField(max_length=255)
   origin_has_query = m.BooleanField()
   origin_port = m.IntegerField()
   origin_scheme = m.CharField(max_length=10)
   origin_path = m.CharField(max_length=1024)
   cited_on_host = m.CharField(max_length=255)
   cited_on_has_query = m.BooleanField()
   cited_on_port = m.IntegerField()
   cited_on_scheme = m.CharField(max_length=10)
   cited_on_path = m.CharField(max_length=1024)
   control_family = m.CharField(max_length=255)
   diff_functions = JSONField()

   # responsibility for this data is left entirely to etl_publish_hits.py
   class Meta:
       db_table = "etl_hits"
       managed = False
