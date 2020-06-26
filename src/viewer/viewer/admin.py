from django.contrib import admin
from .models import *
from django.urls import reverse
from django.utils.html import format_html
from bson.objectid import ObjectId
import json

class JavascriptControlAdmin(admin.ModelAdmin):
    list_display = [ 'family', 'subfamily', 'provider', 'origin', '_id' ]
    ordering = ['family', 'subfamily', 'origin']

class BlacklistedDomainAdmin(admin.ModelAdmin):
    list_display = [ 'domain', 'reason' ]
    ordering = [ 'domain' ]

class JavascriptControlSummaryAdmin(admin.ModelAdmin):
    list_display = [ 'sum_of_ast_features', 'sum_of_functions', 'sum_of_literals', 'origin', '_id']
    ordering = ['sum_of_ast_features', 'sum_of_functions']

class ScriptAdmin(admin.ModelAdmin):
    exclude = ( 'code', )
    list_display = ['size_bytes', 'sha256', 'md5', '_id']
    # NB: cant specify ordering due to expense in sorting after loading all code objects...
    #ordering = [ 'size_bytes' ]
    list_per_page = 20
    readonly_fields = ["sha256", "md5", "size_bytes", "code"]
    search_fields = ( 'sha256', 'size_bytes', 'md5', )

class ETLHitAdmin(admin.ModelAdmin):
    list_display = ['control_url', 'origin_url', 'sha256_matched', 'ast_dist', 'function_dist', 'literal_dist', 'literals_not_in_control', 'literals_not_in_origin', 'n_diff_literals', 'more_info', 'script_info' ]
    #ordering = [ 'ast_dist', 'function_dist' ]
    search_fields = ( 'control_url', )

    def script_info(self, obj):
        val = getattr(obj, 'origin_js_id', None)
        if val is None: 
            return ''
        link = reverse("admin:viewer_script_change", args=(val,))
        return format_html('<a href="{}">Script</a>', link)

    def more_info(self, obj):
        val = getattr(obj, 'xref', None)
        if val is None:
            return ''
        link = reverse("admin:viewer_vetagainstcontrol_change", args=(val,))
        return format_html('<a href="{}">Hit</a>', link) 

    more_info.short_description = 'Hit'
    script_info.short_description = 'Script'

class FeatureVectorAdmin(admin.ModelAdmin):
    list_display = ['size_bytes', 'sha256', 'origin', 'url', 'md5', 'vectors']
    search_fields = ( 'sha256', 'md5', )
    readonly_fields = ( 'sha256', 'md5', 'origin', 'url', 'size_bytes', )
    list_per_page = 20
   
    def vectors(self, obj):
       val = getattr(obj, 'analysis_bytes', None)
       if not isinstance(val, bytes):
          return ''
       vectors = json.loads(val.decode('utf-8'))
       ast_vector = str(vectors.get('statements_by_count'))
       literal_vector = str(vectors.get('literals_by_count'))
       fcall_vector = str(vectors.get('calls_by_count'))
       return format_html("<table><tr><th>Vector</th><th>Values</th></tr>" + 
                                 "<tr><td>AST Features</td><td>{}</td></tr>" + 
                                 "<tr><td>Literals</td><td>{}</td></tr>" +
                                 "<tr><td>Function calls</td><td>{}</td></tr></table>", ast_vector, literal_vector, fcall_vector)

class VetAgainstControlAdmin(admin.ModelAdmin):
    list_display = [  'ast_dist', 'function_dist', 'literal_dist', 'literals_not_in_control', 'literals_not_in_origin', 
                      'sha256_matched', 'control_url', 'origin_url' ]
    list_display_links = None
    ordering = ( '-literal_dist', '-function_dist', )
    search_fields = ( 'control_url', 'origin_url', )

admin.site.register(JavascriptControl, JavascriptControlAdmin)
admin.site.register(BlacklistedDomain, BlacklistedDomainAdmin)
admin.site.register(JavascriptControlSummary, JavascriptControlSummaryAdmin)
admin.site.register(Script, ScriptAdmin)
admin.site.register(ControlHit, ETLHitAdmin)
admin.site.register(FeaturesModel, FeatureVectorAdmin)
admin.site.register(VetAgainstControl, VetAgainstControlAdmin)
