from django.contrib import admin
from .models import *

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
    exclude = ( 'code', ) # DONT load this as it will be VERY slow due to the amount of data 
    list_display = ['size_bytes', 'sha256', 'md5', '_id']
    ordering = [ 'size_bytes' ]

class ControlHitAdmin(admin.ModelAdmin):
    pass

class FeatureVectorAdmin(admin.ModelAdmin):
    list_display = ['size_bytes', 'sha256', 'origin', 'url']
    ordering = ['size_bytes', 'origin']

class VetAgainstControlAdmin(admin.ModelAdmin):
    pass

admin.site.register(JavascriptControl, JavascriptControlAdmin)
admin.site.register(BlacklistedDomain, BlacklistedDomainAdmin)
admin.site.register(JavascriptControlSummary, JavascriptControlSummaryAdmin)
admin.site.register(Script, ScriptAdmin)
admin.site.register(ControlHit, ControlHitAdmin)
admin.site.register(FeatureVector, FeatureVectorAdmin)
admin.site.register(VetAgainstControl, VetAgainstControlAdmin)
