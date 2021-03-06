from django import forms
from django.core.exceptions import ValidationError

def is_not_blank(value):
    if value == None or len(value) < 1 or len(value.strip()) < 1:
        raise ValidationError("Invalid value - cannot be blank")

def at_least_one(value):
    if value < 1:
        raise ValidationError("Must be at least 1")

class HostSearchForm(forms.Form):
    host = forms.CharField(max_length=100, required=True, validators=[is_not_blank])
    case_sensitive = forms.BooleanField(required=False)
    partial = forms.BooleanField(required=False)

def validate_distance(value):
    if value is None or value < 0.0 or value > 10000.0:
        raise ValidationError("Invalid value {} - must be in range [0, 10000]".format(value))

class ControlSearchForm(forms.Form):
    control_url = forms.CharField(max_length=255, required=True, validators=[is_not_blank])
    partial = forms.BooleanField(required=False)

class DistanceSearchForm(forms.Form):
    min_ast_dist = forms.FloatField(validators=[validate_distance])
    max_ast_dist = forms.FloatField(validators=[validate_distance])
    min_fcall_dist = forms.FloatField(validators=[validate_distance])
    max_fcall_dist = forms.FloatField(validators=[validate_distance])
    min_diff_literals = forms.IntegerField(validators=[validate_distance])
    max_diff_literals = forms.IntegerField(validators=[validate_distance])

class RecentSearchForm(forms.Form):
    n_days = forms.IntegerField(validators=[at_least_one], required=True, initial=2)
    exclude_hash_matches = forms.BooleanField(required=False, initial=True)
    min_ast_dist = forms.FloatField(validators=[validate_distance], required=False, initial=10.0) 
    min_diff_literals = forms.IntegerField(validators=[validate_distance], required=False, initial=10)

class FunctionSearchForm(forms.Form):
    function = forms.CharField(max_length=100, required=True, initial='post')
