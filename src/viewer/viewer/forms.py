from django import forms
from django.core.exceptions import ValidationError

def is_not_blank(value):
    if value == None or len(value) < 1 or len(value.strip()) < 1:
        raise ValidationError("Invalid value - cannot be blank")

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
