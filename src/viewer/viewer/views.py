from django.views.generic import FormView, TemplateView
from django.http import HttpResponseRedirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic.list import MultipleObjectTemplateResponseMixin, MultipleObjectMixin
from pymongo import ASCENDING, DESCENDING
from django.shortcuts import get_object_or_404, render
from django.contrib import messages
from viewer.models import ControlHit
from viewer.forms import HostSearchForm, ControlSearchForm, DistanceSearchForm, RecentSearchForm, FunctionSearchForm

class SearchMixin:
    template_name = "search_form.html"
    paginate_by = 100
    ordering = [ '-function_dist' ] # needed to ensure pagination is happy with contents not shifting during pagination
    model = ControlHit
    object_list = ControlHit.objects.none()

    def get(self, request, *args, **kwargs):
       # need to subclass this method to ensure pagination works correctly (as 'next', 'last' etc. is GET not POST) 
       d = {}
       key = self.__class__.__name__
       print("Updating session state: {}".format(key))
       d.update(request.session.get(key, {})) # update the form to the session state
       return self.update_form(d)

    def update_form(self, form_values):
       assert isinstance(form_values, dict)
       # apply the form settings to self.queryset (specific to a CBV - watch for subclass overrides)
       self.object_list = self.get_queryset(**form_values)
       state_field = self.__class__.__name__  # NB: must use class name so that each search type has its own state for a given user
       self.request.session[state_field] = form_values
       context = self.get_context_data()
       assert self.action_url is not None
       context['action_url'] = self.action_url
       assert context is not None
       self.form = self.form_class(initial=form_values) 
       context['form'] = self.form
       return self.render_to_response(context)

    def form_invalid(self, form):
       return self.update_form(form.cleaned_data)
 
    # this is called from self.post()    
    def form_valid(self, form):
       assert form.is_valid()
       return self.update_form(form.cleaned_data)

class SearchView(LoginRequiredMixin, SearchMixin, MultipleObjectMixin, MultipleObjectTemplateResponseMixin, FormView):
    form_class = HostSearchForm
    action_url = '/search/host/' # NB: must end in '/'

    def get_queryset(self, **kwargs):
       if kwargs == {}:
          return self.model.objects.none()

       qs = super().get_queryset()
       want_partial = kwargs.get('partial', False)
       want_case = kwargs.get('case_sensitive', False)
       if 'host' in kwargs and len(kwargs['host'].strip()) > 0:
           if want_partial:
               if want_case:
                   qs = qs.filter(cited_on_host__contains=kwargs['host'])
               else:
                   qs = qs.filter(cited_on_host__icontains=kwargs['host'])
           else:
               qs = qs.filter(cited_on_host=kwargs['host'])
       qs = qs.order_by(*self.ordering)
       return qs

class ControlSearchView(LoginRequiredMixin, SearchMixin, MultipleObjectMixin, MultipleObjectTemplateResponseMixin, FormView):
    form_class = ControlSearchForm
    action_url = '/search/control/'

    def get_queryset(self, **kwargs):
       # NB: do not call the superclass since it assumes form parameters which do not exist via kwargs
       if kwargs == {}:
           return ControlHit.objects.none()
       qs = super().get_queryset()
       control_url = kwargs.get('control_url', None)
       partial = kwargs.get('partial', False)
       if control_url is None or len(control_url.strip()) < 1:  # should not happen given form validation, but...
           return ControlHit.objects.none()
       if not partial:
           qs = qs.filter(control_url=control_url)
       else:
           qs = qs.filter(control_url__contains=control_url)
       qs = qs.order_by(*self.ordering)
       return qs

class DistanceSearchView(LoginRequiredMixin, SearchMixin, MultipleObjectMixin, MultipleObjectTemplateResponseMixin, FormView):
    form_class = DistanceSearchForm
    action_url = '/search/distance/'

    def get_queryset(self, **kwargs):
       if kwargs == {}:
           return ControlHit.objects.none()

       qs = super().get_queryset()
       min_ast_dist = kwargs.get('min_ast_dist', 0.0)
       max_ast_dist = kwargs.get('max_ast_dist', float('Inf'))
       min_fcall_dist = kwargs.get('min_fcall_dist', 0.0)
       max_fcall_dist = kwargs.get('max_fcall_dist', float('Inf'))
       min_diff_literals = int(kwargs.get('min_diff_literals', 0.0))
       max_diff_literals = int(kwargs.get('max_diff_literals', float('Inf')))
       qs = qs.filter(ast_dist__gte=min_ast_dist)
       qs = qs.filter(ast_dist__lt=max_ast_dist)
       qs = qs.filter(function_dist__gte=min_fcall_dist)
       qs = qs.filter(function_dist__lt=max_fcall_dist)
       qs = qs.filter(n_diff_literals__gte=min_diff_literals)
       qs = qs.filter(n_diff_literals__lt=max_diff_literals)
       return qs

class RecentSearchView(LoginRequiredMixin, SearchMixin, MultipleObjectMixin, MultipleObjectTemplateResponseMixin, FormView):
    form_class = RecentSearchForm
    action_url = '/search/recent/'
    ordering = ['control_url', '-function_dist']

    def get_queryset(self, **kwargs):
       if kwargs == {}:
           return ControlHit.objects.none()
       from datetime import datetime, timedelta
       from bson.objectid import ObjectId
       gen_time = datetime.utcnow() - timedelta(days=kwargs.get('n_days', 48)) 
       dummy_id = ObjectId.from_datetime(gen_time)
       # NB: "raw" call to mongo via djongo - note mongo_ prefix!
       filters = { '_id': { "$gte": dummy_id } }
       if 'exclude_hash_matches' in kwargs:
            filters.update({ 'sha256_matched': False })
       if 'min_ast_dist' in kwargs:
            filters.update({ 'ast_dist': { '$gte': kwargs.get('min_ast_dist') } })
       if 'min_diff_literals' in kwargs:
            filters.update({ 'n_diff_literals': { '$gte': kwargs.get('min_diff_literals') } })
       #print(filters)
       results = ControlHit.objects.mongo_find(filters)
       wanted_pks = set([r.get('_id') for r in results])
       print("Found {} records to report for {} since {} {}".format(len(wanted_pks), self.__class__.__name__, str(dummy_id), str(gen_time)))
       qs = ControlHit.objects.filter(_id__in=wanted_pks) # return a QuerySet with the desired hits which is the cleanest way atm...
       qs = qs.order_by(*self.ordering)
       return qs

class FunctionSearchView(LoginRequiredMixin, SearchMixin, MultipleObjectMixin, MultipleObjectTemplateResponseMixin, FormView):
    form_class = FunctionSearchForm
    template_name = "function_search_form.html"
    action_url = '/search/function/'
    ordering = ['origin_url', '-function_dist', '-ast_dist'] # keep pagination happy, but not used by get_queryset()

    def as_dict(self, dict_as_tuples):
       d = { k:v for k,v in dict_as_tuples.items() if k in ('diff_functions', 'diff_literals', 'xref', 'origin_url', 'control_url', 'cited_on') }
       return d

    def get_queryset(self, **kwargs):
       if kwargs == {}:
           return ControlHit.objects.none()
       assert 'function' in kwargs and len(kwargs['function']) > 0
       results = ControlHit.objects.mongo_find({ "diff_functions": kwargs.get('function') })
       results = results.sort([ ('cited_on_host', ASCENDING,), ('ast_dist', DESCENDING,), ('function_dist', DESCENDING,) ])
       qs = [self.as_dict(r) for r in results]
       return qs

index = TemplateView.as_view(template_name="index.html")
host_search = SearchView.as_view()
control_search = ControlSearchView.as_view()
distance_search = DistanceSearchView.as_view()
recent_search = RecentSearchView.as_view()
function_search = FunctionSearchView.as_view()
