from django.views.generic import ListView
from django.db.models import Max, StdDev, Count
from viewer.models import ControlHit

class StatsByControlView(ListView):
   template_name = 'stats_control_summary.html'
   ordering = ['-n', '-max_ast_dist', 'control_url'] # ignored by the mongo query but keeps pagination happy about what is going on
   model = ControlHit
   paginate_by = 100

   # template wants a dict to work with so...
   def as_dict(self, dict_of_tuples):
      d = { key:val for key, val in dict_of_tuples.items() }
      return d
 
   def get_queryset(self, **kwargs): 
      qs = ControlHit.objects.mongo_aggregate([{ '$group': {
                                                        '_id':  "$control_url",
                                                        'set_of_xref': { '$addToSet': '$xref' },
                                                        'max_ast_dist': { '$max': "$ast_dist" },
                                                        'max_literal_dist': { '$max': "$literal_dist" },
                                                        'max_diff_literals': { '$max': "$n_diff_literals" },
                                                        'stddev_ast_dist': { '$stdDevSamp': "$ast_dist" },
                                                 }},
                                                { '$project': {
                                                        'n': { '$size': '$set_of_xref' },
                                                        'max_ast_dist': 1,
                                                        'max_literal_dist': 1,
                                                        'max_diff_literals': 1,
                                                        'stddev_ast_dist': 1,
                                                        'control_url': '$_id',
                                                }},
                                                { '$match': { 'n': { '$gte': 50 } } },  # require at least 50 samples for meaningful stats
                                                { '$project': {'set_of_xref': 0, '_id': 0 }},
                                                { '$sort': { 'n': -1, 'max_ast_dist': -1, 'control_url': 1 } },
                                               ], allowDiskUse=True)
      qs = [self.as_dict(r) for r in qs]
      return qs

by_control_stats = StatsByControlView.as_view()
