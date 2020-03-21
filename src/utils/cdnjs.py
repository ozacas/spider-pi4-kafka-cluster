import json
import requests

class CDNJS:
   def __init__(self, *args, **kwargs):
      self.base = "https://api.cdnjs.com/libraries"
      self.cdn = "https://cdnjs.cloudflare.com/ajax/libs/"
      pass

   def suitable_variants(self, variant, asset):
      #print(variant)
      if variant is not None and variant.lower() in ('min', 'minimised', 'minimized'):
          variant = '.min.js'
      #print(variant)
      found = False
      matched_file = None
      if variant:
          return list(filter(lambda f: variant in f, asset['files']))
      return filter(lambda f: f.endswith('.js'), asset['files'])

   def fetch(self, family, variant, version):
      url = "{}/{}".format(self.base, family)
      resp = requests.get(url, headers={ 'Content-Type': 'application/json' }) 
      j = resp.json()
      if len(j.keys()) == 0:
         raise ValueError('No such family/variant/version {}/{}/{}'.format(family, variant, version))

      for asset in j['assets']:
          suitable_js = self.suitable_variants(variant, asset)
          for file in suitable_js:
             if not file.endswith(".js"):
                 continue
             if version is not None and (version and asset['version'] == version):
                 yield ("{}{}/{}/{}".format(self.cdn, family, version, file), family, variant, version)
             else:
                 yield ("{}{}/{}/{}".format(self.cdn, family, asset['version'], file), family, variant, asset['version'])
