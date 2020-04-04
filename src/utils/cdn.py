import json
import requests

class CDNJS:
   """
   Access the Cloudflare-run JS CDN to retrieve reputable copies of major JS packages
   """
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

   def is_i18n(self, ret):
      url = ret[0].lower()
      return '/i18n/' in url or '/lang/' in url

   def fetch(self, family, variant, version, ignore_i18n=True):
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
                 ret = ("{}{}/{}/{}".format(self.cdn, family, version, file), family, variant, version)
             else:
                 ret = ("{}{}/{}/{}".format(self.cdn, family, asset['version'], file), family, variant, asset['version'])
             if ignore_i18n and self.is_i18n(ret):
                 pass
             else:
                 yield ret

class JSDelivr:
   def __init__(self, *args, **kwargs):
      pass

   def fetch(self, family, variant, version, ignore_i18n=True):
      pass
