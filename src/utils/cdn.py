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

   def fetch(self, family, variant, version, ignore_i18n=True, provider=None):
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
#            print(version, asset['version'])
             if version is not None:
                 if asset['version'] == version:
                     ret = ("{}{}/{}/{}".format(self.cdn, family, version, file), family, variant, version, provider)
                 else:
                     continue
             else:
                 ret = ("{}{}/{}/{}".format(self.cdn, family, asset['version'], file), family, variant, asset['version'], provider)
             if ignore_i18n and self.is_i18n(ret):
                 pass
             else:
                 yield ret

class JSDelivr:
   def __init__(self, *args, **kwargs):
      self.base = "https://data.jsdelivr.com/v1/package"
      self.cdn_base = "https://cdn.jsdelivr.net/{}/{}@{}{}" # package_type, family, version_spec, file path
      pass

   def suitable_variant(self, variant, name):
      if variant is None or len(variant) < 1:
          return True
      return variant in name

   def suitable_version(self, reported_version, wanted_version):
      if wanted_version is None:
          return True # all versions suitable
      #print(reported_version+ " " + wanted_version)
      return wanted_version == reported_version

   def fetch(self, family, variant, version, ignore_i18n=True, provider=None):
      # 1. compute the package type since the API doesnt provide it: TODO FIXME - risky if malicious package with same family? Nah...
      package_type = None
      for type in ['gh', 'npm']:
          reported_versions = "{}/{}/{}".format(self.base, type, family)
          resp = requests.get(reported_versions)
          if resp.status_code == 200:
             j = resp.json()
             package_type = type
             available_versions = [v for v in j['versions'] if self.suitable_version(v, version)]
             break

      # 2. get all available versions and obtain JS artefacts for caller to process
      if package_type:
          for v in available_versions:
              resp = requests.get("{}/{}/{}@{}/flat".format(self.base, package_type, family, v))
              if resp.status_code == 200: # ignore un-available versions
                 j = resp.json()
                 for artefact in filter(lambda a: a['name'].endswith(".js") and self.suitable_variant(variant, a['name']), j['files']): 
                     path = artefact['name']      # NB: ensure exactly one slash between version and path, as it will 404 if too many...
                     if not path.startswith("/"):
                         path = "/" + path
                     ret = (self.cdn_base.format(package_type, family, v, path), family, variant, v, provider)
                     yield ret
          return
      # failure 
      return []
