import subprocess
import json
import os
from dataclasses import asdict
from tempfile import NamedTemporaryFile

def analyse_script(js, jsr, producer=None, java='/usr/bin/java', feature_extractor="/home/acas/src/pi-cluster-ansible-cfg-mgmt/src/extract-features.jar"):
   # save code to a file
   tmpfile = NamedTemporaryFile(delete=False)
   tmpfile.write(js)
   tmpfile.close()

   url = jsr.url
   # save to file and run extract-features.jar to identify the javascript features
   process = subprocess.run([java, "-jar", feature_extractor, tmpfile.name, url], capture_output=True)

   # turn process stdout into something we can save
   ret = None
   d = asdict(jsr)
   if process.returncode == 0:
       ret = json.loads(process.stdout)
       ret.update(d)       # will contain url key
       ret.pop('id', None) # remove duplicate url entry silently
       # since $ for fields is not permissable in mongo, we replace with F$ 
       function_vector = ret.pop('calls_by_count') 
       d = {}
       for k, v in function_vector.items():
            if k.startswith('$'):
                k = "F"+k
            d[k] = v
       ret['calls_by_count'] = d
   elif producer:
       producer.send("feature-extraction-failures", d)

   # cleanup
   os.unlink(tmpfile.name)

   return ret

