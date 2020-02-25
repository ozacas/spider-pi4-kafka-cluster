#!/usr/bin/python3
import json
import csv
import sys
from kafka import KafkaConsumer


def consumer():
    return KafkaConsumer('analysis-results', bootstrap_servers='kafka1', auto_offset_reset='earliest',
                          consumer_timeout_ms=1000, group_id=None, # ensure consumer does not wait once it consumes all messages
                          value_deserializer=lambda m: json.loads(m.decode('utf-8'))) # dont set group id, we dont want to keep an offset for this program

def identify_fields_in_results():
    fields = set()
    key_fn_freq = { }  # only what is seen in the queue from the first time it is seen
    # NB: only interested in AST statement counts for now
    keywords = ['elem', 'script', 'iframe', 'url', 'ajax', 'query']

    for message in consumer():
       d = message.value 
       fields.update(d['statements_by_count'].keys())
       # also keep track of function calls with various keywords in them
       fns = d['calls_by_count'].keys();
       for fn in fns:
           lfn = fn.lower()
           if any(k in lfn for k in keywords):
               if not fn in key_fn_freq:
                   key_fn_freq[fn] = 1
               else:
                   key_fn_freq[fn] = key_fn_freq[fn] + 1
    key_functions = set()
    for fn in key_fn_freq:
          if key_fn_freq[fn] > 1000:
              key_functions.add(fn)
    return (fields, key_functions)


if __name__ == "__main__":
    fields, wanted_functions = identify_fields_in_results()
    f = list(fields)
    f.append('url') 
    for fn in wanted_functions: 
       f.append(fn)
    with sys.stdout as fp:
        writer = csv.DictWriter(fp, delimiter='\t', fieldnames=f)
        writer.writeheader()
        for message in consumer(): 
            d = message.value['statements_by_count']
            d['url'] = message.value['id']
            fns = message.value['calls_by_count']
            wanted_fns = { k: fns[k] for k in wanted_functions if k in fns }
            d.update(wanted_fns)
            writer.writerow(d)
    exit(0)
