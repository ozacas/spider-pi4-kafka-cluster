#!/usr/bin/python3
import json
import csv
import sys


def identify_fields_in_results(json_filename):
    fields = set()
    key_fn_freq = { }  # only what is seen in the queue from the first time it is seen
    # NB: only interested in AST statement counts for now
    keywords = ['elem', 'script', 'iframe', 'url', 'ajax', 'query']

    with open(json_filename, 'r') as fp:
       for line in fp:
           d = json.loads(line)
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
    fields, wanted_functions = identify_fields_in_results(sys.argv[1])
    f = list(fields)
    f.append('url') 
    for fn in wanted_functions: 
       f.append(fn)
    with sys.stdout as fp:
        writer = csv.DictWriter(fp, delimiter='\t', fieldnames=f)
        writer.writeheader()
        with open(sys.argv[1], 'r') as fp:
            for line in fp:
                message = json.loads(line)
                d = message['statements_by_count']
                n_gt_zero = 0
                for k, v in d.items():
                    if v > 0:
                       n_gt_zero += 1
                if n_gt_zero == 0:
                    sys.stderr.write("Got all zero control - ignored: {}".format(d['url']))
                    continue
                d['url'] = "http://control/jquery.js?ver={}".format(message['id'])
                fns = message['calls_by_count']
                wanted_fns = { k: fns[k] for k in wanted_functions if k in fns }
                d.update(wanted_fns)
                writer.writerow(d)
    exit(0)
