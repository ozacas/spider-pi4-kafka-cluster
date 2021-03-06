#!/usr/bin/python3
import os
import math
import json
import argparse
import sys
import hashlib
from utils.features import analyse_script, calculate_ast_vector, calculate_vector, compute_distance, calc_function_dist, calculate_literal_distance
from utils.misc import *
from utils.models import JavascriptArtefact
try:
    from scipy import spatial
    has_scipy = True
except:
    has_scipy = False

# Usage: python3 calc_distance.py --file1 test-javascript/customize-preview.js --file2 test-javascript/customize-preview.min.js --extractor `pwd`/extract-features.jar
# which will compare a minimised JS artefact against a non-minified artefact and report distances

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
add_debug_arguments(a)
add_extractor_arguments(a)
a.add_argument("--file1", help="File 1 (javascript only)", type=str, required=True)
a.add_argument("--file2", help="File 2 (javascript only)", type=str, required=True)
args = a.parse_args()

def calc_vector(filename):
    with open(filename, 'rb') as fp:
        jsr = JavascriptArtefact(url="file:{}".format(filename), sha256="XXX", md5="XXX")
        ret, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor=args.extractor)
        if failed:
            raise Exception(stderr)
        return json.loads(ret.decode('utf-8'))

ret1 = calc_vector(args.file1)
ret2 = calc_vector(args.file2)
nv1, sum1 = calculate_ast_vector(ret1["statements_by_count"])
nv2, sum2 = calculate_ast_vector(ret2["statements_by_count"])
if args.v:
    print(ret1)
    print(ret2)
    print(nv1)
    print(nv2)
euclidean_dist = math.dist(nv1, nv2)
if has_scipy:
    cosine_dist = spatial.distance.cosine(nv1, nv2)
    print("Cosine distance for AST vector: "+str(cosine_dist))
print("Euclidean distance for AST vector: "+str(euclidean_dist))
print("Computed distance is: "+str(compute_distance(nv1, nv2)))
fn1 = ret1["calls_by_count"].keys()
fn2 = ret2["calls_by_count"].keys()
all_fns = set(fn1).union(fn2)
print(all_fns)
nv1, sum3 = calculate_vector(ret1["calls_by_count"], feature_names=all_fns)
nv2, sum4 = calculate_vector(ret2["calls_by_count"], feature_names=all_fns)
if args.v:
    print("Function Vector 1"+str(nv1)) 
    print("Function Vector 2"+str(nv2))
d1 = {t[0]: t[1] for t in zip(all_fns, nv1)}
d2 = {t[0]: t[1] for t in zip(all_fns, nv2)}
print("Euclidean distance for Function Call vector: "+str(calc_function_dist(d1, d2)))
if has_scipy:
    print("Cosine distance for Function Call vector: "+str(spatial.distance.cosine(nv1, nv2)))
print("Computed distance for function call vector: "+str(calc_function_dist(ret1['calls_by_count'], ret2['calls_by_count'])))
t = calculate_literal_distance(ret1['literals_by_count'], ret2['literals_by_count'], debug=True)
print("Euclidean distance for Literal (both string and integer) vector: "+str(t[0]))
print("Number of literals not found in origin: "+str(t[1]))
print("Number of literals not found in control: "+str(t[2]))
print("DE literals: "+str(t[3]))
exit(0)
