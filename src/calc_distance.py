#!/usr/bin/python3
import os
import math
import json
import argparse
import sys
import hashlib
from utils.features import analyse_script, calculate_ast_vector, calculate_vector
from utils.models import JavascriptArtefact
from scipy import spatial

# Usage: python3 calc_distance.py --file1 test-javascript/customize-preview.js --file2 test-javascript/customize-preview.min.js --extractor `pwd`/extract-features.jar
# which will compare a minimised JS artefact against a non-minified artefact and report distances

a = argparse.ArgumentParser(description="Evaluate and permanently store each AST vector against all controls, storing results in MongoDB and Kafka")
a.add_argument("--v", help="Debug verbosely", action="store_true")
a.add_argument("--file1", help="File 1 (javascript only)", type=str, required=True)
a.add_argument("--file2", help="File 2 (javascript only)", type=str, required=True)
a.add_argument("--extractor", help="JAR file to extract features as JSON [extract-features.jar]", type=str, default="/home/acas/src/extract-features.jar")
args = a.parse_args()

def calc_vector(filename):
    with open(filename, 'rb') as fp:
        jsr = JavascriptArtefact(url="file:{}".format(filename), sha256="XXX", md5="XXX")
        ret, failed, stderr = analyse_script(fp.read(), jsr, feature_extractor=args.extractor)
        if failed:
            raise Exception(stderr)
        return ret

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
cosine_dist = spatial.distance.cosine(nv1, nv2)
print("Euclidean distance for AST vector: "+str(euclidean_dist))
print("Cosine distance for AST vector: "+str(cosine_dist))
fn1 = set(ret1["calls_by_count"].keys())
fn2 = set(ret2["calls_by_count"].keys())
all_fns = fn1.union(fn2)
nv1, sum3 = calculate_vector(ret1["calls_by_count"], feature_names=all_fns)
nv2, sum4 = calculate_vector(ret2["calls_by_count"], feature_names=all_fns)
if args.v:
    print("Function Vector 1"+str(nv1)) 
    print("Function Vector 2"+str(nv2))
print("Euclidean distance for Function Call vector: "+str(math.dist(nv1, nv2)))
print("Cosine distance for Function Call vector: "+str(spatial.distance.cosine(nv1, nv2)))
exit(0)
