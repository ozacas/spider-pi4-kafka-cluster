#!/usr/bin/python3
import pytest
from utils.features import safe_for_mongo, as_url_fields, compute_distance, calculate_ast_vector, calc_function_dist

def test_safe_for_mongo():
   assert safe_for_mongo({ 'a': 0, '$': 1 }) == { 'a': 0, 'F$': 1 }

def test_as_url_fields():
   assert as_url_fields('https://www.google.com') == { 'has_query': False, 'host': 'www.google.com', 'path': '', 'port': 443,  'scheme': 'https' }
   assert as_url_fields('https://www.google.com/map', prefix='g') == { 'g_has_query': False, 'g_host': 'www.google.com', 'g_path': '/map', 'g_port': 443, 'g_scheme': 'https' }

def test_compute_distance():
   assert compute_distance([1.0, 3.0, 9.0], [1.0, 3.0, 9.0]) < 0.0000001
   assert compute_distance([1.0, 3.0], [1.0, 3.0], short_vector_penalty=True) < 0.0001
   assert compute_distance([112, 33], [99, 12]) >= 246.98
   dist = compute_distance([112, 33], [99, 12], short_vector_penalty=False)
   assert dist >= 24.6981 and dist <= 24.6982

def test_compute_function_dist():
   a = { "foo": 3, "bar": 7, "baz": 1 }
   b = { "crud": 20, "foo": 3, "bar": 7 }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist >= 10.01 and dist <= 10.02 
   assert sorted(diff_functions) == ['baz', 'crud']
   a = { "tmp": 1 }
   b = { "tmp": 2 }
   dist, diff_functions = calc_function_dist(a, b)
   assert dist > 0.0
   assert diff_functions == ['tmp']

def test_compute_ast_vector():
   d = { "ArrayLiteral": 10, "Assignment": 7, "AstRoot": 1 }
   tuple = calculate_ast_vector(d) 
   assert tuple == ([10, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 18) 
   assert compute_distance(tuple[0], tuple[0]) <= 0.001
