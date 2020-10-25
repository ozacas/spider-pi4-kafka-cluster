#!/usr/bin/python3
import pytest
from utils.geo import AustraliaGeoLocator

def test_is_ip():
   au_loc = AustraliaGeoLocator() 
   assert au_loc.is_ip('128.1') == False
   assert au_loc.is_ip('128.1.2.3') == True
   with pytest.raises(Exception):
       assert au_loc.is_ip(None) == False
   assert au_loc.is_ip('1::') == True
  
def test_as_ip(): 
   au_loc = AustraliaGeoLocator() 
   ret = au_loc.as_ip('www.google.com')
   assert ret != None
   ret = au_loc.as_ip('bogus.---host.name')
   assert ret is None 

def test_is_au():
   au_loc = AustraliaGeoLocator() 
   assert au_loc.is_au('www.vic.gov.au') == True
   assert au_loc.is_au('bbc.co.uk') == False

def test_country_code():
   au_loc = AustraliaGeoLocator()
   # portal.aisi.gov.au is an ELB - one ip is 13.236.114.249
   assert au_loc.country_code('portal.aisi.gov.au') == '' # API requires IP, so this must be empty string
   assert au_loc.country_code('13.236.114.249') == 'AU'
