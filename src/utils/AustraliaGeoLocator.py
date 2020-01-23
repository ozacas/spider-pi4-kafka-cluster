#!/usr/bin/python3

import socket
import ipaddress
import geoip2.database

class AustraliaGeoLocator(object):
   
   def __init__(self, db_location="/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb"): 
      self.reader = geoip2.database.Reader(db_location) 

   def is_ip(self, ip):
      if ip is None:
         raise Exception("Must be a valid IP or hostname!")
      try:
        ip = ipaddress.ip_address(ip) 
        return ip is not None # test for ip_network instance in case of cidr? nah... 
      except:
        return False

   def as_ip(self, host):
      try:
          ip = socket.gethostbyname(host)
          return ip
      except:
          return None

   def is_au(self, ip_or_host):
      is_host = not self.is_ip(ip_or_host)
      if is_host:
         ip = self.as_ip(ip_or_host)
         if ip is None:
            return False # else fallthru...
      else:
         ip = ip_or_host

      response = self.reader.city(ip) 
      return response is not None and response.country.iso_code == 'AU'
