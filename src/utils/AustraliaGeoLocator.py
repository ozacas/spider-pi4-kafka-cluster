#!/usr/bin/python3

import socket
import ipaddress
import geoip2.database
import pylru

class AustraliaGeoLocator(object):
   
   def __init__(self, db_location="/home/acas/data/GeoLite2-City_20200114/GeoLite2-City.mmdb"): 
      self.reader = geoip2.database.Reader(db_location) 
      self.cache = pylru.lrucache(500)
      self.dns_cache = pylru.lrucache(500) # DNS lookups are expensive, so avoid them when we can...

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
          if host in self.dns_cache:
              return self.dns_cache[host]
          ip = socket.gethostbyname(host)
          self.dns_cache[host] = ip
          return ip
      except:
          return None

   def is_au(self, ip_or_host):
      try:
          if ip_or_host in self.cache:
              return self.cache[ip_or_host]

          is_host = not self.is_ip(ip_or_host)
          if is_host:
             ip = self.as_ip(ip_or_host)
             if ip is None:
                return False # else fallthru...
          else:
               ip = ip_or_host

          response = self.reader.city(ip) 
          ret = response is not None and response.country.iso_code == 'AU'
          self.cache[ip_or_host] = ret # store using the key as the call to the function, since thats what was webmastered ie. likely to cache hit
          return ret
      except:
          return False # if something goes wrong we say no to AU...
