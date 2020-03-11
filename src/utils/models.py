from dataclasses import dataclass

@dataclass
class PageStats:
   url: str
   when: str
   scripts: str = '' # whitespace separated list of <script src=X> URLs
   n_hrefs: int = 0
   n_hrefs_max_permitted: int = 0
   n_external: int = 0
   n_external_accepted: int = 0
   n_internal: int = 0
   n_internal_accepted: int = 0
   n_scripts: int = 0
   n_scripts_accepted: int = 0

@dataclass
class DownloadFailure:
   reason: str
   url: str    # valid URL
   origin: str # valid URL
   http_status: int # eg. 404 
   when: str # UTC timestamp

@dataclass
class JavascriptLocation:
   country: str # NB: first non AU/US/na country reported only
   origin: str
   script: str
   when: str
   ip: str # NB: first IP associated with the script host only
