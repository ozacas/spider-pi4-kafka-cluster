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

@dataclass
class JavascriptArtefact: # definition corresponds to visited kafka topic record schema
    url: str
    inline: bool
    content_type: str
    when: str
    sha256: str
    md5: str
    size_bytes: int

@dataclass
class CallsByCountVector:
    # we dont represent all functions here, just the main ones likely to indicate something worth investigating
    dollar: int # $.ajax(...)
    createElement: int
    ajax: int

@dataclass
class ThugLog:
    origin: str # URL for HTML page which contains the non-AU hosted JS that caused thug to be run
    log: str # text based (maybe quite big depending on the page)
    user_agent: str # randomly chosen UA from thug builtins by default
    scripts: str # whitespace separated list of JS URLs found 
    script_countries: str # whitespace separated set of countries JS is sourced from
    when: str # UTC timestamp
    thug_analysis_id: str # objectId (ie. foreign key) into thug mongoDB with JSON and other logging
