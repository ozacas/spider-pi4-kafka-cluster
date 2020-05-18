from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enforce_typing import enforce_types
import getpass
import os

class Password:
    DEFAULT = 'Prompt if not specified'

    def __init__(self, value, prompt='Password: '):
        if value == self.DEFAULT:
            env = dict(os.environ)
            if 'PASSWORD' in env:
                value = env['PASSWORD']
                if len(value) < 1:
                    value = getpass.getpass(prompt)
            else:
                value = getpass.getpass(prompt)
        self.value = value

    def __str__(self):
        return self.value

@enforce_types
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

@enforce_types
@dataclass
class JavascriptLocation:
   country: str # NB: first non AU/US/na country reported only
   origin: str
   script: str
   when: str
   ip: str # NB: first IP associated with the script host only

@enforce_types
@dataclass
class JavascriptArtefact: # definition corresponds to visited kafka topic record schema
    url: str
    sha256: str
    md5: str
    inline: bool = False
    content_type: str = 'text/javascript'
    when: str = str(datetime.utcnow())
    size_bytes: int = 0
    origin: str = None # HTML page citing this artefact (maybe static or dynamic depending on who provides the artefact). Should always be provided if possible

    def __init__(self, *args, **kwargs):
       for k,v in kwargs.items():
          if k == "checksum":
              k = "md5"
          setattr(self, k, v)
 
    # ensure when sorted, that list has same checksums next to each other for ingestion efficiency
    def __lt__(self, other):
        return self.md5 < other.md5

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

@enforce_types
@dataclass
class BestControl:
    control_url: str        # chosen control URL with best ast_dist (from CDNJS typically)
    origin_url: str         # javascript artefact URL found by kafkaspider
    sha256_matched: bool    # hash match (only computed if distance is low since thats the only way it can be matched)
    ast_dist: float         # AST syntax tree feature distance between origin and control? (0 means same features in JS code)
    function_dist: float    # function call feature distance between origin and control (lower means fewer differences, not comparable across JS families)
    diff_functions: str     # functions which do not have the same count between control and origin
    cited_on: Optional[str] = None    # include HTML page which cited this origin_url (useful for ETL)
    origin_js_id: Optional[str] = None# objectid referring into db.script collection (only recent records have this set)
    literal_dist: float = -1.0 # if negative, denotes not computed. Otherwise represents similarity score between control and origin (includes union of all control and origin literals)
    xref: str = None        # ObjectId into db.vet_against_control collection for this hit
    literals_not_in_control: int = -1 # literals present in origin but not in control (computed by find_best_control())
    literals_not_in_origin: int = -1  # literals present in control but not found also in origin (ditto)
    n_diff_literals: int = -1         # literals which are present in both, but not found the same number of times

    def dist_prod(self):
       return self.ast_dist * self.function_dist

    def diff_functions_as_list(self):
       if len(self.diff_functions) < 1:
           return []
       return self.diff_functions.split(' ')

    def is_good_hit(self):
       n_diff = self.diff_functions.count(' ') 
       dist = self.ast_dist
       if (dist < 10.0 or (dist < 20.0 and n_diff < 10)) and (dist > 0.0 and self.function_dist > 0.0):
           return True
       # in case the literal distance cannot be computed, say False. Should not happen anymore.
       if self.literal_dist < 0.0:
           return False
       # NB: experience suggests that reasonable hits will have 3 dists < 100.0 (TODO FIXME: does this include all skimmers???)      
       return self.dist_prod() * self.literal_dist < 100.0

    def is_better(self, other):
        other_prod = other.dist_prod()
        if other_prod > 50.0:
            return True       # return True since other is a poor hit ie. self is better
        return self.dist_prod() < other_prod

@dataclass
class FeatureVector:
    sha256: str
    md5: str
    size_bytes: int
    first_url: str
    n: int   # count of URLs with given hash (not necessarily de-duped URLs)
    ArrayLiteral: int = 0
    CatchClause: int = 0
    ContinueStatement: int = 0
    SwitchStatement: int = 0
    InfixExpression: int = 0
    TryStatement: int = 0
    XmlString: int = 0
    WithStatement: int = 0
    EmptyExpression: int = 0
    PropertyGet: int = 0
    FunctionNode: int = 0
    ForInLoop: int = 0
    Block: int = 0
    NewExpression: int = 0
    RegExpLiteral: int = 0
    XmlLiteral: int = 0
    Scope: int = 0
    FunctionCall: int = 0
    WhileLoop: int = 0
    AstRoot: int = 0
    BreakStatement: int = 0
    ForLoop: int = 0
    ConditionalExpression: int = 0
    ThrowStatement: int = 0
    LabeledStatement: int = 0
    Assignment: int = 0
    EmptyStatement: int = 0
    ReturnStatement: int = 0
    VariableDeclaration: int = 0
    KeywordLiteral: int = 0
    NumberLiteral: int = 0
    ObjectLiteral: int = 0
    VariableInitializer: int = 0
    IfStatement: int = 0
    StringLiteral: int = 0
    ParenthesizedExpression: int = 0
    ExpressionStatement: int = 0
    UnaryExpression: int = 0
    ObjectProperty: int = 0
    DoLoop: int = 0
    SwitchCase: int = 0 
    Label: int = 0
    ElementGet: int = 0
    Name: int = 0

@enforce_types
@dataclass 
class JavascriptVectorSummary:
    origin: str                 # control CDN url
    sum_of_ast_features: int    # JS Abstract Syntax Tree Vector sum
    sum_of_functions: int       # JS Function Call count Vector sum
    last_updated: str           # date that etl_control_fix_magnitude.py was last run on the control
    sum_of_literals: int = 0    # Literals (truncated to max of 200 chars see analyse_script()) vector

    def __lt__(self, other):
       if self.sum_of_ast_features < other.sum_of_ast_features:
          return True
       elif self.sum_of_ast_features == other.sum_of_ast_features:
          return self.sum_of_functions < other.sum_of_functions
       else:
          return False
