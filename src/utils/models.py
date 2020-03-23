from dataclasses import dataclass
from datetime import datetime

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
    sha256: str
    md5: str
    inline: bool = False
    content_type: str = 'text/javascript'
    when: str = str(datetime.utcnow())
    size_bytes: int = 0

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

@dataclass
class BestControl:
    control_url: str        # chosen control URL with best ast_dist (from CDNJS typically)
    origin_url: str         # javascript artefact URL found by kafkaspider
    sha256_matched: bool    # hash match (only computed if distance is low since thats the only way it can be matched)
    ast_dist: float         # AST syntax tree feature distance between origin and control? (0 means same features in JS code)
    function_dist: float    # function call feature distance between origin and control (lower means fewer differences, not comparable across JS families)
    diff_functions: str     # functions which do not have the same count between control and origin
    
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
