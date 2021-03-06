AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 3.0
CONCURRENT_REQUESTS = 32 # NB: since small artefacts, lots of parallellism possible
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DEPTH_PRIORITY = 2
DNS_TIMEOUT = 10
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DOWNLOAD_DELAY = 0.125
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAXSIZE = 10000000
DOWNLOAD_FAIL_ON_DATALOSS = True
DOWNLOADER_STATS = False
HTTPERROR_ALLOWED_CODES = [400, 401, 402, 403, 404, 410, 500, 501, 502, 503] # just the major ones for applying a crawl penalty to the site
LOG_LEVEL = 'INFO'
MEDIA_ALLOW_REDIRECTS = True # item pipeline must also permit redirects to fetch JS (ugh... ugly!)
RANDOMIZE_DOWNLOAD_DELAY = True
REDIRECT_ENABLED = True
REDIRECT_MAX_TIMES = 5
# generating too many requests: since we dont visit many pages to the same site courtesy of kafka
#ROBOTSTXT_OBEY = True
SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'
TELNETCONSOLE_ENABLED = False

# set user agent exactly once per crawl session (eg. per day) to a random chosen one from thug personalities
from utils.misc import random_user_agent
USER_AGENT = random_user_agent()

# Dont want caching/proxying, ajax crawling or http auth for now
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None,
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': None,
    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': 580,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
    'scrapy.downloadermiddlewares.stats.DownloaderStats': None,
    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': None,
}


# COMMON spider settings
MONGO_HOST = 'pi1'
MONGO_PORT = 27017
MONGO_DB   = 'au_js'
ONEURL_MAXMIND_DB = '/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb'
LRU_MAX_PAGES_PER_SITE = 20   # only 20 pages per recent_sites cache entry ie. 20 pages per site for at least 500 sites spidered
# kafkaspider: specific settings
SPIDER_ROOT = '/home/spider' # where PID files and log files for the run are kept
ONEURL_KAFKA_BOOTSTRAP = 'kafka2'
ONEURL_KAFKA_CONSUMER_GROUP = 'scrapy-thug2'
ONEURL_KAFKA_URL_TOPIC = 'thug.gen5'
VISITED_TOPIC = 'visited'   # where to save details of each page spidered by kafkaspider
PAGESTATS_TOPIC = 'html-page-stats' # where to save details of each html page (link stats)
SITE_INTERNAL_LINK_LIMIT = 20 # if we visit more than twenty pages according to the LRU cache: we stop adding internal links to the kafka queue
KAFKASPIDER_MAX_SITE_CACHE = 1000 # dont go crazy with this number, doing so may exceed the max kafka message size which will fail persisting site cache state
KAFKASPIDER_MAX_RECENT_CACHE = 5000 # handle navbar related links quickly without refetching. Cache is not persisted
KAFKASPIDER_MONGO_USER = 'rw' # must be read-write to dump settings used for run (unfortunately)
OVERREPRESENTED_HOSTS_TOPIC = 'kafkaspider-long-term-disinterest'

# we use a modified FilesPipeline to persist the javascript to local storage (which scales better than mongo)
FILES_STORE = '/data/kafkaspider16' # must exist on scrapy host with suitable permissions for the spider-user account
FILES_DOWNLOAD_FAILURE_TOPIC = 'javascript-download-failure'
FILES_PIPELINE_FAILURE_TOPIC = 'javascript-pipeline-failure'
FILES_DOWNLOAD_ARTEFACTS_TOPIC = 'javascript-artefacts-16'
ITEM_PIPELINES = {'utils.mypipeline.MyFilesPipeline': 1}

# snippetspider: for persisting html-embedded javascript snippets into mongo
SNIPPETSPIDER_CONSUMER_GROUP = 'snippetspider' # where to keep track of current position in SNIPPET_SPIDER_URL_TOPIC
SNIPPETSPIDER_URL_TOPIC = 'html-page-stats' # where to read HTML pages visited from (kafka topic)
SNIPPETSPIDER_MONGO_USER = 'rw' # must have read-write access to be able to save JS snippets
