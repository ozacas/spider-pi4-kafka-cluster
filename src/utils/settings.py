LOG_LEVEL = 'INFO'
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
DEPTH_PRIORITY = 2
DNS_TIMEOUT = 10
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DOWNLOAD_DELAY = 0.5
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAXSIZE = 10000000
DOWNLOAD_FAIL_ON_DATALOSS = True
HTTPERROR_ALLOW_ALL = True 
RANDOMIZE_DOWNLOAD_DELAY = True
REDIRECT_MAX_TIMES = 5
REDIRECT_ENABLED = True
SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'
TELNETCONSOLE_ENABLED = False

# other spider related settings
MONGO_HOST = 'pi1'
MONGO_PORT = 27017
MONGO_DB   = 'au_js'
ONEURL_MAXMIND_DB = '/opt/GeoLite2-City_20200114/GeoLite2-City.mmdb'
LRU_MAX_PAGES_PER_SITE = 20   # only 20 pages per recent_sites cache entry ie. 20 pages per site for at least 500 sites spidered

# kafkaspider: specific settings
ONEURL_KAFKA_BOOTSTRAP = 'kafka1'
ONEURL_KAFKA_CONSUMER_GROUP = 'scrapy-thug2'
ONEURL_KAFKA_URL_TOPIC = 'thug.gen5'
VISITED_TOPIC = 'visited'   # where to save details of each page spidered by kafkaspider
PAGESTATS_TOPIC = 'html-page-stats' # where to save details of each html page (link stats)

# we use a modified FilesPipeline to persist the javascript to local storage (which scales better than mongo)
FILES_STORE = '/data/kafkaspider3' # must exist on scrapy host with suitable permissions for the spider-user account
FILES_DOWNLOAD_FAILURE_TOPIC = 'javascript-download-failure'
FILES_PIPELINE_FAILURE_TOPIC = 'javascript-pipeline-failure'
FILES_DOWNLOAD_ARTEFACTS_TOPIC = 'javascript-artefacts-3'
ITEM_PIPELINES = {'utils.mypipeline.MyFilesPipeline': 1}

# snippetspider: for persisting html-embedded javascript snippets into mongo
SNIPPETSPIDER_CONSUMER_GROUP = 'snippetspider' # where to keep track of current position in SNIPPET_SPIDER_URL_TOPIC
SNIPPETSPIDER_URL_TOPIC = 'visited' # where to read HTML pages visited from (kafka topic)
