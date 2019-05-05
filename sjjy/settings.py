# -*- coding: utf-8 -*-

# Scrapy settings for sjjy project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'sjjy'

SPIDER_MODULES = ['sjjy.spiders']
NEWSPIDER_MODULE = 'sjjy.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'sjjy (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

DATA_DIR = r'C:\Users\Administrator\Desktop\weibo\\'
# DATA_DIR = r'/mnt/zhangyanchao/'

DOWNLOAD_TIMEOUT = 10

LOG_FILE = "mySpider.log"

# REDIS = {
#     'url': None,
#     'host': '47.105.103.8',
#     'port': 56789,
#     'password': '12345678'
#     }

REDIS = {
 #   'url': None,
    'host': '172.181.217.58',
    'port': 6379,
    }

MONGODB = {
    'url': 'mongodb://47.105.103.8:27017/',
    'host': '47.105.103.8',
    'port': 6379,
    'password': ''
}

PRODUCT_NAME = 'bjh_test'
PRODUCT_TEMP_NAME = 'bjh_test_t'

BXS_RESOURCE_POOL = 'bxs_resource'  # LIST
INVALID_BXS_RESOURCE_POOL = 'invalid_bxs_resour' # LIST

BXS_RESOURCE_POOL_PR = 'https_proxy'
INVALID_BXS_RESOURCE_POOL_PR = 'invalid_https_proxy'
BXS_RESOURCE_POOL_UA = 'mobile_ua'
BXS_RESOURCE_POOL_CK = 'sjjy_cookies'
INVALID_BXS_RESOURCE_POOL_CK = 'invalid_sjjy_cookies'

NORMAL_TABLE = 'normal_1'
ABNORMAL_TABLE = 'abnormal_1'

NORMAL_ING = 'normal_ing_1'
NORMAL_DONE = 'normal_done_1'

ID_TABLE = 'normal_ids'

HANDLE_HTTPSTATUS_CODES = [404, 406]
RETRY_HTTP_CODES = [500, 502] # default is  [500, 502, 503, 504, 408]
HANDLE_PROXY_ERROR_CODES = [400, 401, 403, 407, 408, 503, 504]

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 10

REDIRECT_ENALBED = False

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'sjjy.middlewares.SjjySpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # 'sjjy.middlewares.SjjyDownloaderMiddleware': 543,
    'sjjy.middlewares.UserAgentMiddleware': 401,
   # 'sjjy.middlewares.CookiesMiddleware': 402,
    'sjjy.middlewares.ProxyMiddleware': 450,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'sjjy.pipelines.SjjyPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
