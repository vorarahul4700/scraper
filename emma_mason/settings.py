import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

BOT_NAME = 'emma_mason'

SPIDER_MODULES = ['emma_mason.fetcher']
NEWSPIDER_MODULE = 'emma_mason.fetcher'

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = int(os.getenv('MAX_WORKERS', '16'))
CONCURRENT_REQUESTS_PER_DOMAIN = 8

DOWNLOAD_DELAY = float(os.getenv('DOWNLOAD_DELAY', '0.1'))
RANDOMIZE_DOWNLOAD_DELAY = False

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 5.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0

COOKIES_ENABLED = True

HTTPCACHE_ENABLED = False

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
FEED_EXPORT_ENCODING = 'utf-8'

DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
}

LOG_LEVEL = 'INFO'
LOG_ENABLED = True
LOG_FILE = None
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
LOG_STDOUT = True
