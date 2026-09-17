import os
import csv
import time
import sys
import gc
import random
import threading
import cloudscraper
from curl_cffi import requests as cc_requests
from typing import Optional, Tuple
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# ================= STORE REGISTRY =================
STORE_REGISTRY = {
    "afa-stores": {
        "name": "AFA Stores",
        "domain": "afastores.com",
        "url": "https://www.afastores.com",
        "sitemap": "https://www.afastores.com/sitemap.xml",
    },
    "english-elm": {
        "name": "English Elm",
        "domain": "englishelm.com",
        "url": "https://englishelm.com",
        "sitemap": "https://englishelm.com/sitemap.xml",
    },
    "grayson-living": {
        "name": "Grayson Living",
        "domain": "graysonliving.com",
        "url": "https://www.graysonliving.com",
        "sitemap": "https://www.graysonliving.com/sitemap.xml",
    },
    "france-and-son": {
        "name": "France & Son",
        "domain": "franceandson.com",
        "url": "https://www.franceandson.com",
        "sitemap": "https://www.franceandson.com/sitemap.xml",
    },
    "grayson-luxury": {
        "name": "Grayson Luxury",
        "domain": "graysonluxury.com",
        "url": "https://www.graysonluxury.com",
        "sitemap": "https://www.graysonluxury.com/sitemap.xml",
    },
}

STORE_ALIASES = {
    "afa": "afa-stores",
    "ee": "english-elm",
    "gl": "grayson-living",
    "fas": "france-and-son",
    "glx": "grayson-luxury",
}

# ================= ENV =================

TARGET_STORE = os.getenv("TARGET_STORE", "").strip().lower()
TARGET_STORE = STORE_ALIASES.get(TARGET_STORE, TARGET_STORE)

RAW_CURR_URL = os.getenv("CURR_URL", "").strip().rstrip("/")
if not RAW_CURR_URL and TARGET_STORE in STORE_REGISTRY:
    CURR_URL = STORE_REGISTRY[TARGET_STORE]["url"]
elif RAW_CURR_URL:
    CURR_URL = RAW_CURR_URL
else:
    CURR_URL = "https://www.graysonliving.com"

# Auto-detect store if CURR_URL provided but TARGET_STORE is not
if not TARGET_STORE:
    for store_key, store_data in STORE_REGISTRY.items():
        if store_data["domain"] in CURR_URL:
            TARGET_STORE = store_key
            break

RAW_SITEMAP = os.getenv("SITEMAP_INDEX", "").strip()
if RAW_SITEMAP:
    SITEMAP_INDEX = RAW_SITEMAP
elif TARGET_STORE in STORE_REGISTRY:
    SITEMAP_INDEX = STORE_REGISTRY[TARGET_STORE]["sitemap"]
else:
    SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"

SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))

# Reduced workers to avoid detection (cap at 4 max for Shopify API)
MAX_WORKERS = min(int(os.getenv("MAX_WORKERS", "3")), 4)  # Max 4 workers
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY_BASE", os.getenv("REQUEST_DELAY", "0.3")))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # Fixed deprecated utcnow()

# ================= LOGGER =================

def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        # Initialize cloudscraper with browser-like headers
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10  # Cloudflare challenge delay
        )
        
        # Headers for HTML/XML pages (sitemap, robots.txt)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/",
        }
        # Headers for JSON API requests (product .json endpoint)
        self.json_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Referer": CURR_URL + "/",
        }
        
        self.scraper.headers.update(self.headers)
        self.retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff
        self.request_count = 0
        self.last_request_time = 0
        self.rate_limit_until = 0  # Global timestamp for synchronized thread backoff
        self.lock = threading.Lock()
        
    def _respect_rate_limit(self, crawl_delay=None):
        """Add minimal delay between requests & enforce shared rate-limit cooldown across all threads"""
        current_time = time.time()
        
        # Check if a global rate limit pause is active
        with self.lock:
            pause_needed = self.rate_limit_until - current_time

        if pause_needed > 0:
            log(f"Global rate-limit active: pausing thread for {pause_needed:.1f}s...")
            time.sleep(pause_needed)
            current_time = time.time()

        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            target_delay = random.uniform(base_delay * 0.8, base_delay * 1.2)
            
            if elapsed < target_delay:
                sleep_time = target_delay - elapsed
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use cloudscraper for Cloudflare-protected pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            response = self.scraper.get(url, timeout=30)
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Cloudscraper error for {url}: {e}")
            return None, 0
    
    def _fetch_with_curl_cffi(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use curl_cffi for JavaScript-heavy pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            response = cc_requests.get(
                url, 
                headers=self.headers,
                timeout=30,
                impersonate="chrome110"
            )
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None, json_mode: bool = False) -> Optional[str]:
        """Intelligent fetching with synchronized thread backoff"""
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        # Choose strategy based on retry count
        if retry_count == 0:
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        elif retry_count % 2 == 1:
            try:
                self._respect_rate_limit(crawl_delay)
                h = self.json_headers if json_mode else self.headers
                response = cc_requests.get(url, headers=h, timeout=30, impersonate="chrome120")
                content, status = (response.text, response.status_code) if response.status_code == 200 else (None, response.status_code)
            except Exception as e:
                log(f"Curl_cffi error for {url}: {e}")
                content, status = None, 0
        else:
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        
        if content:
            return content
        
        # Synchronized retry backoff across all threads
        if status == 429:
            delay = random.uniform(3.0, 5.0)
            with self.lock:
                new_until = time.time() + delay
                if new_until > self.rate_limit_until:
                    self.rate_limit_until = new_until
                    log(f"HTTP 429 encountered for {url}! Setting global cooldown of {delay:.1f}s for all threads.")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay, json_mode)
        elif status in [403, 503]:
            delay = random.uniform(2.0, 4.0)
            with self.lock:
                new_until = time.time() + delay
                if new_until > self.rate_limit_until:
                    self.rate_limit_until = new_until
                    log(f"HTTP {status} encountered for {url}! Setting global cooldown of {delay:.1f}s for all threads.")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay, json_mode)
        elif status == 404:
            log(f"URL not found: {url}")
            return None
        
        if status != 200:
            delay = random.uniform(2.0, 5.0)
            log(f"Retry {retry_count+1} for {url} in {delay:.1f}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        
        return None

# Initialize global request manager
request_manager = RequestManager()

# ================= HTTP FUNCTIONS =================

def http_get(url: str, crawl_delay=None, json_mode: bool = False) -> Optional[str]:
    """Wrapper for request manager"""
    return request_manager.fetch(url, crawl_delay=crawl_delay, json_mode=json_mode)

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

def fetch_json(url: str, crawl_delay=None) -> Optional[dict]:
    data = http_get(url, crawl_delay, json_mode=True)
    if not data:
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}")
        return None

def normalize_image(url: str) -> str:
    return "https:" + url if url and url.startswith("//") else (url or "")

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def extract_category(tags):
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    elif not isinstance(tags, list):
        tags = []
    for t in tags:
        if t.startswith("collection_"):
            return t.replace("collection_", ""), ""
    return "", ""

def get_main_image(product: dict) -> str:
    img = product.get("image")
    if isinstance(img, dict) and img.get("src"):
        return normalize_image(img.get("src"))
    images = product.get("images")
    if isinstance(images, list) and len(images) > 0 and isinstance(images[0], dict) and images[0].get("src"):
        return normalize_image(images[0].get("src"))
    featured = product.get("featured_image")
    if isinstance(featured, str):
        return normalize_image(featured)
    elif isinstance(featured, dict) and featured.get("src"):
        return normalize_image(featured.get("src"))
    return ""

def process_product(url: str, writer, seen: set, crawl_delay=None):
    if url in seen:
        return
    seen.add(url)
    
    # Guard: only process URLs that are actual product pages
    clean_url = url.rstrip("/")
    if "/products/" not in clean_url:
        log(f"Skipping non-product URL: {url}")
        return
    
    product_url = clean_url + ".json"
    data = fetch_json(product_url, crawl_delay)
    
    if not data:
        log(f"Failed to fetch product: {product_url}")
        return
        
    if isinstance(data, dict) and "product" in data:
        product = data["product"]
    else:
        product = data

    if not isinstance(product, dict) or not product.get("variants"):
        log(f"No variants for product: {product_url}")
        return

    tags = product.get("tags", [])
    category, category_url = extract_category(tags)
    if not category:
        category = product.get("product_type") or product.get("type", "")
    
    brand = product.get("vendor", "")
    product_name = product.get("title", "")
    product_id = product.get("id", "")
    main_image = get_main_image(product)
    
    handle = product.get("handle", "")
    if handle:
        product_page_url = f"{CURR_URL}/products/{handle}"
    else:
        product_page_url = f"{CURR_URL}{product.get('url', '')}"

    variants_processed = 0
    raw_json_str = json.dumps(data, ensure_ascii=False)  # Full raw JSON for the row
    for v in product["variants"]:
        available = v.get("available")
        if available is None:
            iq = v.get("inventory_quantity")
            if iq is not None:
                available = iq > 0
            else:
                available = True

        row = [
            f"{product_page_url}?variant={v.get('id', '')}",  # Ref Product URL
            product_id,                         # Ref Product ID
            v.get("id", ""),                    # Ref Variant ID
            v.get("title", ""),                 # Ref Variant Title
            category,                           # Ref Category
            category_url,                       # Ref Category URL
            brand,                              # Ref Brand Name
            product_name,                       # Ref Product Name
            v.get("sku", ""),                   # Ref SKU
            v.get("sku", ""),                   # Ref MPN
            v.get("barcode", ""),               # Ref GTIN
            v.get("price", ""),                 # Ref Price
            main_image,                         # Ref Main Image
            1 if available else 0,              # Ref Quantity
            v.get("option1", "") or "",          # Ref Group Attr 1
            v.get("option2", "") or "",          # Ref Group Attr 2
            "active" if available else "inactive",  # Ref Status
            SCRAPED_DATE,                       # Date Scraped
            raw_json_str,                       # Raw JSON
        ]

        with csv_lock:
            writer.writerow(row)
        variants_processed += 1
    
    log(f"Processed {variants_processed} variants from {product_url}")

# ================= ROBOTS.TXT CHECK =================

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location.
    Only reads Crawl-delay from the User-agent: * section to avoid picking up
    delays meant for specific bots (AhrefsBot, MJ12bot, etc.).
    """
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    robots_content = http_get(robots_url)
    if not robots_content:
        log("No robots.txt found or couldn't fetch it")
        return None, None

    lines = robots_content.split('\n')
    crawl_delay = None
    sitemap_url = None
    in_wildcard_section = False  # track if we're in User-agent: * block
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        lower = line.lower()
        
        # Detect User-agent section changes
        if lower.startswith('user-agent:'):
            ua = line.split(':', 1)[1].strip()
            in_wildcard_section = (ua == '*')
            continue
        
        # Sitemap is global (not per user-agent) — collect it from anywhere
        if lower.startswith('sitemap:'):
            parts = line.split(':', 1)
            if len(parts) > 1:
                potential_url = parts[1].strip()
                if potential_url.startswith('http') and not sitemap_url:
                    sitemap_url = potential_url
                    log(f"Found sitemap in robots.txt: {sitemap_url}")
            continue
        
        # Crawl-delay: only honour from the User-agent: * section
        if lower.startswith('crawl-delay:') and in_wildcard_section and crawl_delay is None:
            try:
                crawl_delay = float(line.split(':', 1)[1].strip())
                log(f"Found Crawl-delay for *: {crawl_delay} seconds")
            except (ValueError, IndexError) as e:
                log(f"Error parsing crawl-delay: {e}")
    
    if crawl_delay is None:
        log("No Crawl-delay specified for User-agent: * — using configured REQUEST_DELAY_BASE")
    
    return crawl_delay, sitemap_url

# ================= MAIN =================

def main():
    log("Enhanced Cloudflare-resistant scraper started")
    log(f"Base URL: {CURR_URL}")
    log(f"Using cloudscraper + curl_cffi for bypass")
    
    # Check robots.txt
    crawl_delay, robots_sitemap = check_robots_txt()
    
    # Validate the sitemap URL from robots.txt
    sitemap_index = SITEMAP_INDEX  # Default to standard sitemap
    
    if robots_sitemap and robots_sitemap.startswith('http'):
        sitemap_index = robots_sitemap
        log(f"Using sitemap from robots.txt: {sitemap_index}")
    else:
        if robots_sitemap:
            log(f"Invalid sitemap URL in robots.txt: '{robots_sitemap}', using default")
        else:
            log(f"No valid sitemap in robots.txt, using default: {sitemap_index}")
    
    # If crawl_delay is found in robots.txt, use it (but cap it at reasonable value)
    if crawl_delay:
        if crawl_delay > 30:  # Cap at 30 seconds max
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")
    
    # Load sitemap index — try multiple sitemap locations
    SITEMAP_CANDIDATES = [
        sitemap_index,
        f"{CURR_URL}/sitemap_index.xml",
        f"{CURR_URL}/sitemaps/sitemap.xml",
        f"{CURR_URL}/sitemap/sitemap.xml",
    ]
    
    index = None
    for candidate in SITEMAP_CANDIDATES:
        log(f"Trying sitemap: {candidate}")
        index = load_xml(candidate, crawl_delay)
        if index is not None:
            log(f"Successfully loaded sitemap: {candidate}")
            break
        log(f"Failed to load: {candidate}")
    
    if not index:
        log("ERROR: Could not load sitemap index from any known location. Site may be blocking requests.")
        log("Exiting without failure to allow merge job to run.")
        sys.exit(0)
    
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = [e.text for e in index.findall(".//ns:sitemap/ns:loc", ns)]
    
    # Flat sitemap detection
    flat_product_urls = []
    if not sitemaps:
        # Try alternative namespace
        sitemaps = [e.text for e in index.findall(".//sitemap/loc")]
    if not sitemaps:
        # Flat sitemap: all <loc> entries are product URLs directly
        flat_product_urls = [e.text for e in index.findall(".//loc")]
        if flat_product_urls:
            log(f"Flat sitemap detected: {len(flat_product_urls)} product URLs found directly")
        else:
            log("No sitemaps or product URLs found in sitemap index. Exiting.")
            sys.exit(0)
    
    log(f"Total sitemaps found: {len(sitemaps)}")
    
    # Apply offset and limit (only for indexed sitemaps)
    if sitemaps:
        if MAX_SITEMAPS > 0:
            sitemaps = sitemaps[SITEMAP_OFFSET:SITEMAP_OFFSET + MAX_SITEMAPS]
        elif SITEMAP_OFFSET > 0:
            sitemaps = sitemaps[SITEMAP_OFFSET:]
        
        log(f"Sitemaps to process in this chunk: {len(sitemaps)}")
        
        if not sitemaps:
            log("No sitemaps to process in this chunk")
            sys.exit(0)
    
    # Create output file
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
            "Ref Variant Title",
            "Ref Category",
            "Ref Category URL",
            "Ref Brand Name",
            "Ref Product Name",
            "Ref SKU",
            "Ref MPN",
            "Ref GTIN",
            "Ref Price",
            "Ref Main Image",
            "Ref Quantity",
            "Ref Group Attr 1",
            "Ref Group Attr 2",
            "Ref Status",
            "Date Scraped",
            "Raw JSON",
        ])
        
        seen = set()
        total_products = 0
        
        def process_url_list(urls, label=""):
            nonlocal total_products
            if MAX_URLS_PER_SITEMAP and len(urls) > MAX_URLS_PER_SITEMAP:
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"  Limited to {len(urls)} URLs")
            
            log(f"  Processing {len(urls)} URLs{' in ' + label if label else ''}")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for url in urls:
                    if url and (url.startswith(CURR_URL) or url.startswith('http')):
                        future = executor.submit(process_product, url, writer, seen, crawl_delay)
                        futures.append(future)
                    elif url:
                        log(f"  Skipping URL not from current domain: {url}")
                
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    if completed % 5 == 0:
                        log(f"  Processed {completed}/{len(futures)} URLs")
                    try:
                        future.result()
                    except Exception as e:
                        log(f"  Error processing URL: {e}")
            
            total_products += len(urls)
        
        # Handle flat sitemap (product URLs directly in sitemap)
        if flat_product_urls:
            process_url_list(flat_product_urls, "flat sitemap")
        else:
            # Process indexed sitemaps
            for sitemap_idx, sitemap_url in enumerate(sitemaps):
                log(f"[{sitemap_idx+1}/{len(sitemaps)}] Loading sitemap: {sitemap_url}")
                
                xml = load_xml(sitemap_url, crawl_delay)
                if not xml:
                    log(f"  Failed to load sitemap, skipping")
                    continue
                
                # Extract URLs
                urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
                if not urls:
                    urls = [e.text for e in xml.findall(".//url/loc")]
                if not urls:
                    urls = [e.text for e in xml.findall(".//loc")]
                
                log(f"  Found {len(urls)} URLs in sitemap")
                process_url_list(urls, f"sitemap {sitemap_idx+1}")
                
                # Longer pause between sitemaps
                if sitemap_idx < len(sitemaps) - 1:
                    base_pause = crawl_delay * 5 if crawl_delay else 10
                    pause = random.uniform(base_pause * 0.8, base_pause * 1.2)
                    log(f"  Pausing {pause:.1f}s before next sitemap...")
                    time.sleep(pause)
                
                gc.collect()
    
    log(f"Chunk completed: {OUTPUT_CSV}")
    log(f"Total unique products processed: {len(seen)}")
    log(f"Total requests made: {request_manager.request_count}")

if __name__ == "__main__":
    main()