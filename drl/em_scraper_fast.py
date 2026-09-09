import os
import csv
import time
import sys
import gc
import threading
import requests
import random
import re
import json
import html
import ast
from typing import Optional, List, Dict, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV & CONFIG =================

CURR_URL = os.getenv("CURR_URL", "https://www.emmamason.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "0.2"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))

# FlareSolverr configuration
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "60"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HYBRID FAST FETCH ENGINE =================

class FastScraperEngine:
    """
    High-Performance Hybrid Scraping Engine:
    1. Uses a SINGLE global FlareSolverr session to solve Cloudflare & extract cookies.
    2. Shared HTTP session uses FlareSolverr cookies/User-Agent for lightning-fast direct HTTP requests (0.1s - 0.3s/URL).
    3. If FlareSolverr is down or unavailable, falls back directly to HTTP.
    4. If a 403/401 is encountered, thread-safely refreshes FlareSolverr cookies and retries.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.flaresolverr_available = True
        self.flaresolverr_session_id = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer": CURR_URL + "/",
        }
        self.session.headers.update(self.headers)
        self.check_flaresolverr()

    def check_flaresolverr(self):
        """Test FlareSolverr connectivity."""
        try:
            resp = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.list"}, timeout=5)
            if resp.status_code == 200:
                log("✓ FlareSolverr service connected successfully", "INFO")
                self.flaresolverr_available = True
                self.init_flaresolverr_session()
            else:
                log(f"FlareSolverr service returned status {resp.status_code}. Using Direct HTTP mode.", "WARNING")
                self.flaresolverr_available = False
        except Exception as e:
            log(f"FlareSolverr not available ({e}). Running in Direct HTTP mode.", "WARNING")
            self.flaresolverr_available = False

    def init_flaresolverr_session(self):
        """Initialize single global FlareSolverr session."""
        if not self.flaresolverr_available:
            return
        with self.lock:
            if self.flaresolverr_session_id:
                try:
                    requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.destroy", "session": self.flaresolverr_session_id}, timeout=10)
                except Exception:
                    pass
                self.flaresolverr_session_id = None

            try:
                resp = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.create"}, timeout=30)
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    self.flaresolverr_session_id = resp.json().get("session")
                    log(f"✓ Initialized Global FlareSolverr Session: {self.flaresolverr_session_id}", "INFO")
                    self.solve_and_update_cookies(CURR_URL)
            except Exception as e:
                log(f"Error creating FlareSolverr session: {e}", "WARNING")

    def solve_and_update_cookies(self, url: str) -> Optional[str]:
        """Request URL via FlareSolverr to solve Cloudflare challenge and update cookies."""
        if not self.flaresolverr_available:
            return None

        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000,
            "headers": self.headers
        }
        if self.flaresolverr_session_id:
            payload["session"] = self.flaresolverr_session_id

        try:
            resp = requests.post(FLARESOLVERR_URL, json=payload, timeout=FLARESOLVERR_TIMEOUT)
            if resp.status_code == 200:
                res = resp.json()
                if res.get("status") == "ok":
                    solution = res.get("solution", {})
                    cookies = solution.get("cookies", [])
                    user_agent = solution.get("userAgent")

                    if user_agent:
                        self.session.headers["User-Agent"] = user_agent

                    for c in cookies:
                        self.session.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))

                    log(f"✓ FlareSolverr solved Cloudflare challenge. Updated {len(cookies)} cookies.", "INFO")
                    return solution.get("response", "")
        except Exception as e:
            log(f"FlareSolverr request error for {url}: {e}", "WARNING")

        return None

    def fetch(self, url: str, max_retries: int = 3) -> Tuple[Optional[str], int]:
        """
        Fetch URL with high-speed direct HTTP requests.
        If blocked (403/401/503), uses FlareSolverr to refresh cookies and retries.
        """
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=15)

                if resp.status_code == 200:
                    return resp.text, 200

                if resp.status_code in [403, 401, 503] and self.flaresolverr_available:
                    log(f"HTTP {resp.status_code} on fast fetch for {url}. Refreshing Cloudflare cookies via FlareSolverr...", "WARNING")
                    with self.lock:
                        content = self.solve_and_update_cookies(url)
                        if content:
                            return content, 200
                        self.init_flaresolverr_session()

                    time.sleep(1.0)
                    continue

                if resp.status_code == 404:
                    return None, 404

            except requests.exceptions.RequestException as e:
                log(f"Direct request attempt {attempt + 1} failed for {url}: {e}", "WARNING")
                if self.flaresolverr_available:
                    with self.lock:
                        content = self.solve_and_update_cookies(url)
                        if content:
                            return content, 200
                time.sleep(1.0)

        return None, 0

engine = FastScraperEngine()

# ================= REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        self.request_count = 0
        self.last_request_time = 0
        self.lock = threading.Lock()

    def _respect_rate_limit(self, crawl_delay=None):
        with self.lock:
            current_time = time.time()
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            if self.request_count > 0:
                elapsed = current_time - self.last_request_time
                if elapsed < base_delay:
                    time.sleep(base_delay - elapsed)

            self.last_request_time = time.time()
            self.request_count += 1

    def fetch(self, url: str, crawl_delay=None) -> Optional[str]:
        self._respect_rate_limit(crawl_delay)
        content, status = engine.fetch(url)
        return content if status == 200 else None

request_manager = RequestManager()

def http_get(url: str, crawl_delay=None) -> Optional[str]:
    return request_manager.fetch(url, crawl_delay=crawl_delay)

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")

    content, status = engine.fetch(robots_url)
    if content and status == 200:
        lines = content.split('\n')
        crawl_delay = None
        sitemap_url = None

        for line in lines:
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    if potential_url.startswith('http'):
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            elif line.lower().startswith('crawl-delay:'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        crawl_delay = float(parts[1].strip())
                        log(f"Found Crawl-delay: {crawl_delay} seconds")
                except (ValueError, IndexError) as e:
                    log(f"Error parsing crawl-delay: {e}")

        return crawl_delay, sitemap_url

    log("No robots.txt found or couldn't fetch it")
    return None, None

# ================= DATA EXTRACTION =================

def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    return obj

def extract_datalayer(html_text: str):
    patterns = [
        r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);',
    ]

    raw = None
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            raw = match.group(1)
            break

    if not raw:
        return None

    raw = html.unescape(raw)
    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None') \
             .replace('true,', 'True,') \
             .replace('false,', 'False,') \
             .replace('null,', 'None,')

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            if raw.strip().startswith('{'):
                raw = f'[{raw}]'
            data = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                data = json.loads(raw)
            except Exception:
                return None

    return _clean_strings(data)

def extract_additional_product_info(html_text: str) -> str:
    """Extract Magento specifications table key-values."""
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')

        if not table:
            table = soup.find('table', class_='additional-attributes')
            if not table:
                return json.dumps({})

        additional_info = {}
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        for row in rows:
            th = row.find('th')
            td = row.find('td')

            if th and td:
                label_text = th.get_text(strip=True)
                data_text = td.get_text(strip=True)

                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    json_key = json_key.strip('_')
                    additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        log(f"Error extracting additional attributes table: {e}", "WARNING")
        return json.dumps({})

def extract_json_ld(html_text: str) -> str:
    """Extract schema.org JSON-LD scripts (<script type='application/ld+json'>)."""
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        scripts = soup.find_all('script', type='application/ld+json')
        ld_blocks = []
        for s in scripts:
            if s.string:
                try:
                    parsed = json.loads(s.string.strip())
                    ld_blocks.append(parsed)
                except Exception:
                    pass

        if not ld_blocks:
            return json.dumps({})

        result = ld_blocks[0] if len(ld_blocks) == 1 else ld_blocks
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        log(f"Error extracting JSON-LD script: {e}", "DEBUG")
        return json.dumps({})

def fetch_json(url: str, crawl_delay=None) -> Optional[dict]:
    """Fetch HTML page and parse dataLayer, Magento specs, and JSON-LD structured data."""
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        data_layer = extract_datalayer(data)
        product_data = (data_layer[0] if isinstance(data_layer, list) else data_layer) if data_layer else {}

        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None) if product_data else None
        if is_pdp == 0:
            return None

        additional_info = extract_additional_product_info(data)
        json_ld_str = extract_json_ld(data)

        if not product_data:
            product_data = {}

        product_data["additional_product_info_html"] = additional_info
        product_data["json_ld_data"] = json_ld_str
        product_data["raw_json_data"] = json.dumps(data_layer, ensure_ascii=False) if data_layer else json.dumps({})
        return product_data
    except Exception as e:
        log(f"Error processing product page for {url}: {e}", "WARNING")
        return None

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    return url

def extract_product_data(product_data: dict) -> dict:
    try:
        product_id = str(product_data.get('ecomm_prodid', [''])[0] if isinstance(product_data.get('ecomm_prodid'), list) and product_data.get('ecomm_prodid') else '')

        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        name = ''
        if ecommerce_items:
            name = ecommerce_items[0].get('item_name', '').strip()
        if not name:
            name = product_data.get('product', {}).get('name', '').strip()
        if not product_id:
            name = product_data.get('product', {}).get('id', '').strip()

        sku = product_data.get('ecomm_prodsku', '')
        if not sku:
            sku = product_data.get('product', {}).get('sku', '')

        brand = ''
        quantity = 0
        price = ''

        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
            price_item = ecommerce_items[0].get('price', '')
            if price_item:
                price = str(price_item)

        if not price:
            ecomm_value = product_data.get('ecommerce', {}).get('value', '')
            if ecomm_value:
                price = str(ecomm_value)

        main_image = ''
        additional_data = product_data.get('additional_product_info_html', '{}')
        json_ld_data = product_data.get('json_ld_data', '{}')
        raw_json_data = product_data.get('raw_json_data', '{}')

        mpn = sku
        category = ''

        try:
            additional_info_dict = json.loads(additional_data)
            mpn = additional_info_dict.get('item_number', "")
            category = additional_info_dict.get('product_type', "")
        except Exception as e:
            log(f"Error parsing additional specs JSON: {e}", "DEBUG")

        category_url = ''

        if ecommerce_items and not category:
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = [ecommerce_items[0].get(field, '') for field in category_fields if ecommerce_items[0].get(field, '')]
            if categories:
                category = ' | '.join(categories)

        availability = product_data.get('ecommerce', {}).get('magentoProductAvailability', '')
        status = 'SELLABLE' if availability == 'InStock' else 'OUT_OF_STOCK'

        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': main_image,
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': category_url,
            'quantity': quantity,
            'status': status,
            'variation_id': '',
            'group_attr_1': '',
            'group_attr_2': '',
            'additional_data': additional_data,
            'json_ld_data': json_ld_data,
            'raw_json_data': raw_json_data,
        }

    except Exception as e:
        log(f"Error extracting product fields: {e}", "ERROR")
        return {}

csv_lock = threading.Lock()

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    with csv_lock:
        if product_url in seen:
            return
        seen.add(product_url)

    log(f"Processing product: {product_url}", "DEBUG")
    data = fetch_json(product_url, crawl_delay)

    if not data:
        with csv_lock:
            stats['errors'] += 1
        return

    product_info = extract_product_data(data)
    if not product_info.get('product_id') and not product_info.get('json_ld_data'):
        with csv_lock:
            stats['errors'] += 1
        return

    try:
        row = [
            product_url,
            product_info['product_id'],
            product_info['variation_id'],
            product_info['category'],
            product_info['category_url'],
            product_info['brand'],
            product_info['name'],
            product_info['sku'],
            product_info['mpn'],
            '',
            product_info['price'],
            normalize_image_url(product_info['main_image']),
            product_info['quantity'],
            product_info['group_attr_1'],
            product_info['group_attr_2'],
            product_info['status'],
            product_info['additional_data'],
            product_info['json_ld_data'],
            product_info['raw_json_data'],
            SCRAPED_DATE
        ]

        with csv_lock:
            writer.writerow(row)
            stats['products_fetched'] += 1
            stats['urls_processed'] += 1

        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")

    except Exception as e:
        log(f"Error writing row for product {product_info.get('product_id', 'unknown')}: {e}", "ERROR")
        with csv_lock:
            stats['errors'] += 1

# ================= MAIN EXECUTION =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    crawl_delay = 0
    sitemap = robots_sitemap if (robots_sitemap and robots_sitemap.startswith('http')) else SITEMAP_INDEX

    log("=" * 60)
    log("Emma Mason High-Performance Fast Scraper")
    log(f"FlareSolverr Available: {engine.flaresolverr_available}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay Base: {REQUEST_DELAY_BASE}s")
    log("=" * 60)

    log(f"Loading sitemap index: {sitemap}")
    index = load_xml(sitemap, crawl_delay)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break

    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)

    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process in this job: {len(sitemaps_to_process)}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Varient ID",
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
            "Additional Product Data",
            "JSON-LD Data",
            "Raw JSON Data",
            "Date Scrapped"
        ])

        seen = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }

        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")

            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue

            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                        and ('.html' in e.text)
                    ]
                    if urls:
                        break

            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue

            if MAX_URLS_PER_SITEMAP > 0:
                urls = urls[:MAX_URLS_PER_SITEMAP]

            log(f"Processing {len(urls)} product URLs using {MAX_WORKERS} thread workers...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Thread execution error: {e}", "ERROR")

            gc.collect()

    log("=" * 60)
    log("FAST SCRAPING COMPLETED")
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    log(f"Output saved: {OUTPUT_CSV}")
    log("=" * 60)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
