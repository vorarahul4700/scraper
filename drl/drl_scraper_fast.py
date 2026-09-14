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

# ================= STORE REGISTRY =================
# Pre-configured stores sharing the same Magento/dataLayer architecture
STORE_REGISTRY = {
    "bedroom-furniture-discounts": {
        "name": "Bedroom Furniture Discounts",
        "domain": "bedroomfurniturediscounts.com",
        "url": "https://www.bedroomfurniturediscounts.com",
        "sitemap": "https://www.bedroomfurniturediscounts.com/sitemaps/sitemap.xml",
    },
    "discount-living-rooms": {
        "name": "Discount Living Rooms",
        "domain": "discountlivingrooms.com",
        "url": "https://www.discountlivingrooms.com",
        "sitemap": "https://www.discountlivingrooms.com/sitemaps/sitemap_discount.xml",
    },
    "dining-rooms-outlet": {
        "name": "Dining Rooms Outlet",
        "domain": "diningroomsoutlet.com",
        "url": "https://www.diningroomsoutlet.com",
        "sitemap": "https://www.diningroomsoutlet.com/sitemaps/sitemap_diningrooms.xml",
    },
    "tv-stands-outlet": {
        "name": "TV Stands Outlet",
        "domain": "tvstandsoutlet.com",
        "url": "https://www.tvstandsoutlet.com",
        "sitemap": "https://www.tvstandsoutlet.com/sitemaps/sitemap_tvstands.xml",
    },
}

# Aliases for convenience
STORE_ALIASES = {
    "bfd": "bedroom-furniture-discounts",
    "dlr": "discount-living-rooms",
    "dro": "dining-rooms-outlet",
    "tvs": "tv-stands-outlet",
}

# ================= ENV & CONFIGURATION =================

TARGET_STORE = os.getenv("TARGET_STORE", "").strip().lower()
TARGET_STORE = STORE_ALIASES.get(TARGET_STORE, TARGET_STORE)

# Resolve Base URL
RAW_CURR_URL = os.getenv("CURR_URL", "").strip().rstrip("/")
if not RAW_CURR_URL and TARGET_STORE in STORE_REGISTRY:
    CURR_URL = STORE_REGISTRY[TARGET_STORE]["url"]
elif RAW_CURR_URL:
    CURR_URL = RAW_CURR_URL
else:
    CURR_URL = "https://www.bedroomfurniturediscounts.com"

# Auto-detect store if CURR_URL provided but TARGET_STORE is not
if not TARGET_STORE:
    for store_key, store_data in STORE_REGISTRY.items():
        if store_data["domain"] in CURR_URL:
            TARGET_STORE = store_key
            break

# Resolve Sitemap Index URL
RAW_SITEMAP = os.getenv("SITEMAP_INDEX", "").strip()
if RAW_SITEMAP:
    SITEMAP_INDEX = RAW_SITEMAP
elif TARGET_STORE in STORE_REGISTRY:
    SITEMAP_INDEX = STORE_REGISTRY[TARGET_STORE]["sitemap"]
else:
    SITEMAP_INDEX = f"{CURR_URL}/sitemaps/sitemap.xml"

# Scraper execution settings
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
SPECIFIC_OFFSETS = os.getenv("SPECIFIC_OFFSETS", "").strip()
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "0.3"))

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
    1. Connects to FlareSolverr to solve Cloudflare Turnstile/Managed Challenges.
    2. Synchronizes cookies and User-Agent into a persistent requests.Session.
    3. Performs direct HTTP requests at ultra-fast speeds (0.1s - 0.3s/URL).
    4. Auto-refreshes Cloudflare clearance cookies if a 403/401/503 is detected.
    5. Gracefully falls back to direct HTTP mode if FlareSolverr is offline.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.flaresolverr_available = False
        self.flaresolverr_session_id = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
        """Test connectivity to FlareSolverr service."""
        try:
            resp = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.list"}, timeout=5)
            if resp.status_code == 200:
                log("✓ Connected to FlareSolverr service successfully", "INFO")
                self.flaresolverr_available = True
                self.init_flaresolverr_session()
            else:
                log(f"FlareSolverr returned status {resp.status_code}. Using Direct HTTP mode.", "WARNING")
                self.flaresolverr_available = False
        except Exception as e:
            log(f"FlareSolverr not reachable ({e}). Running in Direct HTTP mode.", "WARNING")
            self.flaresolverr_available = False

    def init_flaresolverr_session(self):
        """Initialize or recycle a persistent FlareSolverr session."""
        if not self.flaresolverr_available:
            return
        with self.lock:
            if self.flaresolverr_session_id:
                try:
                    requests.post(
                        FLARESOLVERR_URL,
                        json={"cmd": "sessions.destroy", "session": self.flaresolverr_session_id},
                        timeout=10
                    )
                except Exception:
                    pass
                self.flaresolverr_session_id = None

            try:
                resp = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.create"}, timeout=30)
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    self.flaresolverr_session_id = resp.json().get("session")
                    log(f"✓ Initialized Global FlareSolverr Session: {self.flaresolverr_session_id}", "INFO")
                    # Pre-warm session with the target site to obtain Cloudflare clearance
                    self.solve_and_update_cookies(CURR_URL)
            except Exception as e:
                log(f"Error creating FlareSolverr session: {e}", "WARNING")

    def solve_and_update_cookies(self, url: str) -> Optional[str]:
        """Request URL through FlareSolverr to solve challenge and update session cookies."""
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
                        c_domain = c.get("domain", "")
                        self.session.cookies.set(c.get("name"), c.get("value"), domain=c_domain)
                        if c_domain and not c_domain.startswith("."):
                            self.session.cookies.set(c.get("name"), c.get("value"), domain=f".{c_domain}")

                    log(f"✓ FlareSolverr solved Cloudflare challenge for {url}. Synced {len(cookies)} cookies.", "INFO")
                    return solution.get("response", "")
                else:
                    log(f"FlareSolverr returned status '{res.get('status')}' for {url}: {res.get('message')}", "WARNING")
        except Exception as e:
            log(f"FlareSolverr request error for {url}: {e}", "WARNING")

        return None

    def fetch(self, url: str, max_retries: int = 3) -> Tuple[Optional[str], int]:
        """
        Execute high-speed direct HTTP request using synced cookies.
        Automatically invokes FlareSolverr cookie refresh on 403/401/503.
        """
        last_status = 0
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=15)
                last_status = resp.status_code

                if resp.status_code == 200:
                    return resp.text, 200

                if resp.status_code in [403, 401, 503] and self.flaresolverr_available:
                    log(f"HTTP {resp.status_code} detected for {url}. Refreshing Cloudflare cookies...", "WARNING")
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

        return None, last_status

engine = FastScraperEngine()

# ================= RATE LIMITING & REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        self.request_count = 0
        self.last_request_time = 0
        self.lock = threading.Lock()

    def _respect_rate_limit(self):
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            if self.request_count > 0 and elapsed < REQUEST_DELAY_BASE:
                time.sleep(REQUEST_DELAY_BASE - elapsed)

            self.last_request_time = time.time()
            self.request_count += 1

    def fetch(self, url: str) -> Optional[str]:
        self._respect_rate_limit()
        content, _ = engine.fetch(url)
        return content

request_manager = RequestManager()

def http_get(url: str) -> Optional[str]:
    return request_manager.fetch(url)

# ================= SITEMAP PARSER =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Fetch and parse XML sitemap, handling Cloudflare and XML syntax recovery."""
    data = http_get(url)
    if not data:
        log(f"Failed to fetch XML content from {url}", "ERROR")
        return None

    try:
        # Strip potential HTML wrapper if returned
        if "<?xml" not in data[:100] and "<urlset" not in data and "<sitemapindex" not in data:
            match = re.search(r'(<\?xml[\s\S]*?</(urlset|sitemapindex)>)', data)
            if match:
                data = match.group(1)
            else:
                data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data

        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}. Attempting regex recovery...", "WARNING")
        try:
            root = ET.Element("urlset")
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
            for url_text in urls:
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text.strip()
            return root
        except Exception as e2:
            log(f"Regex extraction failed: {e2}", "ERROR")
            return None

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

def extract_datalayer(html_text: str) -> Optional[dict]:
    """
    Extract Google Tag Manager dataLayer from raw HTML:
    Supports:
    1. dataLayer = [ { ... } ];
    2. dataLayer.push( { ... } );
    3. dataLayer = { ... };
    """
    patterns = [
        r'dataLayer\s*=\s*(\[[\s\S]*?\]);',
        r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);',
        r'dataLayer\s*=\s*(\{[\s\S]*?\});'
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
    raw = (
        raw.replace(':true', ':True')
           .replace(':false', ':False')
           .replace(':null', ':None')
           .replace('true,', 'True,')
           .replace('false,', 'False,')
           .replace('null,', 'None,')
    )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            data = ast.literal_eval(raw)
        except Exception:
            try:
                cleaned = re.sub(r',\s*}', '}', raw)
                cleaned = re.sub(r',\s*]', ']', cleaned)
                data = json.loads(cleaned)
            except Exception:
                return None

    cleaned_data = _clean_strings(data)
    if isinstance(cleaned_data, list) and cleaned_data:
        return cleaned_data[0]
    elif isinstance(cleaned_data, dict):
        return cleaned_data
    return None

def extract_additional_product_info(html_text: str) -> str:
    """
    Extract product specifications from Magento tables:
    Targets .Product__additional-container, .data-table, and #product-attribute-specs-table
    """
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        container = soup.find('div', class_='Product__additional-container')
        if not container:
            container = soup.find('div', class_='data-table')

        additional_info = {}

        if container:
            labels = container.find_all('div', class_='label')
            for label in labels:
                label_text = label.get_text(strip=True)
                data_div = label.find_next_sibling('div', class_='data')
                if data_div:
                    data_text = data_div.get_text(strip=True)
                    if label_text and data_text:
                        json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_')).strip('_')
                        additional_info[json_key] = data_text

        # Also inspect standard HTML table if available
        table = soup.find('table', id='product-attribute-specs-table') or soup.find('table', class_='additional-attributes')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label_text = th.get_text(strip=True)
                    data_text = td.get_text(strip=True)
                    if label_text and data_text:
                        json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_')).strip('_')
                        if json_key not in additional_info:
                            additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        log(f"Error extracting additional info table: {e}", "WARNING")
        return json.dumps({})

def normalize_image_url(url: str) -> str:
    """Normalize relative or protocol-relative image URLs."""
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    return url

def extract_product_data(product_data: dict, raw_html: str, product_url: str) -> dict:
    """
    Transform raw dataLayer + DOM data into standard 18-column dictionary schema.
    """
    try:
        # Product ID
        product_id = str(
            product_data.get('magentoProductId') or
            product_data.get('id') or
            product_data.get('product_id') or ''
        )

        # Name
        name = (
            product_data.get('magentoProductName') or
            product_data.get('name') or ''
        ).strip()

        # SKU
        sku = str(
            product_data.get('magentoProductSku') or
            product_data.get('sku') or ''
        ).strip()

        # Brand & Quantity from ecommerce items
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        brand = ''
        quantity = 0
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
            if not name:
                name = ecommerce_items[0].get('item_name', '').strip()

        # Price
        price = str(
            product_data.get('magentoProductPrice') or
            product_data.get('price') or ''
        )
        if not price and ecommerce_items:
            price = str(ecommerce_items[0].get('price', ''))
        if not price:
            price = str(product_data.get('ecommerce', {}).get('value', ''))

        # Main Image
        main_image = product_data.get('magentoProductImage1') or product_data.get('image') or ''

        # Additional Specs & MPN extraction
        additional_data_str = product_data.get('additional_product_info_html', '{}')
        mpn = sku
        category = ''

        try:
            additional_dict = json.loads(additional_data_str) if additional_data_str else {}
            item_number = additional_dict.get('item_number', '').strip()
            if item_number:
                mpn = item_number

            category = additional_dict.get('product_type', '').strip()

            # HTML Fallback for Item Number
            if (not item_number or mpn == sku) and raw_html:
                soup = BeautifulSoup(raw_html, "html.parser")
                for label in soup.select("div.label"):
                    if "item number" in label.get_text(strip=True).lower():
                        data_div = label.find_next_sibling("div", class_="data")
                        if data_div and data_div.get_text(strip=True):
                            mpn = data_div.get_text(strip=True)
                            break
        except Exception as e:
            log(f"Error parsing MPN or category: {e}", "DEBUG")

        # Category hierarchy from ecommerce items
        if ecommerce_items and not category:
            cat_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            cats = [ecommerce_items[0].get(f, '') for f in cat_fields if ecommerce_items[0].get(f)]
            if cats:
                category = ' | '.join(cats)

        # Availability
        availability = product_data.get('magentoProductAvailability', '')
        status = 'SELLABLE' if availability.lower() in ['instock', 'in stock', '1'] else 'OUT_OF_STOCK'

        # Fallback to DOM Title if name is still empty
        if not name and raw_html:
            soup = BeautifulSoup(raw_html, "html.parser")
            h1 = soup.find('h1')
            if h1:
                name = h1.get_text(strip=True)

        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': normalize_image_url(main_image),
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': '',
            'quantity': quantity,
            'status': status,
            'variation_id': '',
            'group_attr_1': '',
            'group_attr_2': '',
            'additional_data': additional_data_str,
        }
    except Exception as e:
        log(f"Error extracting product details: {e}", "ERROR")
        return {}

# ================= WORKER & MULTI-THREADING =================

csv_lock = threading.Lock()

def process_product_url(product_url: str, writer, seen: set, stats: dict):
    """Fetch product page, parse dataLayer, and append row to CSV."""
    if product_url in seen:
        return
    seen.add(product_url)

    html_content = http_get(product_url)
    if not html_content:
        stats['errors'] += 1
        log(f"Failed to fetch product content for {product_url}", "ERROR")
        return

    # Extract dataLayer
    dl = extract_datalayer(html_content)
    additional_info = extract_additional_product_info(html_content)

    if not dl:
        dl = {}

    dl['additional_product_info_html'] = additional_info
    product_info = extract_product_data(dl, html_content, product_url)

    if not product_info.get('name') and not product_info.get('sku') and not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Could not extract valid product data for {product_url}", "WARNING")
        return

    try:
        row = [
            product_url,
            product_info.get('product_id', ''),
            product_info.get('variation_id', ''),
            product_info.get('category', ''),
            product_info.get('category_url', ''),
            product_info.get('brand', ''),
            product_info.get('name', ''),
            product_info.get('sku', ''),
            product_info.get('mpn', ''),
            '',  # GTIN
            product_info.get('price', ''),
            product_info.get('main_image', ''),
            product_info.get('quantity', ''),
            product_info.get('group_attr_1', ''),
            product_info.get('group_attr_2', ''),
            product_info.get('status', ''),
            product_info.get('additional_data', '{}'),
            SCRAPED_DATE
        ]

        with csv_lock:
            writer.writerow(row)

        stats['products_fetched'] += 1
        log(f"✓ Fetched: {product_info.get('sku', 'N/A')} | {product_info.get('name', '')[:45]}...", "INFO")

    except Exception as e:
        stats['errors'] += 1
        log(f"Error saving row for {product_url}: {e}", "ERROR")

    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    log("=" * 65)
    log("DRL / TVS / DRO / BFD Fast FlareSolverr Scraper")
    log(f"Target Store:        {TARGET_STORE.upper() if TARGET_STORE else 'UNKNOWN'}")
    log(f"Base Site URL:       {CURR_URL}")
    log(f"Sitemap Index:       {SITEMAP_INDEX}")
    log(f"Sitemap Offset:      {SITEMAP_OFFSET}")
    log(f"Max Sitemaps:        {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Specific Offsets:    {SPECIFIC_OFFSETS if SPECIFIC_OFFSETS else 'None'}")
    log(f"Max URLs/Sitemap:    {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Parallel Workers:    {MAX_WORKERS}")
    log(f"Request Delay:       {REQUEST_DELAY_BASE}s")
    log(f"FlareSolverr:        {'Connected' if engine.flaresolverr_available else 'Disabled / Offline'}")
    log(f"Output File:         {OUTPUT_CSV}")
    log("=" * 65)

    # 1. Load Main Sitemap Index
    log(f"Loading main sitemap index from: {SITEMAP_INDEX}")
    index_xml = load_xml(SITEMAP_INDEX)
    if index_xml is None:
        log("Failed to load sitemap index. Exiting.", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    all_sitemaps = []

    # Extract sub-sitemaps
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index_xml.findall(path, ns) if "ns:" in path else index_xml.findall(path)
        if elements:
            all_sitemaps = [
                e.text.strip() for e in elements
                if e.text and (".xml" in e.text.lower() or "product" in e.text.lower())
            ]
            break

    if not all_sitemaps:
        # Check if the root XML is already a urlset rather than an index
        url_elements = index_xml.findall(".//ns:url/ns:loc", ns) or index_xml.findall(".//loc")
        if url_elements:
            log(f"Root sitemap is a direct urlset containing {len(url_elements)} URLs", "INFO")
            all_sitemaps = [SITEMAP_INDEX]
        else:
            log("No sub-sitemaps found in index.", "ERROR")
            sys.exit(1)

    # 2. Filter Sitemaps to Process
    sitemaps_to_process = []
    if SPECIFIC_OFFSETS:
        offsets = [int(x.strip()) for x in SPECIFIC_OFFSETS.split(',') if x.strip().isdigit()]
        for off in offsets:
            if 0 <= off < len(all_sitemaps):
                sitemaps_to_process.append(all_sitemaps[off])
        log(f"Selected {len(sitemaps_to_process)} specific sitemaps from offsets: {offsets}")
    else:
        if SITEMAP_OFFSET >= len(all_sitemaps):
            log(f"Offset {SITEMAP_OFFSET} exceeds total available sitemaps ({len(all_sitemaps)}). Exiting.", "WARNING")
            sys.exit(0)

        end_idx = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(all_sitemaps)
        sitemaps_to_process = all_sitemaps[SITEMAP_OFFSET:end_idx]

    log(f"Total sitemaps in index: {len(all_sitemaps)}")
    log(f"Sitemaps scheduled for this job: {len(sitemaps_to_process)}")

    # 3. Initialize Output CSV
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
            "Date Scrapped"
        ])

        seen_urls = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }

        # 4. Process Each Sitemap
        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"--> Processing Sitemap [{stats['sitemaps_processed']}/{len(sitemaps_to_process)}]: {sitemap_url}")

            sub_xml = load_xml(sitemap_url)
            if not sub_xml:
                log(f"Could not load sub-sitemap: {sitemap_url}", "ERROR")
                continue

            # Extract product page URLs
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = sub_xml.findall(path, ns) if "ns:" in path else sub_xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and not any(ext in e.text.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.xml'])
                        and ('.html' in e.text.lower())
                    ]
                    if urls:
                        break

            if not urls:
                log(f"No valid product URLs found in {sitemap_url}", "WARNING")
                continue

            if MAX_URLS_PER_SITEMAP > 0:
                total_urls_found = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Processing capped {len(urls)} of {total_urls_found} URLs in this sitemap")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")

            # Execute parallel scrape
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_url, u, writer, seen_urls, stats)
                    for u in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Thread worker exception: {e}", "ERROR")
                        stats['errors'] += 1

            gc.collect()

    # 5. Summary Statistics
    log("=" * 65)
    log("JOB SUMMARY & METRICS")
    log("=" * 65)
    log(f"Sitemaps processed:        {stats['sitemaps_processed']}")
    log(f"Product URLs processed:    {stats['urls_processed']}")
    log(f"Products successfully saved: {stats['products_fetched']}")
    log(f"Errors encountered:        {stats['errors']}")
    if stats['urls_processed'] > 0:
        success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
        log(f"Success rate:              {success_rate:.1f}%")
    log(f"Output chunk written to:   {OUTPUT_CSV}")
    log("=" * 65)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
