import os
import sys
import csv
import json
import re
import time
import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from curl_cffi import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from emma_mason.utils.sitemap_processor import SitemapProcessor
except ImportError:
    try:
        from utils.sitemap_processor import SitemapProcessor
    except ImportError:
        SitemapProcessor = None


class ProductFetcher:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        self.logger = logging.getLogger('product')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.logger.addHandler(h)
        self.logger.propagate = True
        
        self.is_ashley = kwargs.get('is_ashley', False)
        self.ashley_urls = kwargs.get('ashley_urls', [])
        
        self.chunk_mode = kwargs.get('chunk_mode', False)
        self.chunk_id = int(kwargs.get('chunk_id', 0))
        self.total_chunks = int(kwargs.get('total_chunks', 1))
        self.chunk_size = int(kwargs.get('chunk_size', 0))

        self.website_url = kwargs.get('website_url', 'https://emmamason.com')
        self.sitemap_offset = int(kwargs.get('sitemap_offset', 0))
        self.max_sitemaps = int(kwargs.get('max_sitemaps', 0))
        self.max_urls_per_sitemap = int(kwargs.get('max_urls_per_sitemap', 0))
        self.job_id = kwargs.get('job_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        self.output_dir = kwargs.get('output_dir', 'output')
        self.max_workers = int(kwargs.get('max_workers', os.getenv('MAX_WORKERS', '8')))
        self.request_delay = float(kwargs.get('request_delay', os.getenv('REQUEST_DELAY', '0.1')))

        parsed_url = urlparse(self.website_url)
        self.domain = parsed_url.netloc or 'emmamason.com'
        self.base_domain = '.'.join(self.domain.split('.')[-2:]).replace('.', '_')
        self.timestamp = os.getenv('GITHUB_RUN_ID', 'local')

        os.makedirs(self.output_dir, exist_ok=True)
        self.output_csv = os.path.join(
            self.output_dir,
            f"output_{self.domain.replace('.', '_')}_{self.job_id}_{self.timestamp}.csv"
        )
        self.remaining_csv = os.path.join(
            self.output_dir,
            f"remaining_{self.domain.replace('.', '_')}_{self.job_id}_{self.timestamp}.csv"
        )
        self.unscraped_csv = os.path.join(
            self.output_dir,
            f"unscraped_{self.domain.replace('.', '_')}_{self.job_id}_{self.timestamp}.csv"
        )

        self.queued_or_processing_urls = set()
        self.processed_successfully_urls = set()
        self.success_db_conn = None
        self.success_db_write_counter = 0

        self.start_time = time.time()
        self.total_urls_found = 0
        self.processed_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.failed_requests = {}
        self.unscraped_requests = {}
        self.last_log_time = self.start_time
        self.log_interval = 30
        self.sitemap_urls_count = {}

        self.csv_lock = threading.Lock()
        self.rem_lock = threading.Lock()
        self.uns_lock = threading.Lock()
        self.db_lock = threading.Lock()

        self.csv_header = [
            'Ref Product URL',
            'Ref Product ID',
            'Ref Variant ID',
            'Ref Category',
            'Ref Category URL',
            'Product Type',
            'Ref Brand Name',
            'Collection Name',
            'Ref Product Name',
            'Ref SKU',
            'Ref MPN',
            'Ref GTIN',
            'Ref Price',
            'Ref Main Image',
            'Ref Quantity',
            'Ref Group Attr 1',
            'Ref Group Attr 2',
            'Ref Images',
            'Ref Dimensions',
            'Ref Status',
            'Ref Highlights',
            'Row JSON',
            'Date Scrapped'
        ]

        self.logger.info(f"📁 Starting job {self.job_id} - chunk {self.chunk_id}")
        self._init_success_store()

        if not self.is_ashley and SitemapProcessor:
            try:
                sitemap_processor = SitemapProcessor()
                self.sitemap_index_url = sitemap_processor.get_sitemap_from_robots(self.website_url)
                self.all_sitemaps = sitemap_processor.extract_all_sitemaps(self.sitemap_index_url)
            except Exception as e:
                self.logger.error(f"❌ Error discovering sitemaps: {e}")
                self.all_sitemaps = [f"{self.website_url.rstrip('/')}/sitemap.xml"]

    def normalize_url(self, url: str) -> str:
        if not url:
            return ""
        parsed = urlparse(url.strip())
        path = parsed.path.rstrip('/')
        normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
        if parsed.query:
            query_parts = sorted(parsed.query.split('&'))
            query = '&'.join(query_parts)
            normalized = f"{normalized}?{query}"
        return normalized

    def _should_schedule_url(self, url: str) -> bool:
        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return False

        if normalized_url in self.processed_successfully_urls:
            self.skipped_count += 1
            if self.verbose:
                self.logger.info(f"⏭️ URL already scraped successfully: {normalized_url}")
            return False

        if normalized_url in self.queued_or_processing_urls:
            self.skipped_count += 1
            if self.verbose:
                self.logger.info(f"⏭️ URL already queued/in-progress: {normalized_url}")
            return False

        self.queued_or_processing_urls.add(normalized_url)
        return True

    def _get_success_store_path(self):
        override_path = os.getenv('SUCCESS_URL_DB_PATH', '').strip()
        if override_path:
            return override_path
        os.makedirs(self.output_dir, exist_ok=True)
        return os.path.join(self.output_dir, f"success_urls_{self.base_domain}.sqlite3")

    def _init_success_store(self):
        try:
            success_store_path = self._get_success_store_path()
            self.success_db_conn = sqlite3.connect(success_store_path, timeout=30, check_same_thread=False)
            cursor = self.success_db_conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS successful_urls (
                    domain TEXT NOT NULL,
                    normalized_url TEXT NOT NULL,
                    first_success_at TEXT NOT NULL,
                    job_id TEXT,
                    PRIMARY KEY (domain, normalized_url)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_successful_urls_domain
                ON successful_urls(domain)
            """)
            self.success_db_conn.commit()

            cursor.execute(
                "SELECT normalized_url FROM successful_urls WHERE domain = ?",
                (self.domain.lower(),)
            )
            rows = cursor.fetchall()
            if rows:
                self.processed_successfully_urls.update(row[0] for row in rows if row and row[0])
            self.logger.info(
                f"🗂️ Loaded {len(rows)} previously successful URLs for {self.domain} from {success_store_path}"
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize persistent dedup store: {e}")
            self.success_db_conn = None

    def _persist_success_url(self, normalized_url: str):
        if not self.success_db_conn or not normalized_url:
            return
        with self.db_lock:
            try:
                self.success_db_conn.execute(
                    """
                    INSERT OR IGNORE INTO successful_urls (domain, normalized_url, first_success_at, job_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        self.domain.lower(),
                        normalized_url,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        self.job_id
                    )
                )
                self.success_db_write_counter += 1
                if self.success_db_write_counter >= 50:
                    self.success_db_conn.commit()
                    self.success_db_write_counter = 0
            except Exception as e:
                self.logger.error(f"❌ Failed to persist successful URL {normalized_url}: {e}")

    def fetch_page(self, session: requests.Session, url: str) -> (int, str):
        impersonates = ['chrome124', 'chrome120', 'safari17_0']
        for attempt in range(3):
            impersonate = impersonates[attempt % len(impersonates)]
            try:
                r = session.get(url, timeout=25, impersonate=impersonate, verify=False)
                if r.status_code == 200:
                    return 200, r.text
                if r.status_code in [404, 301, 302, 410]:
                    return r.status_code, ""
                self.logger.warning(f"⚠️ [Attempt {attempt+1}/3] HTTP {r.status_code} for {url} ({impersonate})")
                if r.status_code in [403, 429]:
                    time.sleep(1.5 * (attempt + 1))
            except Exception as e:
                self.logger.warning(f"⚠️ [Attempt {attempt+1}/3] Request error for {url}: {type(e).__name__} - {e}")
                time.sleep(1)
        return 0, ""

    def extract_specs_table(self, soup: BeautifulSoup) -> dict:
        specs = {}
        try:
            table = soup.find('table', id='product-attribute-specs-table') or soup.find('table', class_='additional-attributes')
            if table:
                for row in table.find_all('tr'):
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        label = th.get_text(strip=True)
                        val = td.get_text(strip=True)
                        if label and val:
                            key = re.sub(r'[^a-zA-Z0-9_]', '_', label.lower().replace(' ', '_')).strip('_')
                            specs[key] = val
        except Exception:
            pass
        return specs

    def process_url(self, session: requests.Session, url: str, writer: csv.writer) -> bool:
        self.processed_count += 1
        requested_normalized = self.normalize_url(url)

        if self.processed_count % 100 == 0:
            success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
            self.logger.info(f"📊 Progress: {self.processed_count}/{self.total_urls_found} URLs | ✅ Success: {self.processed_count - self.failed_count} | ❌ Failed: {self.failed_count} | 📈 Rate: {success_rate:.1f}%")

        status_code, html = self.fetch_page(session, url)

        if status_code in [404, 301, 302, 410]:
            with self.uns_lock:
                self.unscraped_requests[requested_normalized] = {
                    'url': url,
                    'reason': f"HTTP {status_code} status",
                    'status': 'Unscraped',
                    'error_type': f'HTTP_{status_code}',
                    'error_message': f'HTTP status {status_code}',
                    'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            return False

        if status_code != 200 or not html:
            self.failed_count += 1
            with self.rem_lock:
                self.failed_requests[requested_normalized] = {
                    'url': url,
                    'status': 'Failed',
                    'error_type': f'HTTP_{status_code}' if status_code else 'FETCH_ERROR',
                    'error_message': f'Failed to retrieve page (status {status_code})',
                    'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            return False

        soup = BeautifulSoup(html, 'html.parser')
        product_data = None
        breadcrumb_data = None
        is_collection = False
        category = ""
        category_url = ""

        for script in soup.find_all('script', type='application/ld+json'):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)
                if isinstance(data, list) and data:
                    data = data[0]
                if not isinstance(data, dict):
                    continue

                schema_type = data.get('@type', '')
                if schema_type == 'BreadcrumbList':
                    breadcrumb_data = data
                    items = data.get('itemListElement', [])
                    if items and isinstance(items[0], list):
                        items = items[0]
                    crumbs = []
                    for it in items:
                        if isinstance(it, dict) and 'item' in it:
                            c_name = it['item'].get('name', '')
                            c_url = it['item'].get('@id', '')
                            if c_name and c_name != 'Emmamason Home' and not c_url.endswith(url.split('/')[-1]):
                                crumbs.append((c_name, c_url))
                    if crumbs:
                        category = ' > '.join([c[0] for c in crumbs])
                        category_url = crumbs[-1][1]

                elif schema_type == 'Product':
                    product_data = data

                elif schema_type == 'CollectionPage':
                    is_collection = True

            except Exception:
                continue

        if not product_data:
            if is_collection:
                with self.uns_lock:
                    self.unscraped_requests[requested_normalized] = {
                        'url': url,
                        'reason': 'PLP category page (CollectionPage)',
                        'status': 'Skipped',
                        'error_type': 'PLP_SKIPPED',
                        'error_message': 'CollectionPage schema detected',
                        'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                return False

            self.failed_count += 1
            with self.rem_lock:
                self.failed_requests[requested_normalized] = {
                    'url': url,
                    'status': 'Failed',
                    'error_type': 'NO_PRODUCT_JSON',
                    'error_message': 'No Product JSON-LD found on page',
                    'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            return False

        specs = self.extract_specs_table(soup)

        selected_offer = product_data.get('offers', {})
        price = ""
        status = "In Stock"
        if isinstance(selected_offer, dict):
            price = selected_offer.get('price', '') or selected_offer.get('lowPrice', '') or ''
            avail = selected_offer.get('availability', '')
            if 'OutOfStock' in str(avail):
                status = 'Out of Stock'
        elif isinstance(selected_offer, list) and selected_offer:
            first_offer = selected_offer[0]
            if isinstance(first_offer, dict):
                price = first_offer.get('price', '') or first_offer.get('lowPrice', '') or ''
                avail = first_offer.get('availability', '')
                if 'OutOfStock' in str(avail):
                    status = 'Out of Stock'

        images_raw = product_data.get('image', '')
        all_images = []
        if isinstance(images_raw, list):
            all_images = [str(img).strip() for img in images_raw if str(img).strip()]
        elif isinstance(images_raw, str) and images_raw.strip():
            all_images = [images_raw.strip()]
        main_image = all_images[0] if all_images else ""
        images_str = ", ".join(all_images)

        brand_raw = product_data.get('brand', {})
        brand = brand_raw.get('name', '') if isinstance(brand_raw, dict) else str(brand_raw)
        if not brand and 'brand' in specs:
            brand = specs['brand']

        sku = product_data.get('sku', '') or specs.get('sku', '')
        name = product_data.get('name', '')
        mpn = product_data.get('mpn', '') or specs.get('mpn', '')
        gtin = product_data.get('gtin13', '') or product_data.get('gtin', '') or specs.get('gtin', '')
        description = product_data.get('description', '')
        material = product_data.get('material', '') or specs.get('material', '')
        dimensions = specs.get('dimensions', '') or specs.get('item_dimensions', '') or specs.get('size', '')
        collection_name = specs.get('collection', '') or specs.get('series', '')
        product_type = specs.get('product_type', '') or specs.get('type', 'simple')

        raw_json_payload = {
            'json_ld_product': product_data,
            'json_ld_breadcrumb': breadcrumb_data,
            'additional_specs': specs
        }

        row = [
            url,
            sku,
            '',
            category,
            category_url,
            product_type,
            brand,
            collection_name,
            name,
            sku,
            mpn,
            gtin,
            price,
            main_image,
            1,
            description,
            material,
            images_str,
            dimensions,
            status,
            description,
            json.dumps(raw_json_payload, ensure_ascii=False),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ]

        with self.csv_lock:
            writer.writerow(row)

        self.processed_successfully_urls.add(requested_normalized)
        self._persist_success_url(requested_normalized)
        if self.verbose and sku:
            self.logger.info(f"💾 Saved [{sku}] {name[:50]}")

        if self.request_delay > 0:
            time.sleep(self.request_delay)

        return True

    def run(self):
        urls_to_process = []
        if self.is_ashley and self.ashley_urls:
            urls_to_process = [u for u in self.ashley_urls if self._should_schedule_url(u)]
        elif hasattr(self, 'all_sitemaps') and self.all_sitemaps:
            from emma_mason.scripts.fetch_input_urls import fetch_from_sitemap
            urls_to_process = fetch_from_sitemap(self.website_url + "/sitemap.xml")
            urls_to_process = [u for u in urls_to_process if self._should_schedule_url(u)]

        self.total_urls_found = len(urls_to_process)
        self.logger.info(f"🚀 Starting crawl with {self.total_urls_found} URLs on {self.max_workers} threads")

        with open(self.output_csv, 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(self.csv_header)

            _thread_local = threading.local()

            def get_thread_session():
                if not hasattr(_thread_local, 'session'):
                    s = requests.Session()
                    s.headers.update({
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'accept-language': 'en-US,en;q=0.9',
                    })
                    _thread_local.session = s
                return _thread_local.session

            def worker_task(url_to_fetch):
                s = get_thread_session()
                return self.process_url(s, url_to_fetch, writer)

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(worker_task, u)
                    for u in urls_to_process
                ]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        self.logger.error(f"Worker exception: {e}")

        self.closed()
        return self.output_csv

    def closed(self):
        elapsed = time.time() - self.start_time
        success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0

        remaining_file = None
        if self.failed_requests:
            remaining_file = self.remaining_csv
            with open(remaining_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=['url', 'status', 'error_type', 'error_message', 'failed_at', 'job_id', 'chunk_id']
                )
                writer.writeheader()
                for row in self.failed_requests.values():
                    writer.writerow({
                        'url': row.get('url', ''),
                        'status': row.get('status', ''),
                        'error_type': row.get('error_type', ''),
                        'error_message': row.get('error_message', ''),
                        'failed_at': row.get('failed_at', ''),
                        'job_id': self.job_id,
                        'chunk_id': self.chunk_id,
                    })

        unscraped_file = None
        if self.unscraped_requests:
            unscraped_file = self.unscraped_csv
            with open(unscraped_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=['url', 'reason', 'status', 'error_type', 'error_message', 'failed_at', 'job_id', 'chunk_id']
                )
                writer.writeheader()
                for row in self.unscraped_requests.values():
                    writer.writerow({
                        'url': row.get('url', ''),
                        'reason': row.get('reason', ''),
                        'status': row.get('status', ''),
                        'error_type': row.get('error_type', ''),
                        'error_message': row.get('error_message', ''),
                        'failed_at': row.get('failed_at', ''),
                        'job_id': self.job_id,
                        'chunk_id': self.chunk_id,
                    })

        self.logger.info("=" * 70)
        self.logger.info(f"[REPORT] FINAL SCRAPING REPORT - Job: {self.job_id}")
        self.logger.info(f"   Summary:")
        self.logger.info(f"      - Total URLs found: {self.total_urls_found}")
        self.logger.info(f"      - URLs processed: {self.processed_count}")
        self.logger.info(f"      - [SUCCESS] Scraped: {self.processed_count - self.failed_count}")
        self.logger.info(f"      - [SKIP] Skipped: {self.skipped_count}")
        self.logger.info(f"      - [FAIL] Failed: {self.failed_count}")
        if remaining_file:
            self.logger.info(f"      - Remaining file: {remaining_file}")
            print(f"REMAINING_FILE={remaining_file}")
        if unscraped_file:
            self.logger.info(f"      - Unscraped file: {unscraped_file}")
            print(f"UNSCRAPED_FILE={unscraped_file}")
        self.logger.info(f"   Performance:")
        self.logger.info(f"      - Success rate: {success_rate:.1f}%")
        self.logger.info(f"      - Total time: {elapsed:.1f}s")
        self.logger.info("=" * 70)

        if self.success_db_conn:
            try:
                self.success_db_conn.commit()
                self.success_db_conn.close()
            except Exception as e:
                self.logger.error(f"❌ Error closing persistent dedup store: {e}")
