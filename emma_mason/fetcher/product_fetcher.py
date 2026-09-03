import gzip
import xml.etree.ElementTree as ET
import json
import re
import csv
import sqlite3
from datetime import datetime
from urllib.parse import urlparse, urljoin
from scrapy import Spider, Request
import sys
from pathlib import Path
import time
import os
import logging
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from emma_mason.utils.sitemap_processor import SitemapProcessor
except ImportError:
    try:
        from utils.sitemap_processor import SitemapProcessor
    except ImportError:
        SitemapProcessor = None


class ProductFetcher(Spider):
    name = 'product'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.verbose = kwargs.get('verbose', False)
        try:
            self.logger.setLevel(logging.INFO if self.verbose else logging.WARNING)
            self.logger.propagate = True
        except Exception:
            self.logger = logging.getLogger('product')
            self.logger.setLevel(logging.INFO if self.verbose else logging.WARNING)
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

        parsed_url = urlparse(self.website_url)
        self.domain = parsed_url.netloc or 'emmamason.com'
        self.base_domain = '.'.join(self.domain.split('.')[-2:]).replace('.', '_')

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

        self.logger.info(f"📁 Starting job {self.job_id} - chunk {self.chunk_id}")
        self._init_success_store()

        if not self.is_ashley and SitemapProcessor:
            try:
                sitemap_processor = SitemapProcessor()
                self.sitemap_index_url = sitemap_processor.get_sitemap_from_robots(self.website_url)
                self.logger.info(f"📍 Found sitemap index: {self.sitemap_index_url}")
                self.all_sitemaps = sitemap_processor.extract_all_sitemaps(self.sitemap_index_url)
                self.logger.info(f"📚 Total sitemaps discovered: {len(self.all_sitemaps)}")
            except Exception as e:
                self.logger.error(f"❌ Error getting sitemaps: {e}")
                self.all_sitemaps = [f"{self.website_url.rstrip('/')}/sitemap.xml"]

    def _get_remaining_file_path(self):
        timestamp = os.getenv('GITHUB_RUN_ID', 'local')
        return os.path.join(
            self.output_dir,
            f"remaining_{self.domain.replace('.', '_')}_{self.job_id}_{timestamp}.csv"
        )

    def _get_unscraped_file_path(self):
        timestamp = os.getenv('GITHUB_RUN_ID', 'local')
        return os.path.join(
            self.output_dir,
            f"unscraped_{self.domain.replace('.', '_')}_{self.job_id}_{timestamp}.csv"
        )

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
            self.success_db_conn = sqlite3.connect(success_store_path, timeout=30)
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

    def start_requests(self):
        if self.is_ashley and self.ashley_urls:
            urls_to_process = self.ashley_urls
            self.total_urls_found = len(urls_to_process)

            if self.chunk_mode and self.total_chunks > 1:
                chunk_size = self.chunk_size if self.chunk_size > 0 else (len(self.ashley_urls) // self.total_chunks + (1 if len(self.ashley_urls) % self.total_chunks != 0 else 0))
                start_idx = self.chunk_id * chunk_size
                end_idx = min(start_idx + chunk_size, len(self.ashley_urls))
                urls_to_process = self.ashley_urls[start_idx:end_idx]
                self.total_urls_found = len(urls_to_process)

            self.logger.info(f"🎯 Total URLs to process in this job: {self.total_urls_found}")

            for url in urls_to_process:
                if not self._should_schedule_url(url):
                    continue

                yield Request(
                    url,
                    callback=self.parse_product_page_with_check,
                    meta={'url': url, 'is_ashley': True, 'chunk_id': self.chunk_id},
                    errback=self.handle_product_error,
                    priority=10,
                    dont_filter=True,
                )
            return

        # SITEMAP MODE
        if not hasattr(self, 'all_sitemaps') or not self.all_sitemaps:
            self.logger.error("❌ No sitemaps to process")
            return

        end_idx = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(self.all_sitemaps)
        sitemaps_to_run = self.all_sitemaps[self.sitemap_offset:end_idx]

        self.logger.info(f"🚀 Processing {len(sitemaps_to_run)} sitemaps")
        for sitemap_url in sitemaps_to_run:
            yield Request(
                sitemap_url,
                callback=self.parse_product_sitemap,
                meta={'sitemap_url': sitemap_url},
                errback=self.handle_sitemap_error
            )

    def parse_product_sitemap(self, response):
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            root = ET.fromstring(content)
        else:
            root = ET.fromstring(response.body)

        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        all_urls = [url.text.strip() for url in root.findall('ns:url/ns:loc', ns) if url.text]
        if not all_urls:
            all_urls = [url.text.strip() for url in root.findall('.//loc') if url.text]

        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]

        sitemap_url = response.meta.get('sitemap_url', response.url)
        self.sitemap_urls_count[sitemap_url] = len(all_urls)
        self.total_urls_found += len(all_urls)

        for url in all_urls:
            if self._is_plp_url(url):
                continue
            if not self._should_schedule_url(url):
                continue

            yield Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url, 'sitemap': sitemap_url},
                errback=self.handle_product_error
            )

    def _is_plp_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if not path:
            return True
        return '/' in path

    def parse_product_page_with_check(self, response):
        self.processed_count += 1
        requested_url = response.meta.get('url', response.url)
        requested_normalized = self.normalize_url(requested_url)
        response_normalized = self.normalize_url(response.url)

        if self.processed_count % 100 == 0:
            success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0
            self.logger.info(f"📊 Progress: {self.processed_count}/{self.total_urls_found} | ✅ Success: {self.processed_count - self.failed_count} | ❌ Failed: {self.failed_count} | 📈 Rate: {success_rate:.1f}%")

        if self._is_plp_url(response.url):
            self.unscraped_requests[requested_normalized] = {
                'url': requested_url,
                'reason': 'PLP category URL skipped',
                'status': 'Skipped',
                'error_type': 'PLP_SKIPPED',
                'error_message': 'Page is PLP category page',
                'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            return

        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        has_product_json = False
        for script in json_scripts:
            try:
                data = json.loads(script.strip())
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict) and data.get('@type') == 'Product':
                    has_product_json = True
                    break
            except Exception:
                continue

        if has_product_json:
            if requested_normalized in self.queued_or_processing_urls:
                self.queued_or_processing_urls.discard(requested_normalized)
            if requested_normalized:
                self.processed_successfully_urls.add(requested_normalized)
                self._persist_success_url(requested_normalized)
            if response_normalized:
                self.processed_successfully_urls.add(response_normalized)
                self._persist_success_url(response_normalized)

            yield from self.parse_product_page(response)
        else:
            self.failed_count += 1
            self.failed_requests[requested_normalized] = {
                'url': requested_url,
                'status': 'Failed',
                'error_type': 'NO_PRODUCT_JSON',
                'error_message': 'No Product JSON-LD found on page',
                'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

    def extract_specs_table(self, response) -> dict:
        specs = {}
        try:
            rows = response.xpath('//table[@id="product-attribute-specs-table"]//tr | //table[contains(@class, "additional-attributes")]//tr')
            for row in rows:
                th = row.xpath('./th/text()').get()
                td = row.xpath('./td//text()').get()
                if th and td:
                    label = th.strip()
                    val = td.strip()
                    if label and val:
                        key = re.sub(r'[^a-zA-Z0-9_]', '_', label.lower().replace(' ', '_')).strip('_')
                        specs[key] = val
        except Exception:
            pass
        return specs

    def parse_product_page(self, response):
        requested_url = response.meta.get('url', response.url)
        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()

        product_data = None
        breadcrumb_data = None
        category = ""
        category_url = ""

        for script in json_scripts:
            try:
                data = json.loads(script.strip())
                if isinstance(data, list) and data:
                    data = data[0]
                if not isinstance(data, dict):
                    continue

                schema_type = data.get("@type", "")
                if schema_type == "BreadcrumbList":
                    breadcrumb_data = data
                    items = data.get("itemListElement", [])
                    if items and isinstance(items[0], list):
                        items = items[0]
                    crumbs = []
                    for it in items:
                        if isinstance(it, dict) and "item" in it:
                            c_name = it["item"].get("name", "")
                            c_url = it["item"].get("@id", "")
                            if c_name and c_name != "Emmamason Home" and not c_url.endswith(response.url.split("/")[-1]):
                                crumbs.append((c_name, c_url))
                    if crumbs:
                        category = " > ".join([c[0] for c in crumbs])
                        category_url = crumbs[-1][1]

                elif schema_type == "Product":
                    product_data = data
            except Exception:
                continue

        if not product_data:
            return

        specs = self.extract_specs_table(response)

        selected_offer = product_data.get("offers", {})
        price = ""
        status = "In Stock"
        if isinstance(selected_offer, dict):
            price = selected_offer.get("price", "") or selected_offer.get("lowPrice", "") or ""
            avail = selected_offer.get("availability", "")
            if "OutOfStock" in str(avail):
                status = "Out of Stock"
        elif isinstance(selected_offer, list) and selected_offer:
            first_offer = selected_offer[0]
            if isinstance(first_offer, dict):
                price = first_offer.get("price", "") or first_offer.get("lowPrice", "") or ""
                avail = first_offer.get("availability", "")
                if "OutOfStock" in str(avail):
                    status = "Out of Stock"

        images_raw = product_data.get("image", "")
        all_images = []
        if isinstance(images_raw, list):
            all_images = [str(img).strip() for img in images_raw if str(img).strip()]
        elif isinstance(images_raw, str) and images_raw.strip():
            all_images = [images_raw.strip()]
        main_image = all_images[0] if all_images else ""
        images_str = ", ".join(all_images)

        brand_raw = product_data.get("brand", {})
        brand = brand_raw.get("name", "") if isinstance(brand_raw, dict) else str(brand_raw)
        if not brand and "brand" in specs:
            brand = specs["brand"]

        sku = product_data.get("sku", "") or specs.get("sku", "")
        name = product_data.get("name", "")
        mpn = product_data.get("mpn", "") or specs.get("mpn", "")
        gtin = product_data.get("gtin13", "") or product_data.get("gtin", "") or specs.get("gtin", "")
        description = product_data.get("description", "")
        material = product_data.get("material", "") or specs.get("material", "")
        dimensions = specs.get("dimensions", "") or specs.get("item_dimensions", "") or specs.get("size", "")
        collection_name = specs.get("collection", "") or specs.get("series", "")
        product_type = specs.get("product_type", "") or specs.get("type", "simple")

        raw_json_payload = {
            "json_ld_product": product_data,
            "json_ld_breadcrumb": breadcrumb_data,
            "additional_specs": specs
        }

        item = {
            'Ref Product URL': requested_url,
            'Ref Product ID': sku,
            'Ref Variant ID': '',
            'Ref Category': category,
            'Ref Category URL': category_url,
            'Product Type': product_type,
            'Ref Brand Name': brand,
            'Collection Name': collection_name,
            'Ref Product Name': name,
            'Ref SKU': sku,
            'Ref MPN': mpn,
            'Ref GTIN': gtin,
            'Ref Price': price,
            'Ref Main Image': main_image,
            'Ref Quantity': 1,
            'Ref Group Attr 1': description,
            'Ref Group Attr 2': material,
            'Ref Images': images_str,
            'Ref Dimensions': dimensions,
            'Ref Status': status,
            'Ref Highlights': description,
            'Row JSON': json.dumps(raw_json_payload, ensure_ascii=False),
            'Date Scrapped': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        if self.verbose and sku:
            self.logger.info(f"💾 Extracted: [{sku}] {name[:50]}")

        yield item

    def handle_product_error(self, failure):
        self.failed_count += 1
        req_url = failure.request.url if hasattr(failure, 'request') else ''
        status_code = getattr(failure.value.response, 'status', None) if hasattr(failure, 'value') and hasattr(failure.value, 'response') else None

        if status_code in [404, 301, 302, 410]:
            self.unscraped_requests[req_url] = {
                'url': req_url,
                'reason': f"HTTP {status_code} status",
                'status': 'Unscraped',
                'error_type': f'HTTP_{status_code}',
                'error_message': f'HTTP status {status_code}',
                'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
        else:
            self.failed_requests[req_url] = {
                'url': req_url,
                'status': 'Failed',
                'error_type': f'HTTP_{status_code}' if status_code else 'NETWORK_ERROR',
                'error_message': str(failure.value) if hasattr(failure, 'value') else 'Unknown error',
                'failed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

    def handle_sitemap_error(self, failure):
        self.logger.error(f"❌ Sitemap error: {failure}")

    def closed(self, reason):
        elapsed = time.time() - self.start_time
        success_rate = ((self.processed_count - self.failed_count) / self.processed_count * 100) if self.processed_count > 0 else 0

        remaining_file = None
        if self.failed_requests:
            remaining_file = self._get_remaining_file_path()
            os.makedirs(os.path.dirname(remaining_file) or ".", exist_ok=True)
            with open(remaining_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["url", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]
                )
                writer.writeheader()
                for row in self.failed_requests.values():
                    writer.writerow({
                        "url": row.get("url", ""),
                        "status": row.get("status", ""),
                        "error_type": row.get("error_type", ""),
                        "error_message": row.get("error_message", ""),
                        "failed_at": row.get("failed_at", ""),
                        "job_id": self.job_id,
                        "chunk_id": self.chunk_id,
                    })

        unscraped_file = None
        if self.unscraped_requests:
            unscraped_file = self._get_unscraped_file_path()
            os.makedirs(os.path.dirname(unscraped_file) or ".", exist_ok=True)
            with open(unscraped_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["url", "reason", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]
                )
                writer.writeheader()
                for row in self.unscraped_requests.values():
                    writer.writerow({
                        "url": row.get("url", ""),
                        "reason": row.get("reason", ""),
                        "status": row.get("status", ""),
                        "error_type": row.get("error_type", ""),
                        "error_message": row.get("error_message", ""),
                        "failed_at": row.get("failed_at", ""),
                        "job_id": self.job_id,
                        "chunk_id": self.chunk_id,
                    })

        self.logger.info("=" * 70)
        self.logger.info(f"🏁 FINAL SCRAPING REPORT - Job: {self.job_id}")
        self.logger.info(f"      - Total URLs found: {self.total_urls_found}")
        self.logger.info(f"      - URLs processed: {self.processed_count}")
        self.logger.info(f"      - ✅ Successful: {self.processed_count - self.failed_count}")
        self.logger.info(f"      - ❌ Failed: {self.failed_count}")
        if remaining_file:
            self.logger.info(f"      - 🔁 Remaining file: {remaining_file}")
        if unscraped_file:
            self.logger.info(f"      - 📄 Unscraped file: {unscraped_file}")
        self.logger.info(f"      - Success rate: {success_rate:.1f}%")
        self.logger.info(f"      - Total time: {elapsed:.1f}s")
        self.logger.info("=" * 70)

        if self.success_db_conn:
            try:
                self.success_db_conn.commit()
                self.success_db_conn.close()
            except Exception as e:
                self.logger.error(f"❌ Error closing persistent dedup store: {e}")
