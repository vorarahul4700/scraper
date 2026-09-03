import os
import csv
import time
import sys
import gc
import threading
import argparse
from curl_cffi import requests
import re
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_urls_from_file(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"URLs file not found: {file_path}")

    if file_path.lower().endswith(".json"):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            urls = data.get("urls", [])
        elif isinstance(data, list):
            urls = data
        else:
            urls = []
        return [str(u).strip() for u in urls if str(u).strip()]

    if file_path.lower().endswith(".csv"):
        urls = []
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (
                    row.get("url")
                    or row.get("Ref Product URL")
                    or row.get("URL")
                    or row.get("item_url")
                    or row.get("product_url")
                    or row.get("link")
                    or ""
                ).strip()
                if url:
                    urls.append(url)
        if not urls:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip().startswith("http"):
                        urls.append(row[0].strip())
        return urls

    urls = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url and url.startswith("http"):
                urls.append(url)
    return urls


class emmamasonScraper:

    def __init__(self, args):
        self.args = args
        raw_url = (args.website_url or os.getenv("CURR_URL", "https://www.emmamason.com")).strip()
        parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
        self.base_domain = f"{parsed.scheme}://{parsed.netloc}"
        
        if raw_url.endswith(".xml"):
            self.sitemap_input = raw_url
            self.curr_url = self.base_domain
        else:
            self.curr_url = raw_url.rstrip("/")
            self.sitemap_input = f"{self.curr_url}/sitemap.xml"

        self.urls_file = args.urls_file
        self.job_id = args.job_id or os.getenv("GITHUB_JOB", "job0_r1")
        self.output_dir = args.output_dir or "output"
        self.max_workers = args.max_workers or int(os.getenv("MAX_WORKERS", "8"))
        self.request_delay = args.request_delay if args.request_delay is not None else float(os.getenv("REQUEST_DELAY", "0.2"))
        self.verbose = args.verbose
        
        self.sitemap_offset = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))

        self.timestamp = os.getenv("GITHUB_RUN_ID", "local")
        self.domain = self.curr_url.replace("https://", "").replace("http://", "").split("/")[0].replace(".", "_")
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        os.makedirs(self.output_dir, exist_ok=True)
        self.output_csv = os.path.join(self.output_dir, f"output_{self.domain}_{self.job_id}_{self.timestamp}.csv")
        self.remaining_csv = os.path.join(self.output_dir, f"remaining_{self.domain}_{self.job_id}_{self.timestamp}.csv")
        self.unscraped_csv = os.path.join(self.output_dir, f"unscraped_{self.domain}_{self.job_id}_{self.timestamp}.csv")

        self.csv_header = [
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
            "Ref Category",
            "Ref Category URL",
            "Product Type",
            "Ref Brand Name",
            "Collection Name",
            "Ref Product Name",
            "Ref SKU",
            "Ref MPN",
            "Ref GTIN",
            "Ref Price",
            "Ref Main Image",
            "Ref Quantity",
            "Ref Group Attr 1",
            "Ref Group Attr 2",
            "Ref Images",
            "Ref Dimensions",
            "Ref Status",
            "Ref Highlights",
            "Row JSON",
            "Date Scrapped",
        ]

        self.remaining_header = ["url", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]
        self.unscraped_header = ["url", "reason", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]

        self.csv_lock = threading.Lock()
        self.rem_lock = threading.Lock()
        self.uns_lock = threading.Lock()
        self.seen = set()
        self.stats = {
            "urls_processed": 0,
            "products_fetched": 0,
            "errors": 0,
            "plp_urls_skipped": 0,
        }

        self.session = requests.Session()
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24", "Brave";v="125"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "cross-site",
            "sec-fetch-user": "?1",
            "sec-gpc": "1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        }
        self.session.headers.update(self.headers)

    def log(self, msg: str, level: str = "INFO"):
        if not self.verbose and level == "DEBUG":
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()

    def http_get(self, url: str) -> Optional[str]:
        impersonate_targets = ["chrome124", "chrome120", "safari17_0"]
        for attempt in range(3):
            impersonate = impersonate_targets[attempt % len(impersonate_targets)]
            try:
                r = self.session.get(
                    url,
                    timeout=20,
                    verify=True,
                    impersonate=impersonate,
                )

                if r.status_code == 200:
                    self.log(f"Success: {url}", "DEBUG")
                    return r.text

                self.log(f"Status {r.status_code} for {url} (impersonate={impersonate})", "WARNING")
                if r.status_code in [403, 429]:
                    time.sleep(2 * (attempt + 1))

            except requests.exceptions.Timeout:
                self.log(f"Timeout attempt {attempt + 1} for {url}", "WARNING")
                time.sleep(2)
            except Exception as e:
                self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__} ({e})", "WARNING")
                time.sleep(1)

        return None

    def clean_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

    def normalize_image(self, url: str) -> str:
        if not url:
            return ""
        if url.startswith("//"):
            return "https:" + url
        if url.startswith("/"):
            return f"{self.curr_url}{url}"
        if not url.startswith("http"):
            return f"https://{url}"
        return url

    def extract_specs_table(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs = {}
        try:
            table = soup.find('table', id='product-attribute-specs-table') or soup.find('table', class_='additional-attributes')
            if table:
                for row in table.find_all('tr'):
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        label = th.get_text(strip=True)
                        value = td.get_text(strip=True)
                        if label and value:
                            key = re.sub(r'[^a-zA-Z0-9_]', '_', label.lower().replace(' ', '_')).strip('_')
                            specs[key] = value
        except Exception:
            pass
        return specs

    def extract_emmamason_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Parse JSON-LD blocks and HTML specs from an emmamason page. Returns list of product dicts."""
        results: List[Dict] = []
        category = ""
        category_url = ""
        product_data = None
        breadcrumb_data = None
        all_json_ld = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)
                all_json_ld.append(data)

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
                            if c_name and c_name != "Emmamason Home" and not c_url.endswith(url.split("/")[-1]):
                                crumbs.append((c_name, c_url))
                    if crumbs:
                        category = " > ".join([c[0] for c in crumbs])
                        category_url = crumbs[-1][1]

                elif schema_type == "Product":
                    product_data = data

            except (json.JSONDecodeError, AttributeError, TypeError) as e:
                self.log(f"JSON-LD parse error: {e}", "WARNING")
                continue

        if not product_data:
            return results

        specs = self.extract_specs_table(soup)

        selected_offer = product_data.get("offers", {})
        price = ""
        status = "In Stock"
        if isinstance(selected_offer, dict):
            price = (
                selected_offer.get("price", "")
                or selected_offer.get("lowPrice", "")
                or ""
            )
            avail = selected_offer.get("availability", "")
            if "OutOfStock" in str(avail):
                status = "Out of Stock"
        elif isinstance(selected_offer, list) and selected_offer:
            first_offer = selected_offer[0]
            if isinstance(first_offer, dict):
                price = first_offer.get("price", "") or first_offer.get("lowPrice", "")
                avail = first_offer.get("availability", "")
                if "OutOfStock" in str(avail):
                    status = "Out of Stock"

        # ---- Images ----
        images_raw = product_data.get("image", "")
        all_images = []
        if isinstance(images_raw, list):
            all_images = [self.normalize_image(str(img)) for img in images_raw if str(img).strip()]
        elif isinstance(images_raw, str) and images_raw.strip():
            all_images = [self.normalize_image(images_raw.strip())]
        main_image = all_images[0] if all_images else ""
        images_str = ", ".join(all_images)

        # ---- Brand ----
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
        product_type = specs.get("product_type", "") or specs.get("type", "")

        # Combine complete raw JSON object
        raw_json_payload = {
            "json_ld_product": product_data,
            "json_ld_breadcrumb": breadcrumb_data,
            "additional_specs": specs
        }
        row_json_str = json.dumps(raw_json_payload, ensure_ascii=False)

        if name:
            results.append({
                "competitor_url":        url,
                "competitor_product_id": sku,
                "variant_id":            "",
                "category":              category,
                "category_url":          category_url,
                "product_type":          product_type,
                "brand":                 brand,
                "collection_name":       collection_name,
                "comp_received_name":    name,
                "comp_received_sku":     sku,
                "mpn":                   mpn,
                "gtin":                  gtin,
                "competitor_price":      price,
                "main_image":            main_image,
                "quantity":              1,
                "group_attr_1":          description,
                "group_attr_2":          material,
                "images":                images_str,
                "dimensions":            dimensions,
                "status":                status,
                "highlights":            description,
                "row_json":              row_json_str,
                "scraped_date":          self.scraped_date,
            })

        return results

    def write_row(self, writer: csv.writer, product: Dict):
        row = [
            product["competitor_url"],
            product["competitor_product_id"],
            product.get("variant_id", ""),
            product["category"],
            product["category_url"],
            product.get("product_type", ""),
            product["brand"],
            product.get("collection_name", ""),
            product["comp_received_name"],
            product["comp_received_sku"],
            product["mpn"],
            product["gtin"],
            product["competitor_price"],
            product["main_image"],
            product["quantity"],
            product["group_attr_1"],
            product["group_attr_2"],
            product.get("images", ""),
            product.get("dimensions", ""),
            product["status"],
            product.get("highlights", ""),
            product.get("row_json", ""),
            product["scraped_date"],
        ]
        with self.csv_lock:
            writer.writerow(row)

    def log_remaining(self, rem_writer: csv.writer, url: str, status: str, error_type: str, error_message: str):
        with self.rem_lock:
            rem_writer.writerow([url, status, error_type, error_message, self.scraped_date, self.job_id, self.job_id])

    def log_unscraped(self, uns_writer: csv.writer, url: str, reason: str):
        with self.uns_lock:
            uns_writer.writerow([url, reason, "Skipped", "PLP_SKIPPED", reason, self.scraped_date, self.job_id, self.job_id])

    def _is_plp_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if not path:
            return True
        return '/' in path

    def process_product(self, product_url: str, writer: csv.writer, rem_writer: csv.writer, uns_writer: csv.writer):
        if self._is_plp_url(product_url):
            self.stats["plp_urls_skipped"] += 1
            self.log_unscraped(uns_writer, product_url, "PLP category URL skipped")
            return

        base_url = self.clean_url(product_url)
        if base_url in self.seen:
            return
        self.seen.add(base_url)

        self.log(f"Processing: {base_url}", "DEBUG")

        html = self.http_get(base_url)
        if not html:
            self.stats["errors"] += 1
            self.log_remaining(rem_writer, base_url, "Failed", "HTTP_ERROR", "Failed to retrieve HTML")
            return

        soup = BeautifulSoup(html, "html.parser")
        products = self.extract_emmamason_data(soup, base_url)

        if not products:
            self.stats["errors"] += 1
            self.log_remaining(rem_writer, base_url, "Failed", "PARSE_ERROR", "No JSON-LD product data found")
            return

        for product in products:
            if not product.get("comp_received_name"):
                continue
            try:
                self.write_row(writer, product)
                self.stats["products_fetched"] += 1
                self.log(
                    f"Saved [{product['competitor_product_id']}] "
                    f"{product['comp_received_name'][:60]}"
                )
            except Exception as e:
                self.stats["errors"] += 1
                self.log_remaining(rem_writer, base_url, "Failed", "WRITE_ERROR", str(e))

        time.sleep(self.request_delay)
        self.stats["urls_processed"] += 1

    def run(self):
        self.log("=" * 60)
        self.log("Emma Mason Parallel Product Scraper")
        self.log(f"Timestamp:       {self.scraped_date}")
        self.log(f"Base URL:        {self.curr_url}")
        self.log(f"Job ID:          {self.job_id}")
        self.log(f"Max Workers:     {self.max_workers}")
        self.log(f"Request Delay:   {self.request_delay}s")
        self.log(f"Output CSV:      {self.output_csv}")
        self.log("=" * 60)

        out_f = open(self.output_csv, "w", newline="", encoding="utf-8")
        rem_f = open(self.remaining_csv, "w", newline="", encoding="utf-8")
        uns_f = open(self.unscraped_csv, "w", newline="", encoding="utf-8")

        writer = csv.writer(out_f)
        rem_writer = csv.writer(rem_f)
        uns_writer = csv.writer(uns_f)

        writer.writerow(self.csv_header)
        rem_writer.writerow(self.remaining_header)
        uns_writer.writerow(self.unscraped_header)

        try:
            urls_to_process = []
            if self.urls_file:
                urls_to_process = load_urls_from_file(self.urls_file)
                self.log(f"Loaded {len(urls_to_process)} URLs from file: {self.urls_file}")
            else:
                self.log("No URLs file provided. Fetching from sitemap...")
                from fetch_input_urls import fetch_from_sitemap
                urls_to_process = fetch_from_sitemap(self.sitemap_input)
                self.log(f"Extracted {len(urls_to_process)} URLs from sitemap: {self.sitemap_input}")

            if not urls_to_process:
                self.log("No URLs to process. Exiting.", "WARNING")
                return

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self.process_product, url, writer, rem_writer, uns_writer)
                    for url in urls_to_process
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.log(f"Thread error: {e}", "ERROR")
                        self.stats["errors"] += 1

            gc.collect()

        finally:
            out_f.close()
            rem_f.close()
            uns_f.close()

        self.log("=" * 60)
        self.log("SCRAPING COMPLETE")
        self.log(f"  URLs processed:    {self.stats['urls_processed']}")
        self.log(f"  Products saved:    {self.stats['products_fetched']}")
        self.log(f"  Errors/Failed:     {self.stats['errors']}")
        self.log(f"  PLP URLs skipped:  {self.stats['plp_urls_skipped']}")
        if self.stats["urls_processed"] > 0:
            rate = self.stats["products_fetched"] / self.stats["urls_processed"] * 100
            self.log(f"  Success rate:      {rate:.1f}%")
        self.log(f"  Output CSV:        {self.output_csv}")
        self.log(f"  Remaining CSV:     {self.remaining_csv}")
        self.log(f"  Unscraped CSV:     {self.unscraped_csv}")
        self.log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Run Emma Mason product scraper")
    parser.add_argument("--website-url", default="https://emmamason.com", help="Website URL")
    parser.add_argument("--urls-file", default="", help="Path to CSV/JSON/TXT URLs file")
    parser.add_argument("--job-id", default="job0_r1", help="Job identifier")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--max-workers", type=int, default=8, help="Max worker threads")
    parser.add_argument("--request-delay", type=float, default=0.2, help="Delay between requests in seconds")
    parser.add_argument("--verbose", action="store_true", default=False, help="Enable verbose logging")

    args = parser.parse_args()
    scraper = emmamasonScraper(args)
    scraper.run()

if __name__ == "__main__":
    main()
