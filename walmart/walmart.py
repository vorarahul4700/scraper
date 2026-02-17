import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ================= ENV =================

CURR_URL        = os.getenv("CURR_URL", "https://www.walmart.com").rstrip("/")
API_BASE_URL    = os.getenv("API_BASE_URL", "").rstrip("/")          # optional secondary API
SITEMAP_OFFSET  = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS    = int(os.getenv("MAX_SITEMAPS", "0"))                # 0 = all
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))  # 0 = all
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY   = float(os.getenv("REQUEST_DELAY", "1.0"))

OUTPUT_CSV   = f"walmart_products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})


def http_get(url: str, is_json: bool = False) -> Optional[str]:
    """HTTP GET with retry logic. Uses JSON headers when is_json=True."""
    for attempt in range(3):
        try:
            if is_json:
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": f"{CURR_URL}/",
                    "X-Requested-With": "XMLHttpRequest",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
                r = session.get(url, headers=headers, timeout=15, verify=True)
            else:
                r = session.get(url, timeout=15, verify=True)

            if r.status_code == 200:
                log(f"Success fetching {url}", "DEBUG")
                return r.text
            else:
                log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:
                    time.sleep(5)
        except requests.exceptions.Timeout:
            log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
            time.sleep(2)
        except Exception as e:
            log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
            time.sleep(1)
    return None

# ================= SITEMAP =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Fetch and parse XML sitemap."""
    data = None
    for attempt in range(3):
        try:
            data = http_get(url, is_json=False)
            if data:
                break
        except Exception as e:
            log(f"Attempt {attempt+1} for sitemap failed: {e}", "WARNING")
            time.sleep(2)

    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None

    try:
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}", "ERROR")
        try:
            root = ET.Element("urlset")
            for url_text in re.findall(r'<loc>(https?://[^<]+)</loc>', data):
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text
            return root
        except Exception as e2:
            log(f"Regex extraction failed: {e2}", "ERROR")
            return None

# ================= HELPERS =================

def extract_product_id(url: str) -> Optional[str]:
    """
    Extract Walmart product ID (item number) from URL.
    Handles patterns like:
      /ip/product-name/123456789
      /ip/123456789
    """
    if not url:
        return None
    # Pattern: /ip/.../DIGITS  or  /ip/DIGITS
    match = re.search(r'/ip/(?:[^/]+/)?(\d+)', url)
    if match:
        return match.group(1)
    # Fallback: last segment if numeric
    last = url.rstrip("/").split("/")[-1].split("?")[0]
    if last.isdigit():
        return last
    log(f"No product ID found in URL: {url}", "WARNING")
    return None


def clean_url(url: str) -> str:
    """Strip query params and trailing slashes."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


def normalize_image_url(url: str) -> str:
    """Ensure image URL is absolute."""
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return f"{CURR_URL}{url}"
    if not url.startswith("http"):
        return f"https://{url}"
    return url

# ================= WALMART DATA EXTRACTION =================

def extract_walmart_data(soup: BeautifulSoup, url: str) -> List[Dict]:
    """
    Parse JSON-LD from the Walmart product page.
    Returns a list of product dicts (one per variant that matches the URL,
    or all variants when doing a bulk crawl).
    """
    product_id_from_url = extract_product_id(url)
    results = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string
            if not raw:
                continue
            data = json.loads(raw)

            # Normalise to a single root object
            if isinstance(data, list):
                data = data[0]

            if not isinstance(data, dict):
                continue

            # Decide whether we have variants (hasVariant) or a single item
            variants = data.get("hasVariant") or [data]

            for variant in variants:
                if not isinstance(variant, dict):
                    continue

                # ---------- Offers ----------
                offers = variant.get("offers", {})
                selected_offer = {}

                if isinstance(offers, list):
                    # Pick the offer whose URL matches the page URL
                    for offer in offers:
                        offer_url = clean_url(offer.get("url", ""))
                        if extract_product_id(offer_url) == product_id_from_url:
                            selected_offer = offer
                            break
                    # Fallback: use first offer
                    if not selected_offer and offers:
                        selected_offer = offers[0]
                elif isinstance(offers, dict):
                    selected_offer = offers

                # ---------- Images ----------
                images = variant.get("image", "")
                if isinstance(images, list):
                    main_image = images[0] if images else ""
                else:
                    main_image = images or ""

                # ---------- Price ----------
                price = (
                    selected_offer.get("price", "")
                    or selected_offer.get("lowPrice", "")
                    or data.get("offers", {}).get("price", "") if isinstance(data.get("offers"), dict) else ""
                )

                # ---------- Status ----------
                availability = selected_offer.get("availability", "")
                if "InStock" in availability or "InStock" in variant.get("availability", ""):
                    status = "In Stock"
                else:
                    status = "Out of Stock"

                product_info = {
                    "competitor_product_id": product_id_from_url or "",
                    "comp_received_name":    variant.get("name", ""),
                    "comp_received_sku":     variant.get("sku", ""),
                    "brand": (
                        variant.get("brand", {}).get("name", "")
                        if isinstance(variant.get("brand"), dict)
                        else str(variant.get("brand", ""))
                    ),
                    "mpn":          variant.get("model", "") or variant.get("mpn", ""),
                    "category":     "",
                    "category_url": "",
                    "gtin":         variant.get("gtin13", "") or variant.get("gtin", ""),
                    "variation_id": variant.get("variationId", ""),
                    "quantity":     variant.get("inventory", {}).get("quantityAvailable", 0),
                    "status":       status,
                    "competitor_price": price,
                    "group_attr_1": variant.get("description", ""),
                    "group_attr_2": variant.get("color", ""),
                    "main_image":   main_image,
                    "competitor_url": url,
                    "scraped_date": SCRAPED_DATE,
                }
                results.append(product_info)

            # Stop after the first valid JSON-LD block
            if results:
                return results

        except (json.JSONDecodeError, AttributeError) as e:
            log(f"JSON-LD parse error: {e}", "WARNING")
            continue

    return results

# ================= CSV WRITE =================

csv_lock = threading.Lock()

CSV_HEADER = [
    "Ref Product URL",
    "Ref Product ID",
    "Ref Variant ID",
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
    "Date Scrapped",
]


def write_row(writer, product: Dict):
    row = [
        product["competitor_url"],
        product["competitor_product_id"],
        product["variation_id"],
        product["category"],
        product["category_url"],
        product["brand"],
        product["comp_received_name"],
        product["comp_received_sku"],
        product["mpn"],
        product["gtin"],
        product["competitor_price"],
        normalize_image_url(product["main_image"]),
        product["quantity"],
        product["group_attr_1"],
        product["group_attr_2"],
        product["status"],
        product["scraped_date"],
    ]
    with csv_lock:
        writer.writerow(row)

# ================= FAILURE CSV =================

failure_csv_lock = threading.Lock()

def log_failure(url: str, reason: str):
    """Append a failed URL to a dedicated failure CSV."""
    output_dir = "media/output/scrapping/failure_csv"
    os.makedirs(output_dir, exist_ok=True)
    failure_path = os.path.join(output_dir, "Walmart_failures.csv")
    file_exists = os.path.isfile(failure_path)
    with failure_csv_lock:
        with open(failure_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["URL", "Reason", "Timestamp"])
            writer.writerow([url, reason, SCRAPED_DATE])

# ================= PROCESS SINGLE PRODUCT =================

def process_product(product_url: str, writer, seen: set, stats: dict):
    """Fetch and process a single Walmart product URL."""
    base_url = clean_url(product_url)

    if base_url in seen:
        return
    seen.add(base_url)

    log(f"Processing: {base_url}", "DEBUG")

    # Validate product ID
    product_id = extract_product_id(base_url)
    if not product_id:
        stats["errors"] += 1
        log_failure(base_url, "No product ID extracted")
        return

    # Fetch page HTML
    html = http_get(base_url, is_json=False)
    if not html:
        stats["errors"] += 1
        log_failure(base_url, "HTTP fetch failed")
        return

    soup = BeautifulSoup(html, "html.parser")
    products = extract_walmart_data(soup, base_url)

    if not products:
        stats["errors"] += 1
        log_failure(base_url, "No product data extracted from JSON-LD")
        return

    for product in products:
        if not product.get("comp_received_name"):
            continue
        try:
            write_row(writer, product)
            stats["products_fetched"] += 1
            log(
                f"Saved: [{product['competitor_product_id']}] "
                f"{product['comp_received_name'][:60]}",
                "INFO",
            )
        except Exception as e:
            log(f"Row write error for {product_id}: {e}", "ERROR")
            stats["errors"] += 1

    time.sleep(REQUEST_DELAY)
    stats["urls_processed"] += 1

# ================= MAIN =================

def main():

    sitemap_index_url = f"{CURR_URL}/sitemap_hi_ip.xml"
    # log(f"Falling back to default sitemap: {sitemap_index_url}", "WARNING")

    log("=" * 60)
    log("Walmart Parallel Bulk Scraper")
    log(f"Timestamp:            {SCRAPED_DATE}")
    log(f"Base URL:             {CURR_URL}")
    log(f"Sitemap Index:        {sitemap_index_url}")
    log(f"Sitemap Offset:       {SITEMAP_OFFSET}")
    log(f"Max Sitemaps:         {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers:          {MAX_WORKERS}")
    log(f"Request Delay:        {REQUEST_DELAY}s")
    log("=" * 60)

    # Load sitemap index
    index = load_xml(sitemap_index_url)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []

    # Try standard XML namespace paths first, then plain, then regex
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = (
            index.findall(path, ns) if "ns:" in path else index.findall(path)
        )
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break

    if not sitemaps:
        log("No child sitemaps found; treating sitemap index as product sitemap", "WARNING")
        sitemaps = [sitemap_index_url]

    # Apply offset + limit
    if SITEMAP_OFFSET >= len(sitemaps):
        log(
            f"Offset {SITEMAP_OFFSET} >= total sitemaps ({len(sitemaps)}), nothing to do",
            "WARNING",
        )
        sys.exit(0)

    end_idx = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_idx]

    log(f"Total sitemaps found:   {len(sitemaps)}")
    log(f"Sitemaps to process:    {len(sitemaps_to_process)}")

    # ---- Open output CSV ----
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)

        seen: set = set()
        stats = {
            "sitemaps_processed": 0,
            "urls_processed": 0,
            "products_fetched": 0,
            "errors": 0,
        }

        for sitemap_url in sitemaps_to_process:
            stats["sitemaps_processed"] += 1
            log(
                f"Sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: "
                f"{sitemap_url}"
            )

            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Skipping unreachable sitemap: {sitemap_url}", "ERROR")
                continue

            # Extract only Walmart product URLs (/ip/ pattern)
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = (
                    xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                )
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text and "/ip/" in e.text
                    ]
                    if urls:
                        break

            if not urls:
                log(f"No Walmart product URLs (/ip/) found in: {sitemap_url}", "WARNING")
                continue

            if MAX_URLS_PER_SITEMAP > 0:
                original = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} of {original} URLs")
            else:
                log(f"Found {len(urls)} product URLs")

            # ---- Parallel processing ----
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product, url, writer, seen, stats)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Thread error: {e}", "ERROR")
                        stats["errors"] += 1

            gc.collect()

    # ---- Summary ----
    log("=" * 60)
    log("SCRAPING COMPLETE")
    log(f"  Sitemaps processed:  {stats['sitemaps_processed']}")
    log(f"  URLs processed:      {stats['urls_processed']}")
    log(f"  Products saved:      {stats['products_fetched']}")
    log(f"  Errors:              {stats['errors']}")
    if stats["urls_processed"] > 0:
        rate = stats["products_fetched"] / stats["urls_processed"] * 100
        log(f"  Success rate:        {rate:.1f}%")
    log(f"  Output:              {OUTPUT_CSV}")
    log("=" * 60)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not CURR_URL:
        log("CURR_URL environment variable is required", "ERROR")
        sys.exit(1)

    main()