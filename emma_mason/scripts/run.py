import os
import sys
import argparse
import logging
import csv
import json
from pathlib import Path

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from emma_mason.fetcher.product_fetcher import ProductFetcher

def load_urls_from_file(file_path):
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

def main():
    parser = argparse.ArgumentParser(description='Run Emma Mason product scraper (Coleman-style)')
    parser.add_argument('--website-url', default='https://emmamason.com', help='Website URL to scrape')
    parser.add_argument('--sitemap-offset', type=int, default=int(os.getenv('SITEMAP_OFFSET', '0')), help='Offset for sitemap processing')
    parser.add_argument('--max-sitemaps', type=int, default=int(os.getenv('MAX_SITEMAPS', '0')), help='Maximum sitemaps to process (0 for all)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=int(os.getenv('MAX_URLS_PER_SITEMAP', '0')), help='Maximum URLs per sitemap (0 for all)')
    parser.add_argument('--job-id', default='job0_r1', help='Job identifier for output file')
    parser.add_argument('--output-dir', default='output', help='Output directory for CSV files')
    parser.add_argument('--max-workers', type=int, default=int(os.getenv('MAX_WORKERS', '8')), help='Max concurrent worker threads')
    parser.add_argument('--request-delay', type=float, default=float(os.getenv('REQUEST_DELAY', '0.1')), help='Delay between requests in seconds')
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    parser.add_argument('--urls-file', default='', help='Optional file (csv/json/txt) with URLs for chunk mode')

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    input_urls = []
    if args.urls_file:
        input_urls = load_urls_from_file(args.urls_file)
        logger.info(f"🔁 Loaded {len(input_urls)} URLs from file: {args.urls_file}")

    fetcher = ProductFetcher(
        website_url=args.website_url,
        ashley_urls=input_urls,
        is_ashley=bool(args.urls_file),
        sitemap_offset=args.sitemap_offset,
        max_sitemaps=args.max_sitemaps,
        max_urls_per_sitemap=args.max_urls_per_sitemap,
        job_id=args.job_id,
        output_dir=args.output_dir,
        max_workers=args.max_workers,
        request_delay=args.request_delay,
        verbose=args.verbose
    )

    output_file = fetcher.run()
    return output_file

if __name__ == '__main__':
    main()
