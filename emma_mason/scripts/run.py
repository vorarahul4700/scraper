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

logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("twisted").propagate = False
logging.getLogger("scrapy.core.engine").setLevel(logging.WARNING)
logging.getLogger("scrapy.dupefilter").setLevel(logging.WARNING)
logging.getLogger("scrapy.downloadermiddlewares").setLevel(logging.WARNING)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
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
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose logging')
    parser.add_argument('--urls-file', default='', help='Optional file (csv/json/txt) with URLs for chunk mode')

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    os.environ['SCRAPY_SETTINGS_MODULE'] = 'emma_mason.settings'
    settings = get_project_settings()

    timestamp = os.getenv('GITHUB_RUN_ID', 'local')
    domain = args.website_url.replace('https://', '').replace('http://', '').split('/')[0].replace('.', '_')
    output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'

    settings.set('FEED_URI', output_file)
    settings.set('FEED_FORMAT', 'csv')

    max_workers = int(os.getenv('MAX_WORKERS', '16'))
    settings.set('CONCURRENT_REQUESTS', max_workers)

    download_delay = float(os.getenv('DOWNLOAD_DELAY', '0.1'))
    settings.set('DOWNLOAD_DELAY', download_delay)

    if args.verbose:
        settings.set('LOG_LEVEL', 'INFO')
        settings.set('LOG_ENABLED', True)
        logger.info("✅ Verbose logging enabled")
    else:
        settings.set('LOG_LEVEL', 'WARNING')
        settings.set('LOG_ENABLED', True)

    settings.set('FEED_EXPORT_FIELDS', [
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
    ])
    settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')

    process = CrawlerProcess(settings)

    logger.info(f"🚀 Starting scraper for: {args.website_url}")
    logger.info(f"📁 Output will be saved to: {output_file}")
    logger.info(f"🔧 Concurrency: {max_workers} workers, delay={download_delay}s")

    if args.urls_file:
        input_urls = load_urls_from_file(args.urls_file)
        if not input_urls:
            logger.warning(f"No URLs found in urls-file: {args.urls_file}")
            return output_file

        logger.info(f"🔁 Chunk mode enabled with {len(input_urls)} URLs from: {args.urls_file}")
        process.crawl(
            ProductFetcher,
            website_url=args.website_url,
            ashley_urls=input_urls,
            is_ashley=True,
            chunk_mode=False,
            job_id=args.job_id,
            output_dir=args.output_dir,
            verbose=args.verbose
        )
    else:
        process.crawl(
            ProductFetcher,
            website_url=args.website_url,
            sitemap_offset=args.sitemap_offset,
            max_sitemaps=args.max_sitemaps,
            max_urls_per_sitemap=args.max_urls_per_sitemap,
            job_id=args.job_id,
            output_dir=args.output_dir,
            verbose=args.verbose
        )

    process.start()
    logger.info(f"✅ Scraping completed. Output saved to: {output_file}")
    return output_file

if __name__ == '__main__':
    main()
