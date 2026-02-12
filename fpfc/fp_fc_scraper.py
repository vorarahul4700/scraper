#!/usr/bin/env python3
"""
FP FC Scraper - Main entry point for GitHub Actions workflow
Scrapes product URLs from sitemaps found via robots.txt
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required packages are installed"""
    try:
        import scrapy
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings
        return True
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        logger.info("Installing required packages...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", 
                             "scrapy", "lxml", "cssselect", "requests"])
        return True

def run_spider():
    """Run the Scrapy spider with environment configuration"""
    
    # Get environment variables
    base_url = os.environ.get('CURR_URL', 'https://www.furniturepick.com')
    offset = os.environ.get('SITEMAP_OFFSET', '0')
    max_sitemaps = os.environ.get('MAX_SITEMAPS', '2')
    max_urls = os.environ.get('MAX_URLS_PER_SITEMAP', '100')
    max_workers = os.environ.get('MAX_WORKERS', '4')
    delay = os.environ.get('REQUEST_DELAY', '1.0')
    scrape_details = os.environ.get('SCRAPE_DETAILS', 'false')
    
    logger.info("=" * 60)
    logger.info("FP FC Scraper Started")
    logger.info("=" * 60)
    logger.info(f"Base URL: {base_url}")
    logger.info(f"Sitemap Offset: {offset}")
    logger.info(f"Max Sitemaps: {max_sitemaps}")
    logger.info(f"Max URLs per Sitemap: {max_urls}")
    logger.info(f"Max Workers: {max_workers}")
    logger.info(f"Request Delay: {delay}s")
    logger.info(f"Scrape Details: {scrape_details}")
    logger.info("=" * 60)
    
    # Create scrapy command
    cmd = [
        "scrapy", "runspider",
        "spiders/furniturepick_spider.py",
        "-a", f"base_url={base_url}",
        "-a", f"offset={offset}",
        "-a", f"max_sitemaps={max_sitemaps}",
        "-a", f"max_urls_per_sitemap={max_urls}",
        "-a", f"max_workers={max_workers}",
        "-a", f"request_delay={delay}",
        "-s", "LOG_LEVEL=INFO",
        "-s", f"CONCURRENT_REQUESTS={max_workers}",
        "-s", f"DOWNLOAD_DELAY={delay}",
        "-s", "USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "-s", "ROBOTSTXT_OBEY=False",
    ]
    
    # Run spider
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("Spider completed successfully")
        
        # List generated CSV files
        csv_files = [f for f in os.listdir('.') if f.startswith('products_chunk_') and f.endswith('.csv')]
        
        if csv_files:
            logger.info(f"Generated CSV files: {csv_files}")
            
            # Count total URLs
            total_urls = 0
            for csv_file in csv_files:
                try:
                    with open(csv_file, 'r', encoding='utf-8') as f:
                        urls = sum(1 for line in f) - 1  # Subtract header
                        total_urls += urls
                    logger.info(f"  {csv_file}: {urls} product URLs")
                except:
                    pass
            
            logger.info(f"Total product URLs found: {total_urls}")
        else:
            logger.warning("No CSV files were generated")
        
        return 0
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Spider failed with exit code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        return 1

def main():
    """Main entry point"""
    try:
        # Check and install dependencies
        if not check_dependencies():
            logger.error("Failed to install dependencies")
            return 1
        
        # Run the spider
        return run_spider()
        
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())