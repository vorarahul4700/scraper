import scrapy
from scrapy.spiders import SitemapSpider
from scrapy.http import Request
import csv
import os
from urllib.parse import urlparse
import logging

class FurniturepickSpider(SitemapSpider):
    name = "furniturepick"
    
    def __init__(self, *args, **kwargs):
        # Get environment variables or use defaults
        self.base_url = kwargs.get('base_url', os.environ.get('CURR_URL', 'https://www.furniturepick.com'))
        self.sitemap_offset = int(kwargs.get('offset', os.environ.get('SITEMAP_OFFSET', '0')))
        self.max_sitemaps = int(kwargs.get('max_sitemaps', os.environ.get('MAX_SITEMAPS', '2')))
        self.max_urls_per_sitemap = int(kwargs.get('max_urls_per_sitemap', os.environ.get('MAX_URLS_PER_SITEMAP', '100')))
        self.request_delay = float(kwargs.get('request_delay', os.environ.get('REQUEST_DELAY', '1.0')))
        
        # Set download delay
        self.download_delay = self.request_delay
        
        # Initialize sitemap URLs
        self.sitemap_urls = []
        
        # Output file
        self.chunk_num = self.sitemap_offset // int(os.environ.get('SITEMAPS_PER_JOB', '2'))
        self.output_file = f"products_chunk_{self.chunk_num:03d}.csv"
        
        # Initialize CSV file with headers
        self.init_csv()
        
        super().__init__(*args, **kwargs)
    
    def init_csv(self):
        """Initialize CSV file with headers"""
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'url',
                    'product_name',
                    'price',
                    'sku',
                    'brand',
                    'availability',
                    'category',
                    'description',
                    'image_urls',
                    'timestamp'
                ])
    
    def start_requests(self):
        """Start by fetching robots.txt to find sitemaps"""
        robots_url = f"{self.base_url}/robots.txt"
        yield Request(
            robots_url,
            callback=self.parse_robots,
            errback=self.handle_error,
            meta={'dont_retry': False}
        )
    
    def parse_robots(self, response):
        """Parse robots.txt to find sitemap URLs"""
        if response.status == 200:
            # Extract sitemap URLs from robots.txt
            for line in response.text.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    self.sitemap_urls.append(sitemap_url)
                    self.logger.info(f"Found sitemap: {sitemap_url}")
        
        # If no sitemaps found in robots.txt, try common locations
        if not self.sitemap_urls:
            common_sitemaps = [
                f"{self.base_url}/sitemap.xml",
                f"{self.base_url}/sitemap_index.xml",
                f"{self.base_url}/sitemap/sitemap.xml",
                f"{self.base_url}/sitemap/sitemap-index.xml",
            ]
            self.sitemap_urls.extend(common_sitemaps)
        
        # Process sitemaps with offset and limit
        start_idx = self.sitemap_offset
        end_idx = min(start_idx + self.max_sitemaps, len(self.sitemap_urls))
        
        self.logger.info(f"Processing sitemaps {start_idx} to {end_idx} (offset: {self.sitemap_offset}, limit: {self.max_sitemaps})")
        
        for i in range(start_idx, end_idx):
            yield Request(
                self.sitemap_urls[i],
                callback=self.parse_sitemap_index,
                errback=self.handle_error,
                meta={'sitemap_url': self.sitemap_urls[i]}
            )
    
    def parse_sitemap_index(self, response):
        """Parse sitemap index file or individual sitemap"""
        sitemap_url = response.meta.get('sitemap_url', response.url)
        
        # Check if it's a sitemap index
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Try to parse as XML
        try:
            # Check for sitemap index
            sitemaps = response.xpath('//sm:sitemap | //sitemap')
            if sitemaps:
                for sitemap in sitemaps:
                    loc = sitemap.xpath('.//sm:loc | .//loc').extract_first()
                    if loc:
                        yield Request(
                            loc,
                            callback=self.parse_product_sitemap,
                            errback=self.handle_error,
                            meta={'sitemap_url': loc}
                        )
            else:
                # Treat as regular sitemap
                yield from self.parse_product_sitemap(response)
                
        except Exception as e:
            self.logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
    
    def parse_product_sitemap(self, response):
        """Parse product URLs from sitemap"""
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Extract all URLs from sitemap
        urls = response.xpath('//sm:url | //url')
        
        product_urls = []
        url_count = 0
        
        for url in urls:
            loc = url.xpath('.//sm:loc | .//loc').extract_first()
            if loc:
                # Filter for product URLs
                if '/product/' in loc or '/item/' in loc or '/p/' in loc:
                    product_urls.append(loc)
                    url_count += 1
                    
                    if url_count >= self.max_urls_per_sitemap:
                        break
        
        self.logger.info(f"Found {len(product_urls)} product URLs in {response.url}")
        
        # Save product URLs to CSV
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for url in product_urls:
                writer.writerow([
                    url,
                    '',  # product_name
                    '',  # price
                    '',  # sku
                    '',  # brand
                    '',  # availability
                    '',  # category
                    '',  # description
                    '',  # image_urls
                    '',  # timestamp
                ])
        
        # Optionally scrape product details
        if os.environ.get('SCRAPE_DETAILS', 'false').lower() == 'true':
            for url in product_urls[:10]:  # Limit for demo
                yield Request(
                    url,
                    callback=self.parse_product,
                    errback=self.handle_error,
                    meta={'product_url': url}
                )
    
    def parse_product(self, response):
        """Parse individual product page"""
        # This is a basic implementation - you'll need to adjust selectors
        # based on the actual HTML structure of furniturepick.com
        
        product_data = {
            'url': response.url,
            'product_name': response.xpath('//h1/text()').extract_first(),
            'price': response.xpath('//span[@class="price"]/text()').extract_first(),
            'sku': response.xpath('//span[@itemprop="sku"]/text()').extract_first(),
            'brand': response.xpath('//span[@itemprop="brand"]/text()').extract_first(),
            'availability': response.xpath('//link[@itemprop="availability"]/@href').extract_first(),
            'category': ' > '.join(response.xpath('//nav[@aria-label="breadcrumb"]//li//text()').extract()),
            'description': response.xpath('//meta[@name="description"]/@content').extract_first(),
            'image_urls': ', '.join(response.xpath('//img[@class="product-image"]/@src').extract()),
            'timestamp': scrapy.utils.response.response_httprepr(response).decode('utf-8').split('\r\n')[0],
        }
        
        # Update CSV row
        self.update_csv_product(product_data)
        
        yield product_data
    
    def update_csv_product(self, product_data):
        """Update product details in CSV"""
        # Read all rows
        rows = []
        updated = False
        
        with open(self.output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows.append(header)
            
            for row in reader:
                if row[0] == product_data['url']:
                    # Update this row with product details
                    row[1] = product_data.get('product_name', '')
                    row[2] = product_data.get('price', '')
                    row[3] = product_data.get('sku', '')
                    row[4] = product_data.get('brand', '')
                    row[5] = product_data.get('availability', '')
                    row[6] = product_data.get('category', '')
                    row[7] = product_data.get('description', '')
                    row[8] = product_data.get('image_urls', '')
                    row[9] = product_data.get('timestamp', '')
                    updated = True
                rows.append(row)
        
        if updated:
            # Write back to CSV
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(rows)
    
    def handle_error(self, failure):
        """Handle request errors"""
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(f"Error: {repr(failure.value)}")