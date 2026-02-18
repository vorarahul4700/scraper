import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
import urllib3
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class LuxeDecorScraper:
    """Class-based scraper for luxedecor.com"""
    
    def __init__(self):
        """Initialize the scraper with configuration from environment variables"""
        # Environment variables
        self.curr_url = os.getenv("CURR_URL", "https://www.luxedecor.com").rstrip("/")
        self.api_base_url = os.getenv("API_BASE_URL", "https://www.luxedecor.com/api/product").rstrip("/")
        self.sitemap_offset = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay = float(os.getenv("REQUEST_DELAY", "2.0"))
        
        # Rate limiting configuration
        self.initial_delay = 2.0
        self.max_delay = 60.0
        self.current_delay = self.initial_delay
        
        # Output file
        self.output_csv = f"luxedecor_products_chunk_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        # Statistics
        self.stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0,
            'rate_limits': 0
        }
        
        # Thread lock for CSV writing
        self.csv_lock = threading.Lock()
        
        # Session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        
        self.log("=" * 60)
        self.log("LuxeDecor.com Scraper Initialized")
        self.log(f"Timestamp: {self.scraped_date}")
        self.log(f"Base URL: {self.curr_url}")
        self.log(f"API Base URL: {self.api_base_url}")
        self.log(f"Sitemap Offset: {self.sitemap_offset}")
        self.log(f"Max Sitemaps: {self.max_sitemaps if self.max_sitemaps > 0 else 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap if self.max_urls_per_sitemap > 0 else 'All'}")
        self.log(f"Max Workers: {self.max_workers}")
        self.log(f"Request Delay: {self.request_delay}s")
        self.log("=" * 60)
    
    def log(self, msg: str, level: str = "INFO"):
        """Log message to stderr"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()
    
    def handle_rate_limit(self, url: str):
        """Handle rate limiting with exponential backoff"""
        self.stats['rate_limits'] += 1
        self.current_delay = min(self.current_delay * 2, self.max_delay)
        self.log(f"Rate limited on {url}, backing off for {self.current_delay}s (total rate limits: {self.stats['rate_limits']})", "WARNING")
        time.sleep(self.current_delay)
    
    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """HTTP GET request with retry logic and rate limit handling"""
        for attempt in range(5):  # Increased retries
            try:
                if is_json:
                    headers = {
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{self.curr_url}/",
                    }
                    response = self.session.get(url, headers=headers, timeout=30, verify=True)
                else:
                    response = self.session.get(url, timeout=30, verify=True)
                
                if response.status_code == 200:
                    # Reset delay on success
                    self.current_delay = self.initial_delay
                    return response.text
                elif response.status_code == 429:  # Rate limited
                    self.handle_rate_limit(url)
                elif response.status_code == 403:  # Forbidden
                    self.log(f"Access forbidden (403) for {url}", "WARNING")
                    time.sleep(self.current_delay)
                else:
                    self.log(f"Status {response.status_code} for {url}", "WARNING")
                    time.sleep(self.current_delay)
            except requests.exceptions.Timeout:
                self.log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
                time.sleep(self.current_delay)
            except requests.exceptions.ConnectionError:
                self.log(f"Connection error on attempt {attempt+1} for {url}", "WARNING")
                time.sleep(self.current_delay)
            except Exception as e:
                self.log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
                time.sleep(self.current_delay)
        
        self.log(f"Failed to fetch {url} after 5 attempts", "ERROR")
        return None
    
    def fetch_json(self, url: str) -> Optional[dict]:
        """Fetch JSON data from API with rate limit handling"""
        for attempt in range(3):
            try:
                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{self.curr_url}/",
                }
                response = self.session.get(url, headers=headers, timeout=30, verify=True)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    self.handle_rate_limit(url)
                else:
                    self.log(f"API fetch failed: {response.status_code} for {url}", "WARNING")
                    time.sleep(self.current_delay)
            except Exception as e:
                self.log(f"Error fetching JSON from {url}: {e}", "ERROR")
                time.sleep(self.current_delay)
        
        return None
    
    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Load and parse XML from URL"""
        data = self.http_get(url, is_json=False)
        if not data:
            return None
        
        try:
            # Clean XML if needed
            if "<?xml" not in data[:100]:
                data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parsing failed for {url}: {e}", "ERROR")
            # Try to extract URLs with regex as fallback
            try:
                root = ET.Element("urlset")
                urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
                for url_text in urls:
                    url_elem = ET.SubElement(root, "url")
                    loc_elem = ET.SubElement(url_elem, "loc")
                    loc_elem.text = url_text
                return root
            except Exception as e2:
                self.log(f"Regex extraction also failed: {e2}", "ERROR")
                return None
    
    def get_sitemap_urls_from_robots(self) -> List[str]:
        """
        Fetch robots.txt and extract sitemap URLs that start with 'sitemap-products'
        With fallback to known sitemap patterns
        """
        robots_url = f"{self.curr_url}/robots.txt"
        self.log(f"Fetching robots.txt from {robots_url}")
        
        content = self.http_get(robots_url)
        
        # If robots.txt fetch fails, try fallback method
        if not content:
            self.log("Failed to fetch robots.txt, trying fallback methods...", "WARNING")
            return self.get_sitemap_urls_fallback()
        
        sitemap_urls = []
        for line in content.split('\n'):
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                # Filter only product sitemaps
                if 'product' in sitemap_url.lower():
                    sitemap_urls.append(sitemap_url)
                    self.log(f"Found product sitemap: {sitemap_url}", "DEBUG")
        
        # If no product sitemaps found in robots.txt, try fallback
        if not sitemap_urls:
            self.log("No product sitemaps found in robots.txt, trying fallback...", "WARNING")
            return self.get_sitemap_urls_fallback()
        
        self.log(f"Found {len(sitemap_urls)} product sitemaps from robots.txt")
        return sitemap_urls
    
    def get_sitemap_urls_fallback(self) -> List[str]:
        """
        Fallback method to construct sitemap URLs based on common patterns
        """
        self.log("Attempting to construct sitemap URLs using fallback patterns...")
        
        # Common patterns for LuxeDecor sitemaps
        sitemap_urls = []
        
        # Try to fetch sitemap index first
        sitemap_index_url = f"{self.curr_url}/sitemap.xml"
        self.log(f"Trying sitemap index: {sitemap_index_url}")
        
        content = self.http_get(sitemap_index_url)
        if content:
            try:
                root = ET.fromstring(content)
                # Extract all sitemap URLs from index
                ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for sitemap in root.findall(".//ns:loc", ns):
                    url = sitemap.text
                    if url and 'product' in url.lower():
                        sitemap_urls.append(url)
                        self.log(f"Found from index: {url}")
            except:
                pass
        
        # If no sitemaps found in index, try common product sitemap patterns
        if not sitemap_urls:
            # Common patterns for LuxeDecor
            patterns = [
                f"{self.curr_url}/sitemap-products-1.xml",
                f"{self.curr_url}/sitemap-products-1.xml.gz",
                f"{self.curr_url}/sitemap_products_1.xml",
                f"{self.curr_url}/product-sitemap.xml",
            ]
            
            # Test each pattern until we find one that works
            for pattern in patterns:
                self.log(f"Testing pattern: {pattern}")
                # Try HEAD request first to check existence
                try:
                    response = self.session.head(pattern, timeout=10)
                    if response.status_code == 200:
                        sitemap_urls.append(pattern)
                        self.log(f"Found working pattern: {pattern}")
                        break
                except:
                    pass
        
        # If still no sitemaps, try to guess the range (often up to 20-30)
        if not sitemap_urls:
            self.log("No sitemaps found with patterns, trying numeric range...")
            for i in range(1, 31):  # Try first 30 sitemaps
                pattern = f"{self.curr_url}/sitemap-products-{i}.xml.gz"
                try:
                    response = self.session.head(pattern, timeout=5)
                    if response.status_code == 200:
                        sitemap_urls.append(pattern)
                        self.log(f"Found: {pattern}")
                except:
                    pass
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
        
        self.log(f"Fallback found {len(sitemap_urls)} sitemaps")
        return sitemap_urls
    
    def convert_gz_to_xml_url(self, gz_url: str) -> str:
        """Convert .gz sitemap URL to .xml URL"""
        if gz_url.endswith('.gz'):
            return gz_url.replace('.gz', '.xml')
        return gz_url
    
    def extract_product_urls_from_sitemap(self, sitemap_url: str) -> List[str]:
        """
        Extract product URLs from a sitemap
        Converts .gz to .xml and loads the XML
        """
        # Convert .gz to .xml if needed
        xml_url = self.convert_gz_to_xml_url(sitemap_url)
        self.log(f"Loading sitemap from: {xml_url}", "DEBUG")
        
        xml = self.load_xml(xml_url)
        if not xml:
            return []
        
        # Define namespace
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        
        # Extract URLs
        urls = []
        
        # Try different XML structures
        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            if "ns:" in path:
                elements = xml.findall(path, ns)
            else:
                elements = xml.findall(path)
            
            if elements:
                for elem in elements:
                    if elem.text and '/product/' in elem.text:
                        urls.append(elem.text.strip())
                if urls:
                    break
        
        self.log(f"Found {len(urls)} product URLs in {sitemap_url}")
        return urls
    
    def extract_product_identifier(self, product_url: str) -> Optional[str]:
        """
        Extract product identifier from URL
        Example: https://www.luxedecor.com/product/acme-furniture-bertie-end-table-casual-side-acf82842?phash=eff584
        Returns: acf82842 (the identifier at the end before ?phash)
        """
        # Parse URL
        parsed = urlparse(product_url)
        path = parsed.path
        
        # Extract the last part of the path
        last_segment = path.split('/')[-1]
        
        # Look for pattern: something-{identifier} at the end
        # The identifier is typically after the last hyphen
        match = re.search(r'-([a-z0-9]+)$', last_segment, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Alternative: check if the whole last segment is the identifier
        if re.match(r'^[a-z0-9]+$', last_segment, re.IGNORECASE):
            return last_segment.upper()
        
        self.log(f"Could not extract identifier from URL: {product_url}", "WARNING")
        return None
    
    def get_group_attr_details(self, additional_data, identifier, value_fetcher):
        finish_value = None
        for spec in additional_data["specifications"]:
            if spec["name"] == identifier:
                (value,) = spec["values"]   # raises error if not exactly 1
                finish_value = value[value_fetcher]
                break
        return finish_value        
    
    def extract_product_data(self, api_data: dict, product_url: str) -> List[Dict]:
        """
        Extract product data from API response
        """
        try:
            if not api_data or not isinstance(api_data, dict):
                self.log(f"Invalid API data for {product_url}", "ERROR")
                return []
            
            additional_data = self.fetch_product_additional_data(product_url)
            
            product_id = api_data.get('itemProperties', {}).get('itemId', '')
            name = api_data.get('itemProperties', {}).get('description', '')
            sku = api_data.get('itemProperties', {}).get('sku', '')
            brand = api_data.get('vendor', {}).get('name', '')
            price = api_data.get('pricingProperties', {}).get('retailPrice', '')
            main_image = ""
            category = api_data.get('mainCategory', {}).get('name', '') + " / " + api_data.get('subCategory', {}).get('name', {})
            category_url = api_data.get('mainCategory', {}).get('link', '') + " / " + api_data.get('subCategory', {}).get('link', {})
            description = additional_data.get('featureDescription', '') if additional_data else ''
            quantity = api_data.get('stockProperties', {}).get('stockQty', '')
            status = ""
            dimension_str = additional_data.get('dimension', '') if additional_data else ''
            group_attr_1 = self.get_group_attr_details(additional_data, 'finish', 'name') if additional_data else ''
            group_attr_2 = ""
            
            # Create product record
            product_info = {
                'product_url': product_url,
                'product_id': product_id,
                'name': name,
                'brand': brand,
                'sku': sku,
                'price': price,
                'main_image': main_image,
                'category': category,
                'category_url': category_url,
                'description': description,
                'dimensions': dimension_str,
                'quantity': quantity,
                'status': status,
                'group_attr_1': group_attr_1,
                'group_attr_2': group_attr_2
            }
            
            return [product_info]
            
        except Exception as e:
            self.log(f"Error extracting product data for {product_url}: {e}", "ERROR")
            return []
    
    def fetch_product_additional_data(self, product_url: str) -> Optional[dict]:
        """
        Fetch additional data for a product
        """
        identifier = self.extract_product_identifier(product_url)
        if not identifier:
            self.stats['errors'] += 1
            self.log(f"No identifier found for {product_url}", "ERROR")
            return None
        
        # Construct API URL
        api_url = f"{self.api_base_url}/{identifier.upper()}/overview-data"
        self.log(f"API URL: {api_url}", "DEBUG")
        
        # Fetch API data
        api_data = self.fetch_json(api_url)
        if not api_data:
            self.stats['errors'] += 1
            self.log(f"No API data for {identifier}", "ERROR")
            return None
        return api_data
    
    def process_product(self, product_url: str, seen: Set[str], writer) -> None:
        """
        Process a single product URL
        """
        if product_url in seen:
            return
        seen.add(product_url)
        
        self.log(f"Processing: {product_url}", "DEBUG")
        
        # Extract product identifier
        identifier = self.extract_product_identifier(product_url)
        if not identifier:
            self.stats['errors'] += 1
            self.log(f"No identifier found for {product_url}", "ERROR")
            return
        
        # Construct API URL
        api_url = f"{self.api_base_url}/{identifier.upper()}"
        self.log(f"API URL: {api_url}", "DEBUG")
        
        # Fetch API data
        api_data = self.fetch_json(api_url)
        if not api_data:
            self.stats['errors'] += 1
            self.log(f"No API data for {identifier}", "ERROR")
            return
        
        # Extract product data
        products = self.extract_product_data(api_data, product_url)
        
        for product in products:
            if not product.get('product_id'):
                continue
            
            try:
                # Prepare CSV row
                row = [
                    product['product_url'],
                    product['product_id'],
                    product['category'],
                    product['category_url'],
                    product['brand'],
                    product['name'],
                    product['sku'],
                    '',  # MPN (not available)
                    '',  # GTIN (not available)
                    product['price'],
                    self.normalize_image_url(product['main_image']),
                    product['quantity'],
                    product['group_attr_1'],
                    product['group_attr_2'],
                    product['status'],
                    product['description'],
                    product['dimensions'],
                    self.scraped_date
                ]
                
                with self.csv_lock:
                    writer.writerow(row)
                
                self.stats['products_fetched'] += 1
                self.log(f"Fetched: {product['name'][:50]}...", "INFO")
                
            except Exception as e:
                self.log(f"Error creating row for {product_url}: {e}", "ERROR")
                self.stats['errors'] += 1
        
        # Respect request delay
        time.sleep(self.request_delay)
        self.stats['urls_processed'] += 1
    
    def normalize_image_url(self, url: str) -> str:
        """Normalize image URL"""
        if not url:
            return ""
        
        if url.startswith("//"):
            return "https:" + url
        elif url.startswith("/"):
            return f"{self.curr_url}{url}"
        elif not url.startswith("http"):
            return f"https://{url}"
        
        return url
    
    def run(self):
        """Main execution method"""
        try:
            # Step 1: Get sitemap URLs from robots.txt (with fallback)
            sitemap_urls = self.get_sitemap_urls_from_robots()
            if not sitemap_urls:
                self.log("No product sitemaps found even with fallback methods", "ERROR")
                self.log("Please check manually if the site structure has changed", "ERROR")
                return
            
            # Apply offset and limit
            if self.sitemap_offset >= len(sitemap_urls):
                self.log(f"Offset {self.sitemap_offset} exceeds total sitemaps ({len(sitemap_urls)})", "ERROR")
                return
            
            end_index = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(sitemap_urls)
            sitemaps_to_process = sitemap_urls[self.sitemap_offset:end_index]
            
            self.log(f"Total sitemaps: {len(sitemap_urls)}")
            self.log(f"Processing {len(sitemaps_to_process)} sitemaps")
            for idx, sitemap in enumerate(sitemaps_to_process, 1):
                self.log(f"  {idx}. {sitemap}")
            
            # Step 2: Open CSV for writing
            with open(self.output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    "Product URL",
                    "Product ID",
                    "Category",
                    "Category URL",
                    "Brand",
                    "Product Name",
                    "SKU",
                    "MPN",
                    "GTIN",
                    "Price",
                    "Main Image",
                    "Quantity",
                    "group_attr_1",
                    "group_attr_2",
                    "Status",
                    "Description",
                    "Dimensions",
                    "Date Scraped"
                ])
                
                seen = set()
                
                # Step 3: Process each sitemap
                for sitemap_url in sitemaps_to_process:
                    self.stats['sitemaps_processed'] += 1
                    self.log(f"Processing sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
                    
                    # Extract URLs from sitemap (automatically converts .gz to .xml)
                    urls = self.extract_product_urls_from_sitemap(sitemap_url)
                    
                    # Apply URL limit
                    if self.max_urls_per_sitemap > 0:
                        original_count = len(urls)
                        urls = urls[:self.max_urls_per_sitemap]
                        self.log(f"Limited to {len(urls)}/{original_count} URLs")
                    else:
                        self.log(f"Found {len(urls)} product URLs")
                    
                    if not urls:
                        self.log(f"No product URLs found in {sitemap_url}", "WARNING")
                        continue
                    
                    # Process URLs in parallel
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(self.process_product, url, seen, writer)
                            for url in urls
                        ]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                self.log(f"Error in thread: {e}", "ERROR")
                                self.stats['errors'] += 1
                    
                    # Clean up memory
                    gc.collect()
            
            # Print statistics
            self.print_statistics()
            
        except KeyboardInterrupt:
            self.log("Scraping interrupted by user", "WARNING")
            self.print_statistics()
        except Exception as e:
            self.log(f"Fatal error: {e}", "ERROR")
            self.print_statistics()
    
    def print_statistics(self):
        """Print scraping statistics"""
        self.log("=" * 60)
        self.log("SCRAPING STATISTICS")
        self.log("=" * 60)
        self.log(f"Sitemaps processed: {self.stats['sitemaps_processed']}")
        self.log(f"URLs processed: {self.stats['urls_processed']}")
        self.log(f"Products fetched: {self.stats['products_fetched']}")
        self.log(f"Errors: {self.stats['errors']}")
        self.log(f"Rate limits encountered: {self.stats['rate_limits']}")
        if self.stats['urls_processed'] > 0:
            success_rate = (self.stats['products_fetched'] / self.stats['urls_processed']) * 100
            self.log(f"Success rate: {success_rate:.1f}%")
        self.log("=" * 60)
        self.log(f"Output saved to: {self.output_csv}")
        self.log("=" * 60)


def main():
    """Entry point"""
    # Validate environment variables
    if not os.getenv("CURR_URL"):
        print("Error: CURR_URL environment variable is required", file=sys.stderr)
        sys.exit(1)
    
    scraper = LuxeDecorScraper()
    scraper.run()


if __name__ == "__main__":
    main()