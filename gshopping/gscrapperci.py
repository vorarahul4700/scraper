#!/usr/bin/env python3
"""
Google Shopping Scraper for GitHub Actions
Fixed for Chrome 120 compatibility
"""

import json
import random
import time
import os
import csv
import traceback
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import requests
from bs4 import BeautifulSoup

# Configuration
OUTPUT_DIR = "scraping_results"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_driver():
    """Setup Chrome driver for GitHub Actions"""
    try:
        chrome_options = Options()
        
        # Headless mode for CI
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--log-level=3")
        
        # Add user agent
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        
        # Disable automation detection
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        logger.info("Setting up Chrome driver...")
        
        # Use webdriver-manager for automatic ChromeDriver management
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Execute CDP commands to prevent detection
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": random.choice(USER_AGENTS)
        })
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("Chrome driver setup successful")
        return driver
        
    except Exception as e:
        logger.error(f"Failed to setup Chrome driver: {str(e)}")
        raise

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        logger.warning(f"No data to save to {filename}")
        return False
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    try:
        # Get all unique keys from all dictionaries
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        if not all_keys:
            return False
        
        headers = list(all_keys)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for row in data:
                # Ensure all keys are present
                row_data = row.copy()
                for header in headers:
                    if header not in row_data:
                        row_data[header] = ''
                writer.writerow(row_data)
        
        logger.info(f"Saved {len(data)} rows to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving {filename}: {str(e)}")
        return False

def load_product_urls():
    """Load product URLs from file"""
    try:
        with open('product_urls.json', 'r') as f:
            data = json.load(f)
            logger.info(f"Loaded {len(data)} products from product_urls.json")
            return data
    except FileNotFoundError:
        logger.warning("product_urls.json not found, using sample data")
        return [
            {
                "product_id": 1,
                "url": "https://www.google.com/search?q=test+product&tbm=shop",
                "keyword": "test product",
                "test": True
            }
        ]
    except Exception as e:
        logger.error(f"Error loading product URLs: {str(e)}")
        return []

def scrape_product_page(driver, url, product_id, keyword):
    """Scrape a single product page"""
    result = {
        'product_id': product_id,
        'keyword': keyword,
        'url': url,
        'scraped_at': datetime.now().isoformat(),
        'status': 'started'
    }
    
    try:
        logger.info(f"Navigating to: {url}")
        driver.get(url)
        
        # Wait for page to load
        time.sleep(random.uniform(3, 5))
        
        # Check if page loaded successfully
        if "did not match any documents" in driver.page_source:
            result['status'] = 'no_results'
            result['error'] = 'No search results found'
            return result
        
        if "captcha" in driver.page_source.lower() or "recaptcha" in driver.page_source.lower():
            result['status'] = 'captcha_detected'
            result['error'] = 'CAPTCHA detected'
            return result
        
        # Get page information
        result['page_title'] = driver.title
        result['page_url'] = driver.current_url
        
        # Look for shopping results
        try:
            # Try multiple selectors for shopping results
            selectors = [
                "div.sh-dgr__content",
                "div[data-initq]",
                "div[jscontroller]",
                "div.sh-dlr__list-result"
            ]
            
            shopping_elements = []
            for selector in selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    shopping_elements = elements
                    result['selector_used'] = selector
                    break
            
            if shopping_elements:
                result['shopping_elements_count'] = len(shopping_elements)
                result['status'] = 'shopping_results_found'
                
                # Extract some sample data from first few elements
                sample_data = []
                for i, element in enumerate(shopping_elements[:3]):
                    try:
                        text = element.text[:200] if element.text else ""
                        sample_data.append(f"Element {i+1}: {text}")
                    except:
                        pass
                
                if sample_data:
                    result['sample_data'] = " | ".join(sample_data)
                    
            else:
                result['status'] = 'no_shopping_elements'
                result['error'] = 'Could not find shopping results container'
                
        except Exception as e:
            result['status'] = 'element_search_error'
            result['error'] = str(e)[:100]
            
        # Try to find product names
        try:
            product_names = driver.find_elements(By.CSS_SELECTOR, "h3, div[aria-label], a[href*='/shopping/']")
            if product_names:
                result['product_names_count'] = len(product_names)
                
                # Get first product name if available
                for name_element in product_names[:5]:
                    try:
                        name = name_element.text.strip()
                        if name and len(name) > 10:
                            result['first_product_name'] = name[:100]
                            break
                    except:
                        continue
                        
        except Exception as e:
            pass  # This is optional, so don't fail if it errors
        
        result['status'] = 'completed'
        
    except TimeoutException:
        result['status'] = 'timeout'
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['status'] = 'webdriver_error'
        result['error'] = str(e)[:100]
    except Exception as e:
        result['status'] = 'unexpected_error'
        result['error'] = str(e)[:100]
        logger.error(f"Error scraping product {product_id}: {traceback.format_exc()}")
    
    return result

def main():
    """Main scraping function"""
    logger.info("=" * 60)
    logger.info("Starting Google Shopping Scraper")
    logger.info("=" * 60)
    
    # Load products to scrape
    products = load_product_urls()
    if not products:
        logger.error("No products to scrape. Exiting.")
        return
    
    logger.info(f"Found {len(products)} products to scrape")
    
    # Initialize results storage
    all_results = {
        'products': [],
        'metadata': {
            'start_time': datetime.now().isoformat(),
            'total_products': len(products),
            'environment': 'github-actions'
        }
    }
    
    driver = None
    try:
        # Setup Chrome driver
        driver = setup_driver()
        
        # Scrape each product
        for i, product in enumerate(products, 1):
            product_id = product.get('product_id', i)
            url = product.get('url', '')
            keyword = product.get('keyword', f'Product {product_id}')
            
            logger.info(f"[{i}/{len(products)}] Scraping: {keyword}")
            
            # Scrape the product
            result = scrape_product_page(driver, url, product_id, keyword)
            all_results['products'].append(result)
            
            # Log status
            status = result.get('status', 'unknown')
            if 'error' in result:
                logger.warning(f"  Status: {status} - {result.get('error')}")
            else:
                logger.info(f"  Status: {status}")
            
            # Add delay between requests (except for last one)
            if i < len(products):
                delay = random.uniform(5, 10)
                logger.info(f"  Waiting {delay:.1f} seconds...")
                time.sleep(delay)
        
        # Update metadata
        all_results['metadata']['end_time'] = datetime.now().isoformat()
        all_results['metadata']['duration_seconds'] = (
            datetime.fromisoformat(all_results['metadata']['end_time']) - 
            datetime.fromisoformat(all_results['metadata']['start_time'])
        ).total_seconds()
        
        # Calculate success rate
        successful = len([p for p in all_results['products'] 
                         if p.get('status') in ['completed', 'shopping_results_found']])
        all_results['metadata']['successful_scrapes'] = successful
        all_results['metadata']['success_rate'] = f"{(successful/len(products))*100:.1f}%" if products else "0%"
        
    except Exception as e:
        logger.error(f"Fatal error in main loop: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Save partial results
        if all_results['products']:
            save_to_csv(all_results['products'], 'partial_results.csv')
            
    finally:
        # Cleanup
        if driver:
            try:
                driver.quit()
                logger.info("Chrome driver closed")
            except:
                pass
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Save results
    if all_results['products']:
        # Save CSV
        save_to_csv(all_results['products'], 'all_products.csv')
        
        # Save individual product files
        for product in all_results['products']:
            product_id = product.get('product_id')
            if product_id:
                save_to_csv([product], f'product_{product_id}.csv')
    
    # Save JSON summary
    try:
        summary_file = os.path.join(OUTPUT_DIR, 'summary.json')
        with open(summary_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        logger.info(f"Summary saved to {summary_file}")
    except Exception as e:
        logger.error(f"Error saving JSON summary: {str(e)}")
    
    # Print final statistics
    logger.info("=" * 60)
    logger.info("SCRAPING COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total products: {len(products)}")
    logger.info(f"Successful: {all_results['metadata'].get('successful_scrapes', 0)}")
    logger.info(f"Success rate: {all_results['metadata'].get('success_rate', '0%')}")
    logger.info(f"Duration: {all_results['metadata'].get('duration_seconds', 0):.1f} seconds")
    logger.info(f"Results saved to: {OUTPUT_DIR}/")
    
    # Create a simple success flag file
    success_file = os.path.join(OUTPUT_DIR, 'SUCCESS')
    with open(success_file, 'w') as f:
        f.write(f"Scraping completed at {datetime.now().isoformat()}\n")
        f.write(f"Products: {len(products)}\n")
        f.write(f"Successful: {all_results['metadata'].get('successful_scrapes', 0)}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        exit(1)