#!/usr/bin/env python3
"""
Google Shopping Scraper - GitHub Actions Optimized Version
"""

import sys
import json
import random
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import os
import csv
import traceback
import logging

# Setup logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_driver_github():
    """Setup Chrome driver optimized for GitHub Actions"""
    try:
        # Clean up any existing Chrome processes
        os.system("pkill -f chrome 2>/dev/null || true")
        time.sleep(2)
        
        options = uc.ChromeOptions()
        
        # Headless mode for GitHub Actions
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--log-level=3")
        options.add_argument("--remote-debugging-port=9222")
        
        # User agents compatible with headless mode
        user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/120.0.0.0 Safari/537.36",
        ]
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        logger.info("Setting up Chrome driver for GitHub Actions...")
        driver = uc.Chrome(options=options)
        
        logger.info("Chrome driver setup successful")
        return driver
        
    except Exception as e:
        logger.error(f"Failed to setup Chrome driver: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def save_to_csv_fixed(data, filename):
    """Save data to CSV with proper header handling"""
    if not data:
        logger.warning(f"No data to save to {filename}")
        return
    
    # Create directory if it doesn't exist
    os.makedirs('scraping_results', exist_ok=True)
    filepath = os.path.join('scraping_results', filename)
    
    try:
        # Get all unique keys from all dictionaries
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        if not all_keys:
            logger.warning(f"No valid data for {filename}")
            return
        
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
        
        logger.info(f"✅ Saved {len(data)} rows to {filename}")
        
    except Exception as e:
        logger.error(f"❌ Error saving {filename}: {str(e)}")

def load_product_urls():
    """Load product URLs with fallback"""
    try:
        with open('product_urls.json', 'r') as f:
            data = json.load(f)
            logger.info(f"Loaded {len(data)} products from product_urls.json")
            return data
    except FileNotFoundError:
        logger.warning("product_urls.json not found, creating sample data")
        sample_data = [
            {
                "product_id": 1,
                "url": "https://www.google.com/search?q=office+chair&tbm=shop&gl=US&hl=en",
                "keyword": "office chair"
            },
            {
                "product_id": 2,
                "url": "https://www.google.com/search?q=wireless+headphones&tbm=shop&gl=US&hl=en",
                "keyword": "wireless headphones"
            }
        ]
        with open('product_urls.json', 'w') as f:
            json.dump(sample_data, f, indent=2)
        return sample_data
    except Exception as e:
        logger.error(f"Error loading product URLs: {str(e)}")
        return []

def simple_scrape_product(driver, url, product_id, keyword):
    """Simplified scraping function for GitHub Actions"""
    result = {
        'product_id': product_id,
        'keyword': keyword,
        'url': url,
        'timestamp': datetime.now().isoformat(),
        'status': 'started'
    }
    
    try:
        logger.info(f"Scraping product {product_id}: {keyword}")
        driver.get(url)
        time.sleep(random.uniform(3, 5))
        
        # Basic page info
        result['page_title'] = driver.title
        result['current_url'] = driver.current_url
        
        # Try to find shopping elements
        try:
            # Look for common shopping selectors
            selectors = [
                "div.sh-dgr__content",
                "div[data-initq]",
                "div.sh-dlr__list-result",
                "div.MtXiu"
            ]
            
            for selector in selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    result['elements_found'] = len(elements)
                    result['selector_used'] = selector
                    
                    # Get sample text from first element
                    if elements[0].text:
                        result['sample_text'] = elements[0].text[:200]
                    
                    result['status'] = 'elements_found'
                    break
            
            if 'elements_found' not in result:
                result['status'] = 'no_elements_found'
                
        except Exception as e:
            result['status'] = 'element_error'
            result['error'] = str(e)[:100]
            
    except Exception as e:
        result['status'] = 'failed'
        result['error'] = str(e)[:100]
    
    return result

def main_github():
    """Main function optimized for GitHub Actions"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Google Shopping Scraper (GitHub Actions)")
    logger.info("=" * 60)
    
    # Load products
    products = load_product_urls()
    if not products:
        logger.error("No products to scrape. Exiting.")
        return
    
    logger.info(f"📋 Found {len(products)} products to scrape")
    
    # Initialize results
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
        driver = setup_driver_github()
        
        for i, product in enumerate(products, 1):
            product_id = product.get('product_id', i)
            url = product.get('url', '')
            keyword = product.get('keyword', f'Product {product_id}')
            
            logger.info(f"\n[{i}/{len(products)}] Processing: {keyword}")
            
            # Scrape the product
            result = simple_scrape_product(driver, url, product_id, keyword)
            all_results['products'].append(result)
            
            # Log result
            logger.info(f"  Status: {result.get('status')}")
            if 'error' in result:
                logger.warning(f"  Error: {result.get('error')}")
            
            # Add delay between requests
            if i < len(products):
                delay = random.uniform(5, 8)
                logger.info(f"  ⏳ Waiting {delay:.1f} seconds...")
                time.sleep(delay)
        
        # Update metadata
        all_results['metadata']['end_time'] = datetime.now().isoformat()
        all_results['metadata']['successful'] = len([
            p for p in all_results['products'] 
            if p.get('status') in ['elements_found', 'completed']
        ])
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("✅ Chrome driver closed")
            except:
                pass
    
    # Save results
    if all_results['products']:
        save_to_csv_fixed(all_results['products'], 'github_products.csv')
        
        # Also save summary
        try:
            summary_file = os.path.join('scraping_results', 'github_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(all_results, f, indent=2, default=str)
            logger.info(f"✅ Summary saved to {summary_file}")
        except Exception as e:
            logger.error(f"❌ Error saving summary: {str(e)}")
    
    # Final statistics
    logger.info("=" * 60)
    logger.info("📊 SCRAPING COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total products: {len(products)}")
    logger.info(f"Results saved to: scraping_results/")

if __name__ == "__main__":
    # Determine which mode to run
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        main_github()
    else:
        # Import and run your original main if exists
        try:
            from gscrapper import main as original_main
            original_main()
        except ImportError:
            print("Running in GitHub Actions mode...")
            main_github()