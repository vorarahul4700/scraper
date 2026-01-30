#!/usr/bin/env python3
"""
BBB SKU Extractor from OVS Variants
Alternative approach to handle BBB API timeouts
"""

import pandas as pd
import requests
import json
import logging
import sys
import os
import argparse
from datetime import datetime
import time
import re
import csv
import socket
import urllib3
from typing import List, Dict, Any, Optional

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= LOGGER =================

def setup_logging(chunk_id: int):
    """Setup logging configuration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"bbb_extractor_chunk_{chunk_id}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr)
        ]
    )
    return logging.getLogger(__name__)

# ================= API CALLER =================

class BBBApiClient:
    """Client for BBB API with robust error handling"""
    
    def __init__(self):
        self.session = None
        self.base_url = "https://api.bedbathandbeyond.com"
        
    def __enter__(self):
        self.session = requests.Session()
        # Configure session with very conservative settings
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
        })
        
        # Configure adapter with conservative settings
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5,
            pool_maxsize=5,
            max_retries=1,  # We'll handle retries manually
            pool_block=False
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
    
    def fetch_variant_data(self, variant_id: str, timeout: int = 30) -> Optional[Dict]:
        """
        Fetch variant data with multiple fallback strategies
        """
        # Try multiple approaches
        approaches = [
            self._try_direct_api_call,
            self._try_with_proxy_headers,
            self._try_alternative_endpoint,
        ]
        
        for approach in approaches:
            result = approach(variant_id, timeout)
            if result is not None:
                return result
            time.sleep(1)  # Small delay between approaches
        
        return None
    
    def _try_direct_api_call(self, variant_id: str, timeout: int) -> Optional[Dict]:
        """Direct API call with multiple URL patterns"""
        url_patterns = [
            f"{self.base_url}/options/{variant_id}",
            f"{self.base_url}/v1/options/{variant_id}",
            f"{self.base_url}/api/options/{variant_id}",
            f"{self.base_url}/product-api/options/{variant_id}",
        ]
        
        for url in url_patterns:
            try:
                logger.debug(f"Trying URL: {url}")
                response = self.session.get(
                    url,
                    timeout=timeout,
                    verify=False,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data and isinstance(data, dict):
                            logger.info(f"Successfully fetched variant {variant_id}")
                            return data
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON from {url}")
                        continue
                elif response.status_code == 404:
                    logger.warning(f"Variant {variant_id} not found (404)")
                    return None
                elif response.status_code == 429:
                    logger.warning(f"Rate limited for variant {variant_id}")
                    time.sleep(5)
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except (requests.exceptions.Timeout, socket.timeout):
                logger.warning(f"Timeout for {url}")
                continue
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error for {url}")
                continue
            except Exception as e:
                logger.warning(f"Error for {url}: {type(e).__name__}")
                continue
        
        return None
    
    def _try_with_proxy_headers(self, variant_id: str, timeout: int) -> Optional[Dict]:
        """Try with different headers that might bypass restrictions"""
        url = f"{self.base_url}/options/{variant_id}"
        
        # Try different user agents
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        ]
        
        for user_agent in user_agents:
            try:
                headers = {
                    "User-Agent": user_agent,
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.bedbathandbeyond.com/",
                    "Origin": "https://www.bedbathandbeyond.com",
                }
                
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    verify=False
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        logger.info(f"Success with alternative headers for {variant_id}")
                        return data
                        
            except Exception as e:
                logger.debug(f"Header approach failed: {type(e).__name__}")
                continue
        
        return None
    
    def _try_alternative_endpoint(self, variant_id: str, timeout: int) -> Optional[Dict]:
        """Try completely different endpoints"""
        # Sometimes the API might be accessible through different domains
        alternative_domains = [
            "https://api.bedbathandbeyond.com",
            # "https://api.overstock.com",  # Since Overstock owns BBB now
            # "https://api.bbb.com",
        ]
        
        for domain in alternative_domains:
            url = f"{domain}/options/{variant_id}"
            try:
                response = self.session.get(
                    url,
                    timeout=timeout,
                    verify=False
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        logger.info(f"Success with alternative domain for {variant_id}")
                        return data
                        
            except Exception as e:
                logger.debug(f"Alternative domain failed: {type(e).__name__}")
                continue
        
        return None

# ================= DATA PROCESSING =================

def extract_data_from_response(data: Dict) -> Dict[str, Any]:
    """Extract relevant data from API response"""
    if not data or not isinstance(data, dict):
        return {}
    
    try:
        result = {
            'BBB_SKU': data.get('modelNumber'),
            'BBB_ModelNumber': data.get('modelNumber'),
            'BBB_OptionId': data.get('optionId'),
            'BBB_Description': data.get('description'),
            'BBB_Dimensions': None,
            'BBB_Attributes': None,
            'BBB_Attributes_Count': 0,
            'BBB_AttributeIcons_Count': 0,
        }
        
        # Extract dimensions
        dims = data.get('assembledDimensions', {})
        if dims:
            length = dims.get('length')
            width = dims.get('width')
            height = dims.get('height')
            
            if length and width:
                dim_parts = [f"{length}", f"{width}"]
                if height:
                    dim_parts.append(f"{height}")
                result['BBB_Dimensions'] = "x".join(dim_parts)
        
        # Extract attributes
        attributes = data.get('attributes', [])
        if attributes:
            attr_list = []
            for attr in attributes:
                name = attr.get('name', '')
                value = attr.get('value', '')
                if name and value:
                    attr_list.append(f"{name}: {value}")
            
            if attr_list:
                result['BBB_Attributes'] = " | ".join(attr_list)
            result['BBB_Attributes_Count'] = len(attributes)
        
        # Extract attribute icons
        icons = data.get('attributeIcons', [])
        result['BBB_AttributeIcons_Count'] = len(icons)
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        return {}

def process_variant_single_thread(variant_id: str, client: BBBApiClient, stats: dict) -> Dict[str, Any]:
    """Process a single variant ID (single-threaded to avoid overwhelming API)"""
    try:
        # Clean and validate variant ID
        variant_id = str(variant_id).strip()
        variant_id = re.sub(r'\.0$', '', variant_id)
        
        if not re.match(r'^\d+$', variant_id):
            logger.warning(f"Invalid variant ID: {variant_id}")
            stats['invalid'] += 1
            return None
        
        logger.info(f"Processing variant: {variant_id}")
        
        # Fetch data with retries
        data = None
        for attempt in range(3):  # 3 attempts with increasing delays
            try:
                data = client.fetch_variant_data(variant_id, timeout=20)
                if data:
                    break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {variant_id}: {e}")
            
            # Exponential backoff
            delay = (attempt + 1) * 5  # 5, 10, 15 seconds
            logger.info(f"Waiting {delay} seconds before retry...")
            time.sleep(delay)
        
        if not data:
            logger.warning(f"Failed to fetch data for variant {variant_id}")
            stats['errors'] += 1
            
            # Return error record
            return {
                'Ref Varient ID': variant_id,
                'BBB_SKU': '',
                'BBB_ModelNumber': '',
                'BBB_OptionId': '',
                'BBB_Description': '',
                'BBB_Dimensions': '',
                'BBB_Attributes': '',
                'BBB_Attributes_Count': '',
                'BBB_AttributeIcons_Count': '',
                'BBB_Error': 'API timeout or unavailable',
                'BBB_Status': 'Failed'
            }
        
        # Extract data
        extracted = extract_data_from_response(data)
        
        result = {
            'Ref Varient ID': variant_id,
            'BBB_SKU': extracted.get('BBB_SKU', ''),
            'BBB_ModelNumber': extracted.get('BBB_ModelNumber', ''),
            'BBB_OptionId': extracted.get('BBB_OptionId', ''),
            'BBB_Description': extracted.get('BBB_Description', ''),
            'BBB_Dimensions': extracted.get('BBB_Dimensions', ''),
            'BBB_Attributes': extracted.get('BBB_Attributes', ''),
            'BBB_Attributes_Count': extracted.get('BBB_Attributes_Count', 0),
            'BBB_AttributeIcons_Count': extracted.get('BBB_AttributeIcons_Count', 0),
            'BBB_Error': '',
            'BBB_Status': 'Success'
        }
        
        stats['processed'] += 1
        logger.info(f"Successfully processed {variant_id}: {extracted.get('BBB_SKU', 'N/A')}")
        
        return result
        
    except Exception as e:
        logger.error(f"Unexpected error processing {variant_id}: {e}")
        stats['errors'] += 1
        
        return {
            'Ref Varient ID': variant_id if 'variant_id' in locals() else '',
            'BBB_SKU': '',
            'BBB_ModelNumber': '',
            'BBB_OptionId': '',
            'BBB_Description': '',
            'BBB_Dimensions': '',
            'BBB_Attributes': '',
            'BBB_Attributes_Count': '',
            'BBB_AttributeIcons_Count': '',
            'BBB_Error': str(e),
            'BBB_Status': 'Error'
        }

# ================= MAIN FUNCTION =================

def main():
    parser = argparse.ArgumentParser(description='Extract BBB SKUs from OVS variants')
    parser.add_argument('--chunk-id', type=int, required=True)
    parser.add_argument('--total-chunks', type=int, required=True)
    parser.add_argument('--input-file', type=str, required=True)
    parser.add_argument('--api-url', type=str, default='https://api.bedbathandbeyond.com/options')
    parser.add_argument('--output-dir', type=str, default='output')
    parser.add_argument('--timeout', type=int, default=30)
    
    args = parser.parse_args()
    
    # Setup logging
    global logger
    logger = setup_logging(args.chunk_id)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("BBB SKU Extractor - Single-threaded Edition")
    logger.info(f"Chunk ID: {args.chunk_id}/{args.total_chunks}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"API URL: {args.api_url}")
    logger.info(f"Timeout: {args.timeout}s")
    logger.info("Using single-threaded approach to avoid rate limiting")
    logger.info("=" * 60)
    
    # Read input file
    try:
        df = pd.read_csv(args.input_file, dtype=str)
        logger.info(f"Loaded {len(df)} rows from {args.input_file}")
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        sys.exit(1)
    
    # Find variant ID column
    variant_col = None
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['variant', 'varient', 'option', 'id']):
            variant_col = col
            break
    
    if not variant_col:
        logger.error("Could not find variant ID column")
        logger.info(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Clean variant IDs
    df['Ref Varient ID'] = df[variant_col].astype(str).str.strip()
    df['Ref Varient ID'] = df['Ref Varient ID'].str.replace(r'\.0$', '', regex=True)
    
    # Filter valid numeric IDs
    valid_mask = df['Ref Varient ID'].str.match(r'^\d+$')
    df_valid = df[valid_mask].copy()
    
    invalid_count = len(df) - len(df_valid)
    if invalid_count > 0:
        logger.warning(f"Removed {invalid_count} invalid variant IDs")
    
    logger.info(f"Valid rows: {len(df_valid)}")
    
    if len(df_valid) == 0:
        logger.warning("No valid variant IDs to process")
        # Create empty output
        output_file = os.path.join(args.output_dir, f"bbb_chunk_{args.chunk_id}.csv")
        pd.DataFrame().to_csv(output_file, index=False)
        logger.info(f"Created empty output: {output_file}")
        sys.exit(0)
    
    # Split into chunks
    if args.total_chunks > 1:
        chunk_size = len(df_valid) // args.total_chunks
        if chunk_size == 0:
            chunk_size = 1
        
        start_idx = (args.chunk_id - 1) * chunk_size
        end_idx = start_idx + chunk_size if args.chunk_id < args.total_chunks else len(df_valid)
        
        start_idx = max(0, min(start_idx, len(df_valid)))
        end_idx = max(0, min(end_idx, len(df_valid)))
        
        chunk_df = df_valid.iloc[start_idx:end_idx].copy()
        logger.info(f"Processing chunk {args.chunk_id}: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
    else:
        chunk_df = df_valid.copy()
    
    # Get variant IDs
    variant_ids = chunk_df['Ref Varient ID'].unique().tolist()
    logger.info(f"Processing {len(variant_ids)} unique variant IDs")
    
    # Initialize stats
    stats = {
        'processed': 0,
        'errors': 0,
        'invalid': 0,
        'total': len(variant_ids)
    }
    
    # Process variants sequentially
    results = []
    with BBBApiClient() as client:
        for i, variant_id in enumerate(variant_ids, 1):
            logger.info(f"Processing {i}/{len(variant_ids)}: {variant_id}")
            
            result = process_variant_single_thread(variant_id, client, stats)
            if result:
                results.append(result)
            
            # Add delay between requests to avoid rate limiting
            if i < len(variant_ids):
                delay = 2  # 2 seconds between requests
                logger.info(f"Waiting {delay} seconds before next request...")
                time.sleep(delay)
    
    # Create results DataFrame
    if results:
        results_df = pd.DataFrame(results)
        
        # Merge with original data
        if len(chunk_df.columns) > 1:
            # Keep all original columns
            results_df = chunk_df.merge(
                results_df,
                on='Ref Varient ID',
                how='left',
                suffixes=('', '_bbb')
            )
    else:
        # Create empty results
        results_df = chunk_df.copy()
        bbb_columns = [
            'BBB_SKU', 'BBB_ModelNumber', 'BBB_OptionId', 'BBB_Description',
            'BBB_Dimensions', 'BBB_Attributes', 'BBB_Attributes_Count',
            'BBB_AttributeIcons_Count', 'BBB_Error', 'BBB_Status'
        ]
        for col in bbb_columns:
            results_df[col] = ''
    
    # Save output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(args.output_dir, f"bbb_output_chunk_{args.chunk_id}_{timestamp}.csv")
    
    # Ensure consistent column order
    all_columns = list(results_df.columns)
    bbb_cols = [col for col in all_columns if col.startswith('BBB_')]
    other_cols = [col for col in all_columns if not col.startswith('BBB_')]
    
    final_cols = other_cols + sorted(bbb_cols)
    results_df = results_df[final_cols]
    
    results_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # Print summary
    logger.info("=" * 60)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total variants: {stats['total']}")
    logger.info(f"Successfully processed: {stats['processed']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"Invalid IDs: {stats['invalid']}")
    
    if stats['total'] > 0:
        success_rate = (stats['processed'] / stats['total']) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    logger.info(f"Output file: {output_file}")
    logger.info(f"Output size: {len(results_df)} rows, {len(results_df.columns)} columns")
    
    # Save summary
    summary = {
        'chunk_id': args.chunk_id,
        'total_chunks': args.total_chunks,
        'input_file': args.input_file,
        'output_file': output_file,
        'stats': stats,
        'success_rate': f"{success_rate:.1f}%" if stats['total'] > 0 else "0%",
        'timestamp': datetime.now().isoformat()
    }
    
    summary_file = os.path.join(args.output_dir, f"summary_{args.chunk_id}.json")
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Summary saved: {summary_file}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()