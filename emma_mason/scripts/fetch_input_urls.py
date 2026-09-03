#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import csv
import json
import tempfile
import ftplib
import re
from pathlib import Path
from typing import List
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

# Set up logger
logger = logging.getLogger("fetch_input_urls")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def save_urls_to_csv(urls: List[str], output_file: str) -> int:
    """Writes a list of URLs to the standard remaining_merged.csv structure."""
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    
    clean_urls = []
    seen = set()
    for u in urls:
        u_str = str(u).strip()
        if u_str and u_str.startswith("http") and u_str not in seen:
            seen.add(u_str)
            clean_urls.append(u_str)

    header = ["url", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for u in clean_urls:
            writer.writerow([u, "", "", "", "", "", ""])
            
    logger.info(f"Successfully saved {len(clean_urls)} unique URLs to {output_file}")
    return len(clean_urls)

def fetch_from_ftp(host: str, port: int, user: str, pass_: str, remote_dir: str, target_filename: str = "") -> List[str]:
    logger.info(f"Connecting to FTP {host}:{port} as user '{user}'...")
    ftp = ftplib.FTP()
    ftp.connect(host, port, timeout=30)
    ftp.login(user, pass_)
    
    # Support relative sub-paths in target_filename (e.g., "input_urls/emma-mason.csv")
    if target_filename:
        target_filename = target_filename.strip()
        if "/" in target_filename or "\\" in target_filename:
            parts = target_filename.replace("\\", "/").lstrip("/").split("/")
            sub_dir = "/".join(parts[:-1])
            target_filename = parts[-1]
            if remote_dir:
                remote_dir = remote_dir.rstrip("/") + "/" + sub_dir
            else:
                remote_dir = sub_dir

    if remote_dir:
        try:
            ftp.cwd(remote_dir)
            logger.info(f"Changed directory to remote path: {remote_dir}")
        except ftplib.error_perm as e:
            logger.warning(f"Cannot change to FTP directory '{remote_dir}': {e}")
            try:
                root_items = ftp.nlst()
            except Exception:
                root_items = []
            raise FileNotFoundError(
                f"FTP directory '{remote_dir}' does not exist or is not accessible on "
                f"{host}:{port} (user='{user}'). "
                f"Available items in FTP root: {root_items[:30]}. "
                f"Please correct the --ftp-path / FTP_PATH value."
            ) from e
        
    items = ftp.nlst()
    logger.info(f"Found {len(items)} items in remote FTP directory.")
    
    target_file = ""
    if target_filename and target_filename in items:
        target_file = target_filename
    elif target_filename:
        for it in items:
            if it.lower() == target_filename.lower():
                target_file = it
                break
        if not target_file:
            raise FileNotFoundError(f"Specified target file '{target_filename}' not found in FTP folder '{remote_dir}'. Available: {items[:20]}...")
    else:
        csv_files = [it for it in items if it.lower().endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No .csv files found in FTP directory '{remote_dir}'. Available items: {items[:20]}...")
        target_file = csv_files[0]
        logger.info(f"No filename specified. Auto-selected CSV file from FTP: {target_file}")
        
    logger.info(f"Downloading '{target_file}' from FTP...")
    urls = []
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(tmp_fd, "wb") as tmp:
            ftp.retrbinary(f"RETR {target_file}", tmp.write)
        ftp.quit()
        logger.info("FTP download complete. Parsing CSV file...")
        
        with open(tmp_name, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f)
            header = None
            url_idx = -1
            priority_cols = ["item_url", "ref product url", "product url", "product_url", "url", "link", "product_link", "target_url", "item url"]
            
            for row in reader:
                if not row:
                    continue
                if header is None:
                    header = [c.strip().lower() for c in row]
                    for pcol in priority_cols:
                        for idx, col in enumerate(header):
                            if col == pcol:
                                url_idx = idx
                                break
                        if url_idx != -1:
                            break
                            
                    if url_idx == -1:
                        for idx, col in enumerate(header):
                            if "url" in col or "link" in col:
                                url_idx = idx
                                break
                                
                    if url_idx != -1:
                        logger.info(f"Selected column index {url_idx} ('{header[url_idx]}') for product URLs")
                    else:
                        if row[0].strip().startswith("http"):
                            urls.append(row[0].strip())
                            url_idx = 0
                    continue
                
                if url_idx != -1 and len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u.startswith("http"):
                        urls.append(u)
                else:
                    for cell in row:
                        c = cell.strip()
                        if c.startswith("http"):
                            urls.append(c)
                            break
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
            
    logger.info(f"Extracted {len(urls)} URLs from FTP file '{target_file}'")
    return urls

def fetch_from_sitemap(sitemap_url: str) -> List[str]:
    import requests
    logger.info(f"Fetching sitemap from: {sitemap_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    def load_sitemap_xml(url: str):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                logger.warning(f"Sitemap HTTP status {r.status_code} for {url}")
                return None
            text = r.text
            text = re.sub(r'<script[^>]*/>', '', text)
            text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
            if "<?xml" not in text[:100]:
                text = '<?xml version="1.0" encoding="UTF-8"?>\n' + text
            return ET.fromstring(text)
        except Exception as e:
            logger.warning(f"Error fetching/parsing sitemap {url}: {e}")
            return None

    root = load_sitemap_xml(sitemap_url)
    if root is None:
        return []

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    
    child_sitemaps = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = root.findall(path, ns) if "ns:" in path else root.findall(path)
        if elements:
            found = [e.text.strip() for e in elements if e.text and (e.text.strip().endswith(".xml") or "sitemap" in e.text.strip().lower())]
            if found:
                child_sitemaps.extend(found)
                break

    product_urls = []
    if child_sitemaps:
        logger.info(f"Found {len(child_sitemaps)} child sitemaps in sitemap index")
        for sm_url in child_sitemaps:
            sub_root = load_sitemap_xml(sm_url)
            if sub_root is None:
                continue
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = sub_root.findall(path, ns) if "ns:" in path else sub_root.findall(path)
                if elements:
                    urls = [e.text.strip() for e in elements if e.text and e.text.strip().startswith("http")]
                    product_urls.extend(urls)
                    break
    else:
        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = root.findall(path, ns) if "ns:" in path else root.findall(path)
            if elements:
                urls = [e.text.strip() for e in elements if e.text and e.text.strip().startswith("http")]
                product_urls.extend(urls)
                break

    logger.info(f"Extracted total {len(product_urls)} raw URLs from sitemap")
    return product_urls

def fetch_from_direct(urls_str: str, urls_file: str = "") -> List[str]:
    urls = []
    if urls_file and os.path.exists(urls_file):
        logger.info(f"Reading direct URLs from file: {urls_file}")
        with open(urls_file, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u.startswith("http"):
                    urls.append(u)
    elif urls_str:
        logger.info("Parsing direct URLs from input text string...")
        for raw in urls_str.replace("\n", ",").split(","):
            u = raw.strip()
            if u.startswith("http"):
                urls.append(u)
    return urls

def main():
    parser = argparse.ArgumentParser(description="Fetch product URLs for Emma Mason from FTP, Sitemap, or Direct inputs")
    parser.add_argument("--source-type", default="ftp", type=str.lower,
                        choices=["ftp", "sitemap", "direct", "urls", "direct urls"],
                        help="Input source type for URL collection (ftp, sitemap, direct)")
    
    # FTP args
    parser.add_argument("--ftp-host", default="ftp-sandbox.1sb.pp.ua", help="FTP Hostname")
    parser.add_argument("--ftp-port", type=int, default=21, help="FTP Port")
    parser.add_argument("--ftp-user", default="onestop_ftp-sandbox", help="FTP Username")
    parser.add_argument("--ftp-pass", default="OneStop123", help="FTP Password")
    parser.add_argument("--ftp-path", default="", help="FTP directory path")
    parser.add_argument("--ftp-filename", default="", help="Specific CSV filename on FTP server (e.g. input_urls/emma-mason.csv)")
    
    # Sitemap args
    parser.add_argument("--sitemap-url", default="https://emmamason.com/sitemap.xml", help="Sitemap XML URL")
    
    # Direct args
    parser.add_argument("--urls", default="", help="Comma or newline separated URLs")
    parser.add_argument("--urls-file", default="", help="Path to file containing URLs")
    
    # Output file
    parser.add_argument("--output-file", default="remaining_input/remaining_merged.csv", help="Target output CSV file path")
    
    args = parser.parse_args()
    
    source = args.source_type.lower()
    logger.info(f"=== Starting URL collection mode: {source.upper()} ===")
    
    urls = []
    if source == "ftp":
        urls = fetch_from_ftp(
            host=args.ftp_host,
            port=args.ftp_port,
            user=args.ftp_user,
            pass_=args.ftp_pass,
            remote_dir=args.ftp_path,
            target_filename=args.ftp_filename
        )
    elif source == "sitemap":
        urls = fetch_from_sitemap(sitemap_url=args.sitemap_url)
    elif source in ["direct", "urls", "direct urls"]:
        urls = fetch_from_direct(urls_str=args.urls, urls_file=args.urls_file)
    else:
        raise ValueError(f"Unsupported source_type: {args.source_type}")
        
    if not urls:
        logger.error(f"No URLs collected using source_type '{source}'!")
        sys.exit(1)
        
    count = save_urls_to_csv(urls, args.output_file)
    logger.info(f"Finished processing. Total {count} URLs written to {args.output_file}")

if __name__ == "__main__":
    main()
