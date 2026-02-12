#!/usr/bin/env python3
"""
generate_chunks.py – Fetches sitemap index, counts product URLs per sitemap,
and generates a GitHub Actions matrix where each job processes one chunk
of at most URLS_PER_JOB URLs.
"""

import os
import sys
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree as ET

# ---------- ENV ----------
CURR_URL = os.environ.get("CURR_URL", "").rstrip("/")
if not CURR_URL:
    print("ERROR: CURR_URL environment variable is required", file=sys.stderr)
    sys.exit(1)

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
MAX_SITEMAPS = int(os.environ.get("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.environ.get("MAX_URLS_PER_SITEMAP", "0"))
URLS_PER_JOB = int(os.environ.get("URLS_PER_JOB", "500"))
SITEMAP_OFFSET = int(os.environ.get("SITEMAP_OFFSET", "0"))
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SitemapParser/1.0)"}

# ---------- FETCH with fallback to FlareSolverr ----------
def fetch_xml(url):
    """Try normal GET first, fallback to FlareSolverr if needed."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.text
        elif r.status_code in (403, 503) and FLARESOLVERR_URL:
            # Fallback to FlareSolverr
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": 30000,
                "headers": HEADERS
            }
            fs = requests.post(FLARESOLVERR_URL, json=payload, timeout=60)
            if fs.status_code == 200:
                return fs.json().get("solution", {}).get("response")
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
    return None

# ---------- 1. Get sitemap index ----------
print(f"Fetching sitemap index: {SITEMAP_INDEX}")
index_xml = fetch_xml(SITEMAP_INDEX)
if not index_xml:
    print("Failed to fetch sitemap index", file=sys.stderr)
    sys.exit(1)

try:
    root = ET.fromstring(index_xml)
except ET.ParseError as e:
    print(f"Failed to parse sitemap index XML: {e}", file=sys.stderr)
    sys.exit(1)

ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemap_locs = []
for loc in root.findall(".//ns:loc", ns) or root.findall(".//loc"):
    if loc.text:
        sitemap_locs.append(loc.text.strip())

if not sitemap_locs:
    print("No sitemaps found in index", file=sys.stderr)
    sys.exit(1)

# Apply sitemap offset & limit
if SITEMAP_OFFSET >= len(sitemap_locs):
    print(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemap_locs)})", file=sys.stderr)
    sys.exit(0)

end = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemap_locs)
sitemap_locs = sitemap_locs[SITEMAP_OFFSET:end]

print(f"Total sitemaps to analyze: {len(sitemap_locs)}")

# ---------- 2. For each sitemap, count product URLs ----------
sitemap_stats = []

def process_sitemap(sm_url):
    xml = fetch_xml(sm_url)
    if not xml:
        return {"url": sm_url, "total_urls": 0}
    try:
        root_sm = ET.fromstring(xml)
    except ET.ParseError:
        return {"url": sm_url, "total_urls": 0}
    urls = []
    for loc in root_sm.findall(".//ns:loc", ns) or root_sm.findall(".//loc"):
        if loc.text and ".html" in loc.text and not any(
            ext in loc.text for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]
        ):
            urls.append(loc.text.strip())
    total = len(urls)
    if MAX_URLS_PER_SITEMAP > 0 and total > MAX_URLS_PER_SITEMAP:
        total = MAX_URLS_PER_SITEMAP
    return {"url": sm_url, "total_urls": total}

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_sitemap, url) for url in sitemap_locs]
    for future in as_completed(futures):
        sitemap_stats.append(future.result())
        time.sleep(0.2)  # be polite

# ---------- 3. Generate chunks (one matrix entry per chunk) ----------
chunks = []
chunk_id = 0
for sm in sitemap_stats:
    total = sm["total_urls"]
    if total == 0:
        continue
    num_chunks = (total + URLS_PER_JOB - 1) // URLS_PER_JOB
    for i in range(num_chunks):
        offset = i * URLS_PER_JOB
        limit = min(URLS_PER_JOB, total - offset)
        chunks.append(
            {
                "sitemap_url": sm["url"],
                "offset": offset,
                "limit": limit,
                "chunk_id": chunk_id,
                "base_url": CURR_URL,
            }
        )
        chunk_id += 1

print(f"Generated {len(chunks)} chunks")

# ---------- 4. Output matrix to GITHUB_OUTPUT ----------
matrix_json = json.dumps(chunks)
github_output = os.environ.get("GITHUB_OUTPUT")
if github_output:
    with open(github_output, "a") as f:
        f.write(f"matrix={matrix_json}\n")
else:
    # When running locally, just print
    print(f"matrix={matrix_json}")