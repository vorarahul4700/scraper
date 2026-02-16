#!/usr/bin/env python3
import csv
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests


CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "13"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "60"))

URL_LIST_FILE = "cymax_chunk_urls.csv"


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{ts}] [{level}] {msg}\n")
    sys.stderr.flush()


def sanitize_url_text(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    m = re.search(r"https?://[^\s\"'<>]+", clean)
    return m.group(0).strip() if m else ""


def extract_xml_payload(raw: str) -> str:
    text = (raw or "").strip()
    for root_tag in ("sitemapindex", "urlset"):
        s = text.find(f"<{root_tag}")
        e_tag = f"</{root_tag}>"
        e = text.rfind(e_tag)
        if s != -1 and e != -1 and e > s:
            return text[s:e + len(e_tag)]
    return text


class FlareSolverrSession:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/",
        }

    def flaresolverr_request(self, url: str, max_retries: int = 3) -> Optional[Tuple[str, int]]:
        for attempt in range(max_retries):
            try:
                payload = {
                    "cmd": "request.get",
                    "url": url,
                    "maxTimeout": 60000,
                    "session": None,
                    "headers": self.headers,
                }
                response = self.session.post(
                    FLARESOLVERR_URL,
                    json=payload,
                    timeout=FLARESOLVERR_TIMEOUT,
                )

                if response.status_code == 200:
                    result = response.json()
                    if result.get("status") == "ok":
                        solution = result.get("solution", {})
                        content = solution.get("response", "")
                        for cookie in solution.get("cookies", []):
                            self.session.cookies.set(
                                cookie.get("name"),
                                cookie.get("value"),
                                domain=cookie.get("domain"),
                            )
                        return content, 200

                log(f"FlareSolverr attempt {attempt + 1} failed for {url}: {response.status_code}", "DEBUG")
            except requests.exceptions.Timeout:
                log(f"FlareSolverr timeout on attempt {attempt + 1} for {url}", "WARNING")
            except requests.exceptions.ConnectionError:
                log(f"FlareSolverr connection error on attempt {attempt + 1} for {url}", "WARNING")
            except Exception as e:
                log(f"FlareSolverr error on attempt {attempt + 1} for {url}: {e}", "WARNING")

            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 1))
        return None, 0

    def fetch(self, url: str) -> Optional[Tuple[str, int]]:
        return self.flaresolverr_request(url)


flaresolverr_session = FlareSolverrSession()


def check_robots_txt():
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    content, status = flaresolverr_session.fetch(robots_url)
    if content and status == 200:
        sitemap_url = None
        for line in content.split("\n"):
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                parts = line.split(":", 1)
                if len(parts) > 1:
                    candidate = sanitize_url_text(parts[1].strip())
                    if candidate.startswith("http"):
                        sitemap_url = candidate
                        log(f"Found sitemap in robots.txt: {sitemap_url}")
                        break
        return sitemap_url
    return None


def fetch_xml(url: str) -> Optional[str]:
    content, status = flaresolverr_session.fetch(url)
    if content and status == 200:
        return content
    return None


def extract_locs(root: ET.Element) -> List[str]:
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = root.findall(".//ns:loc", ns) or root.findall(".//loc")
    return [loc.text.strip() for loc in locs if loc.text and loc.text.strip()]


def collect_urls_from_sitemap(sm_url: str, depth: int = 0, max_depth: int = 6) -> List[str]:
    if depth > max_depth:
        return []
    xml = fetch_xml(sm_url)
    if not xml:
        return []
    try:
        root = ET.fromstring(extract_xml_payload(xml))
    except ET.ParseError:
        return []

    locs = extract_locs(root)
    if not locs:
        return []

    tag = root.tag.lower()
    nested = [u for u in locs if u.lower().endswith(".xml") or u.lower().endswith(".xml.gz")]
    if "sitemapindex" in tag or (nested and len(nested) == len(locs)):
        out: List[str] = []
        for n in nested:
            out.extend(collect_urls_from_sitemap(n, depth + 1, max_depth))
        return out

    return [u for u in locs if ".htm" in u and not any(x in u for x in ["--C", "--PC", "sitemap", "robots"])]


def main() -> None:
    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be > 0")

    log(
        f"Planner config => max_sitemaps={MAX_SITEMAPS}, chunk_size={CHUNK_SIZE}, "
        f"max_workers={MAX_WORKERS}, max_urls_per_sitemap={MAX_URLS_PER_SITEMAP}"
    )
    log(f"FlareSolverr endpoint: {FLARESOLVERR_URL}")

    robots_sitemap = check_robots_txt()
    sitemap_index = robots_sitemap if robots_sitemap else SITEMAP_INDEX
    log(f"Loading sitemap index: {sitemap_index}")

    index_xml = fetch_xml(sitemap_index)
    if not index_xml:
        raise RuntimeError(f"Failed to fetch sitemap index: {sitemap_index}")
    try:
        root = ET.fromstring(extract_xml_payload(index_xml))
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse sitemap index XML: {e}") from e

    sitemap_locs = extract_locs(root)
    if not sitemap_locs:
        raise RuntimeError("No sitemaps found in index")

    if MAX_SITEMAPS > 0:
        sitemap_locs = sitemap_locs[:MAX_SITEMAPS]
    log(f"Top-level sitemaps selected: {len(sitemap_locs)}")

    all_urls: List[str] = []

    def process_sitemap(sm_url: str) -> List[str]:
        urls = collect_urls_from_sitemap(sm_url, 0, 6)
        if MAX_URLS_PER_SITEMAP > 0 and len(urls) > MAX_URLS_PER_SITEMAP:
            urls = urls[:MAX_URLS_PER_SITEMAP]
        log(f"Sitemap done: {sm_url} -> {len(urls)} urls")
        return urls

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, len(sitemap_locs)))) as ex:
        futures = [ex.submit(process_sitemap, sm) for sm in sitemap_locs]
        completed = 0
        for fut in as_completed(futures):
            all_urls.extend(fut.result())
            completed += 1
            log(f"Sitemap progress: {completed}/{len(futures)} processed")

    unique_urls = list(dict.fromkeys(all_urls))
    total = len(unique_urls)
    log(f"Total unique product urls: {total}")
    if total == 0:
        raise RuntimeError("No product urls discovered")

    with open(URL_LIST_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url"])
        for u in unique_urls:
            writer.writerow([u])

    chunks = []
    for i, offset in enumerate(range(0, total, CHUNK_SIZE)):
        limit = min(CHUNK_SIZE, total - offset)
        chunks.append(
            {
                "chunk_id": i,
                "offset": offset,
                "limit": limit,
                "url_file": URL_LIST_FILE,
                "base_url": CURR_URL,
                "total_urls": total,
            }
        )
        log(f"Chunk planned: id={i}, offset={offset}, limit={limit}", "DEBUG")

    log(f"Generated chunks: {len(chunks)}")

    matrix_json = json.dumps(chunks)
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"matrix={matrix_json}\n")
            f.write(f"total_urls={total}\n")
            f.write(f"chunk_count={len(chunks)}\n")
    else:
        print(matrix_json)


if __name__ == "__main__":
    main()
