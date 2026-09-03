import requests
import xml.etree.ElementTree as ET
import gzip
import re
import logging
from typing import List
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class SitemapProcessor:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }

    def get_sitemap_from_robots(self, site_url: str) -> str:
        site_url = site_url.rstrip('/')
        robots_url = urljoin(site_url + '/', 'robots.txt')
        logger.info(f"Checking robots.txt at: {robots_url}")
        try:
            r = requests.get(robots_url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                for line in r.text.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                        return sitemap_url
        except Exception as e:
            logger.warning(f"Error checking robots.txt: {e}")

        return f"{site_url}/sitemap.xml"

    def extract_all_sitemaps(self, sitemap_url: str) -> List[str]:
        sitemaps = []
        try:
            r = requests.get(sitemap_url, headers=self.headers, timeout=30)
            if r.status_code != 200:
                logger.warning(f"Failed to fetch sitemap {sitemap_url}: status {r.status_code}")
                return [sitemap_url]
            
            content = r.content
            if sitemap_url.endswith('.gz'):
                try:
                    content = gzip.decompress(content)
                except Exception:
                    pass

            text = content.decode('utf-8', errors='ignore')
            text = re.sub(r'<script[^>]*/>', '', text)
            text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
            if "<?xml" not in text[:100]:
                text = '<?xml version="1.0" encoding="UTF-8"?>\n' + text

            root = ET.fromstring(text)
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            # Check if index sitemap
            elements = root.findall('.//ns:sitemap/ns:loc', ns) or root.findall('.//sitemap/loc') or root.findall('.//loc')
            for el in elements:
                if el.text:
                    url = el.text.strip()
                    if url.endswith('.xml') or url.endswith('.xml.gz') or 'sitemap' in url.lower():
                        sitemaps.append(url)

            if not sitemaps:
                sitemaps = [sitemap_url]
        except Exception as e:
            logger.warning(f"Error extracting sitemaps from {sitemap_url}: {e}")
            sitemaps = [sitemap_url]

        return sitemaps
