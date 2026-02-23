import argparse
import ftplib
import json
import os
import traceback
import uuid
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


def download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, remote_filename, local_filename):
    try:
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, int(os.getenv("FTP_PORT", 21)))
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)

        if ftp_path and ftp_path != "/":
            ftp.cwd(ftp_path)

        with open(local_filename, "wb") as f:
            ftp.retrbinary(f"RETR {remote_filename}", f.write)

        ftp.quit()
        print(f"✓ Downloaded {remote_filename} to {local_filename}")
        return local_filename
    except Exception as e:
        print(f"Error downloading from FTP: {str(e)}")
        return None


def normalize_url_path_slug(url):
    if not url:
        return ""
    try:
        path = urlparse(url).path.strip("/")
        if not path:
            return ""
        return path.split("/")[-1]
    except Exception:
        return ""


def get_text_safe(element):
    if not element:
        return ""
    return element.get_text(" ", strip=True)


def find_first_text(node, selectors):
    for selector in selectors:
        found = node.select_one(selector)
        text = get_text_safe(found)
        if text:
            return text
    return ""


def parse_flaresolver_urls(urls_str):
    return [u.strip() for u in (urls_str or "").split(",") if u.strip()]


def is_captcha_response(html, final_url=""):
    body = ((html or "") + " " + (final_url or "")).lower()
    markers = [
        "recaptcha",
        "google.com/sorry",
        "/sorry/index",
        "unusual traffic",
        "our systems have detected unusual traffic",
        "detected unusual traffic from your computer network",
    ]
    return any(m in body for m in markers)


def flaresolver_cmd(fs_url, payload, timeout=75):
    try:
        resp = requests.post(fs_url, json=payload, timeout=timeout)
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"
        data = resp.json()
        if data.get("status") != "ok":
            return None, data.get("message", "status_not_ok")
        return data, ""
    except Exception as e:
        return None, str(e)


def create_flaresolver_sessions(fs_urls):
    sessions = {}
    for fs_url in fs_urls:
        session_id = f"gshopping-{uuid.uuid4().hex[:12]}"
        payload = {"cmd": "sessions.create", "session": session_id}
        data, err = flaresolver_cmd(fs_url, payload, timeout=30)
        if data:
            sessions[fs_url] = session_id
            print(f"✓ Session created on {fs_url}")
        else:
            print(f"⚠ Session create failed on {fs_url}: {err}")
    return sessions


def destroy_flaresolver_sessions(sessions):
    for fs_url, session_id in sessions.items():
        payload = {"cmd": "sessions.destroy", "session": session_id}
        flaresolver_cmd(fs_url, payload, timeout=20)


def fetch_with_flaresolver(fs_url, target_url, session_id=""):
    payload = {
        "cmd": "request.get",
        "url": target_url,
        "maxTimeout": 45000,
    }
    if session_id:
        payload["session"] = session_id

    data, err = flaresolver_cmd(fs_url, payload, timeout=90)
    if not data:
        return None, err

    solution = data.get("solution") or {}
    html = solution.get("response", "")
    final_url = solution.get("url", target_url)
    http_status = solution.get("status", 0)

    if not html:
        return None, "empty_response"
    if http_status and int(http_status) >= 400:
        return None, f"target_http_{http_status}"

    return {
        "html": html,
        "final_url": final_url,
        "http_status": http_status,
    }, ""


def extract_product_from_html(html, row):
    keyword = str(row.get("keyword", "") or "")
    input_url = str(row.get("url", "") or "")

    result = {
        "product_id": row.get("product_id", ""),
        "web_id": row.get("web_id", ""),
        "name": row.get("name", ""),
        "mpn_sku": row.get("mpn_sku", ""),
        "gtin": row.get("gtin", ""),
        "brand": row.get("brand", ""),
        "category": row.get("category", ""),
        "keyword": keyword,
        "url": input_url,
        "osb_url": row.get("osb_url", ""),
        "last_response": "",
        "osb_url_match": "",
        "product_url": input_url,
        "seller": "",
        "product_name": "",
        "cid": "",
        "pid": "",
        "last_fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "osb_position": 0,
        "osb_id": "",
        "seller_count": 0,
        "status": "error",
        "competitors": [],
    }

    soup = BeautifulSoup(html, "lxml")
    mains = soup.select_one(".dURPMd")
    if not mains:
        result["status"] = "container_not_found"
        result["last_response"] = "Product container not found"
        return result

    products = mains.select(".MtXiu")
    if not products:
        result["status"] = "no_products"
        result["last_response"] = "No products found"
        return result

    chosen = None
    for product in products:
        product_name = find_first_text(product, ["div.gkQHve", "[class*='gkQHve']"])
        seller = find_first_text(product, ["span.WJMUdc", "[class*='WJMUdc']"])
        cid = product.get("id", "")

        if ("set" in keyword.lower() and "set" not in product_name.lower()) or (
            "set" not in keyword.lower() and "set" in product_name.lower()
        ):
            continue

        chosen = {
            "product_name": product_name,
            "seller": seller,
            "cid": cid,
        }
        break

    if not chosen:
        result["status"] = "no_match"
        result["last_response"] = "No matching product found"
        return result

    result["product_name"] = chosen["product_name"]
    result["seller"] = chosen["seller"]
    result["cid"] = chosen["cid"]

    offers_grid = soup.select_one("div[jsname='RSFNod'][data-attrid='organic_offers_grid']")
    if not offers_grid:
        result["status"] = "no_offers_found"
        result["last_response"] = "Offers grid not found"
        return result

    offer_elements = offers_grid.select(".R5K7Cb")
    competitors = []
    for offer in offer_elements:
        store_name = find_first_text(offer, ["div.hP4iBf.gUf0b.uWvFpd", "[class*='hP4iBf']"]) or "N/A"
        seller_product_name = find_first_text(offer, ["div.Rp8BL", "[class*='Rp8BL']"]) or "N/A"

        seller_link = offer.select_one("a.P9159d") or offer.select_one("a[href]")
        seller_url = (seller_link.get("href", "") if seller_link else "") or "N/A"

        seller_price = (
            find_first_text(offer, ["div.QcEgce span[aria-hidden='true']", "div.GBgquf span", "[class*='QcEgce'] span"])
            or "N/A"
        )

        competitors.append(
            {
                "product_id": result["product_id"],
                "seller": store_name,
                "seller_product_name": seller_product_name,
                "seller_url": seller_url,
                "seller_price": seller_price,
                "last_fetched_date": result["last_fetched_date"],
            }
        )

    if not competitors:
        result["status"] = "no_offers_found"
        result["last_response"] = "No seller offers found"
        return result

    search_seller = "1StopBedrooms"
    sellers = [c.get("seller", "") for c in competitors]
    osb_position = sellers.index(search_seller) + 1 if search_seller in sellers else 0
    osb_id = ""
    osb_url_match = ""

    if osb_position:
        for competitor in competitors:
            if competitor.get("seller") == search_seller:
                osb_id = normalize_url_path_slug(competitor.get("seller_url", ""))
                break

    input_osb_slug = normalize_url_path_slug(result.get("osb_url", ""))
    if input_osb_slug and osb_id and input_osb_slug == osb_id:
        osb_url_match = "true"

    result.update(
        {
            "osb_position": osb_position,
            "seller_count": len(sellers),
            "osb_id": osb_id,
            "osb_url_match": osb_url_match,
            "status": "completed",
            "last_response": f"Completed - OSB Position: {osb_position}, Total Sellers: {len(sellers)}",
            "competitors": competitors,
        }
    )
    return result


def scrape_product_with_host_pool(row, fs_urls, fs_sessions, max_retries=0):
    retries = max_retries if max_retries > 0 else len(fs_urls)
    retries = max(1, retries)

    last_result = None
    last_error = ""

    for attempt in range(retries):
        fs_url = fs_urls[attempt % len(fs_urls)]
        session_id = fs_sessions.get(fs_url, "")

        fetched, err = fetch_with_flaresolver(fs_url, str(row.get("url", "")), session_id=session_id)
        if err:
            last_error = f"{fs_url}: {err}"
            continue

        html = fetched.get("html", "")
        final_url = fetched.get("final_url", "")
        if is_captcha_response(html, final_url):
            last_error = f"{fs_url}: captcha_detected"
            continue

        parsed = extract_product_from_html(html, row)
        parsed["product_url"] = final_url or parsed.get("product_url", "")
        parsed["last_response"] = f"{parsed.get('last_response', '')} | host={fs_url} | attempt={attempt + 1}".strip()

        if parsed.get("status") == "completed":
            return parsed

        last_result = parsed

    if last_result:
        if last_error:
            last_result["last_response"] = f"{last_result.get('last_response', '')} | last_error={last_error}".strip()
        return last_result

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "product_id": row.get("product_id", ""),
        "web_id": row.get("web_id", ""),
        "name": row.get("name", ""),
        "mpn_sku": row.get("mpn_sku", ""),
        "gtin": row.get("gtin", ""),
        "brand": row.get("brand", ""),
        "category": row.get("category", ""),
        "keyword": row.get("keyword", ""),
        "url": row.get("url", ""),
        "osb_url": row.get("osb_url", ""),
        "last_response": f"All FlareSolverr retries failed ({last_error or 'unknown_error'})",
        "osb_url_match": "",
        "product_url": "",
        "seller": "",
        "product_name": "",
        "cid": "",
        "pid": "",
        "last_fetched_date": now,
        "osb_position": 0,
        "osb_id": "",
        "seller_count": 0,
        "status": "flaresolver_failed",
        "competitors": [],
    }


def split_df_chunk(df, chunk_id, total_chunks):
    if total_chunks <= 0:
        return df

    total_rows = len(df)
    rows_per_chunk = total_rows // total_chunks
    start_idx = (chunk_id - 1) * rows_per_chunk
    end_idx = chunk_id * rows_per_chunk if chunk_id < total_chunks else total_rows
    return df.iloc[start_idx:end_idx]


def process_chunk(df, chunk_id, fs_urls, max_retries):
    completed_products = []
    completed_sellers = []
    remaining_rows = []

    fs_sessions = create_flaresolver_sessions(fs_urls)

    try:
        for idx, row in df.iterrows():
            row_dict = {k: ("" if pd.isna(v) else v) for k, v in row.to_dict().items()}
            product_id = row_dict.get("product_id", "")
            print(f"Processing row {idx + 1}/{len(df)} product_id={product_id}")

            result = scrape_product_with_host_pool(row_dict, fs_urls, fs_sessions, max_retries=max_retries)

            if result.get("status") == "completed":
                completed_products.append(
                    {
                        "product_id": result.get("product_id", ""),
                        "web_id": result.get("web_id", ""),
                        "name": result.get("name", ""),
                        "mpn_sku": result.get("mpn_sku", ""),
                        "gtin": result.get("gtin", ""),
                        "brand": result.get("brand", ""),
                        "category": result.get("category", ""),
                        "keyword": result.get("keyword", ""),
                        "url": result.get("url", ""),
                        "osb_url": result.get("osb_url", ""),
                        "last_response": result.get("last_response", ""),
                        "osb_url_match": result.get("osb_url_match", ""),
                        "product_url": result.get("product_url", ""),
                        "seller": result.get("seller", ""),
                        "product_name": result.get("product_name", ""),
                        "cid": result.get("cid", ""),
                        "pid": result.get("pid", ""),
                        "last_fetched_date": result.get("last_fetched_date", ""),
                        "osb_position": result.get("osb_position", 0),
                        "osb_id": result.get("osb_id", ""),
                        "seller_count": result.get("seller_count", 0),
                        "status": result.get("status", ""),
                    }
                )
                completed_sellers.extend(result.get("competitors", []))
            else:
                remaining = dict(row_dict)
                remaining.update(
                    {
                        "status": result.get("status", "error"),
                        "last_response": result.get("last_response", ""),
                        "last_fetched_date": result.get("last_fetched_date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    }
                )
                remaining_rows.append(remaining)
    finally:
        destroy_flaresolver_sessions(fs_sessions)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    completed_products_path = os.path.join(output_dir, f"completed_products_chunk{chunk_id}_{timestamp}.csv")
    completed_sellers_path = os.path.join(output_dir, f"completed_sellers_chunk{chunk_id}_{timestamp}.csv")
    remaining_path = os.path.join(output_dir, f"remaining_chunk{chunk_id}_{timestamp}.csv")

    if completed_products:
        pd.DataFrame(completed_products).to_csv(completed_products_path, index=False)
        print(f"✓ Saved completed products: {os.path.basename(completed_products_path)}")

    if completed_sellers:
        pd.DataFrame(completed_sellers).to_csv(completed_sellers_path, index=False)
        print(f"✓ Saved completed sellers: {os.path.basename(completed_sellers_path)}")

    if remaining_rows:
        pd.DataFrame(remaining_rows).to_csv(remaining_path, index=False)
        print(f"✓ Saved remaining rows: {os.path.basename(remaining_path)}")

    print(f"Summary: completed={len(completed_products)} remaining={len(remaining_rows)}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Google Shopping Scraper via FlareSolverr host pool")
    parser.add_argument("--chunk-id", type=int, required=True, help="Chunk ID (1-based)")
    parser.add_argument("--total-chunks", type=int, required=True, help="Total number of chunks (0 = all)")
    parser.add_argument("--input-file", type=str, required=True, help="Input CSV filename on FTP")
    parser.add_argument("--flaresolver-urls", type=str, required=True, help="Comma-separated FlareSolverr /v1 URLs")
    parser.add_argument("--max-retries", type=int, default=0, help="Max retries per row (0 = number of hosts)")
    args = parser.parse_args()

    fs_urls = parse_flaresolver_urls(args.flaresolver_urls)
    if not fs_urls:
        print("Error: --flaresolver-urls is empty")
        raise SystemExit(1)

    ftp_host = os.getenv("FTP_HOST")
    ftp_user = os.getenv("FTP_USER")
    ftp_pass = os.getenv("FTP_PASS")
    ftp_path = os.getenv("FTP_PATH", "/scrap/")

    if not all([ftp_host, ftp_user, ftp_pass]):
        print("Error: FTP credentials not found in environment variables")
        raise SystemExit(1)

    input_csv = "input.csv"
    if not download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, args.input_file, input_csv):
        raise SystemExit(1)

    try:
        df = pd.read_csv(input_csv)
        if df.empty:
            print("Input CSV is empty")
            raise SystemExit(1)

        chunk_df = split_df_chunk(df, args.chunk_id, args.total_chunks)
        print(f"Chunk {args.chunk_id}/{args.total_chunks}: {len(chunk_df)} rows")

        success = process_chunk(chunk_df, args.chunk_id, fs_urls, max_retries=args.max_retries)
        if success:
            print("✓ Processing completed")
            raise SystemExit(0)
        raise SystemExit(1)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        raise SystemExit(1)
    finally:
        try:
            os.remove(input_csv)
        except Exception:
            pass


if __name__ == "__main__":
    main()
