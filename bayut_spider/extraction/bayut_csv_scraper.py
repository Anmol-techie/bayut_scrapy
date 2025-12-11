#!/usr/bin/env python3
"""
Bayut CSV-driven Scraper

Uses the same extraction and MongoDB logic as bayut_ldjson_to_mongo.py
but loops through sub-locations from bayut_sublocations_all_cities.csv

This script:
1. Reads CSV with sub-locations and their estimated listings count
2. Calculates max pages needed (listings / 24 properties per page)  
3. Uses identical extraction logic from bayut_ldjson_to_mongo.py
4. Loops through: CSV rows -> pages -> properties -> MongoDB

Usage:
    python bayut_csv_scraper.py --csv bayut_sublocations_all_cities.csv --cities "Dubai" --min-listings 1000
"""

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne, ASCENDING
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------- Configuration (Same as original) ------------------------------

UA_DEFAULT = (
    os.getenv("BAYUT_UA")
    or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": UA_DEFAULT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}

COOKIE_STR = os.getenv("COOKIE_STR", "").strip()
TIMEOUT = int(os.getenv("TIMEOUT_SEC", "25"))

DETAIL_ID_RE = re.compile(r"details-(\\d+)\\.html")

# ----------------------------- HTTP Session (Same as original) ------------------------------

def get_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# ----------------------------- Extraction (Exact same as original) ------------------------------

def load_json_lenient(txt: str) -> Any:
    """Parse JSON, tolerating trailing commas (best-effort)."""
    try:
        return json.loads(txt)
    except Exception:
        fixed = re.sub(r",(\\s*[}\\]])", r"\\1", txt)
        return json.loads(fixed)

def extract_single_ldjson(html: str) -> Any:
    """
    Extract the ItemList LD+JSON containing property listings.
    Bayut pages have multiple LD+JSON blocks - we need the one with @type containing ItemList
    that has itemListElement with mainEntity (property data).
    """
    soup = BeautifulSoup(html, "lxml")
    tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    
    if not tags:
        raise RuntimeError("No <script type='application/ld+json'> found.")
    
    # Try to find the ItemList with property data
    for tag in tags:
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        
        try:
            data = load_json_lenient(raw)
            
            # Check if this has ItemList type (can be string or in array)
            if isinstance(data, dict):
                dtype = data.get("@type")
                has_itemlist = False
                
                # Check if @type contains ItemList
                if dtype == "ItemList":
                    has_itemlist = True
                elif isinstance(dtype, list) and "ItemList" in dtype:
                    has_itemlist = True
                
                # If it has ItemList type and itemListElement with mainEntity
                if has_itemlist and "itemListElement" in data:
                    items = data.get("itemListElement", [])
                    if items and len(items) > 0:
                        # Check if first item has mainEntity (property data)
                        first_item = items[0] if isinstance(items, list) else None
                        if first_item and "mainEntity" in first_item:
                            return data
        except Exception:
            # Skip malformed JSON blocks
            continue
    
    # Fallback: if no ItemList with properties found, try to find any ItemList
    for tag in tags:
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        
        try:
            data = load_json_lenient(raw)
            if isinstance(data, dict):
                dtype = data.get("@type")
                if dtype == "ItemList" or (isinstance(dtype, list) and "ItemList" in dtype):
                    return data
        except Exception:
            continue
    
    # Last resort: return the first valid JSON found
    for tag in tags:
        raw = (tag.string or tag.text or "").strip()
        if raw:
            try:
                return load_json_lenient(raw)
            except Exception:
                continue
    
    raise RuntimeError("No valid LD+JSON found in any script tags.")

def property_id_from_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    m = DETAIL_ID_RE.search(u)
    if m:
        return m.group(1)
    return None

def doc_from_item(item: Dict[str, Any], page_num: int, fetched_at: str, location_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Convert one itemListElement entry into a Mongo-ready document.
    Enhanced with location info from CSV.
    """
    pos = item.get("position")
    main = item.get("mainEntity") if isinstance(item, dict) else None
    url = None
    price = None
    if isinstance(main, dict):
        url = main.get("url") or None
        # Extract price from offers
        offers = main.get("offers", [])
        if offers and isinstance(offers[0], dict):
            price_spec = offers[0].get("priceSpecification", {})
            price = price_spec.get("price")
    pid = property_id_from_url(url)

    # last-resort key if no property_id is available
    if not pid:
        base = (url or json.dumps(main, ensure_ascii=False))[:256]
        pid = "hash_" + hashlib.md5(base.encode("utf-8", errors="ignore")).hexdigest()

    return {
        "_upsert_key": pid,              # used only for upsert key in code below
        "property_id": pid,              # persisted
        "position": pos,
        "details_url": url,
        "page_number": page_num,
        "city": location_info["city"],
        "sublocation": location_info["sublocation"],
        "location_url": location_info["url"],
        "estimated_listings": location_info["listings"],
        "fetched_at": fetched_at,
        "raw_item": item,                # full original item (ItemPage + mainEntity)
        "price": price,                  # Extract price for tracking
    }

# ----------------------------- MongoDB (Same logic as original) ------------------------------

def ensure_indexes(coll):
    try:
        coll.create_index([("property_id", ASCENDING)], unique=True, background=True)
    except Exception as e:
        print(f"⚠️  Index creation warning: {e}")

def bulk_upsert_items(coll, docs: List[Dict[str, Any]]):
    if not docs:
        return
    
    ops = []
    for d in docs:
        # Create appearance record with location info
        appearance = {
            "page_number": d["page_number"],
            "position": d["position"],
            "city": d["city"],
            "sublocation": d["sublocation"],
            "price": d.get("price"),
            "scraped_at": d["fetched_at"]
        }
        
        # Use UpdateOne to track appearances (same as original)
        ops.append(
            UpdateOne(
                {"property_id": d["_upsert_key"]},
                {
                    "$set": {
                        "property_id": d["property_id"],
                        "details_url": d["details_url"],
                        "last_raw_item": d["raw_item"],
                        "last_seen": d["fetched_at"],
                        "last_page": d["page_number"],
                        "last_position": d["position"],
                        "last_city": d["city"],
                        "last_sublocation": d["sublocation"],
                        "current_price": d.get("price"),
                    },
                    "$addToSet": {
                        "cities_seen": d["city"],
                        "sublocations_seen": d["sublocation"]
                    },
                    "$push": {
                        "appearances": {
                            "$each": [appearance],
                            "$slice": -100  # Keep last 100 appearances
                        }
                    },
                    "$setOnInsert": {
                        "first_seen": d["fetched_at"],
                        "first_page": d["page_number"],
                        "first_city": d["city"],
                        "first_sublocation": d["sublocation"],
                        "detail_scraped": False,
                        "created_at": d["fetched_at"]
                    }
                },
                upsert=True
            )
        )
    
    res = coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
    return res

# ----------------------------- File I/O (Same as original) ------------------------------

def save_html(html_dir: Path, page_num: int, content: str):
    html_dir.mkdir(parents=True, exist_ok=True)
    fp = html_dir / f"page_{page_num}.html"
    fp.write_text(content, encoding="utf-8", errors="ignore")

def save_json(json_dir: Path, page_num: int, data: Any):
    json_dir.mkdir(parents=True, exist_ok=True)
    fp = json_dir / f"page_{page_num}.json"
    with fp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_sublocations_csv(csv_path: str) -> List[Dict[str, str]]:
    """Load sublocation data from CSV"""
    sublocations = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sublocations.append(row)
    return sublocations

# ----------------------------- Main Process (Enhanced with CSV loop) ------------------------------

def run(args):
    # Load sublocations from CSV
    print(f"📂 Loading sublocations from: {args.csv}")
    sublocations = load_sublocations_csv(args.csv)
    print(f"🌍 Found {len(sublocations)} sub-locations")
    
    # Filter by city if specified
    if args.cities:
        target_cities = [city.strip() for city in args.cities.split(',')]
        sublocations = [loc for loc in sublocations if loc["city"] in target_cities]
        print(f"🎯 Filtered to {len(sublocations)} sub-locations in: {', '.join(target_cities)}")
    
    # Filter by minimum listings if specified
    if args.min_listings:
        sublocations = [
            loc for loc in sublocations 
            if loc["listings"] and int(loc["listings"].replace(",", "")) >= args.min_listings
        ]
        print(f"📊 Filtered to {len(sublocations)} sub-locations with >= {args.min_listings} listings")
    
    if not sublocations:
        print("❌ No sub-locations to process!")
        return
    
    # MongoDB connection (same as original)
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[args.db]
    coll = db[args.collection]
    ensure_indexes(coll)
    print(f"🗄️  Mongo connected: {args.mongo_uri} → {args.db}.{args.collection}")
    
    # HTTP session (same as original)
    sess = get_session()
    headers = dict(HEADERS)
    if COOKIE_STR:
        headers["cookie"] = COOKIE_STR
    
    total_items = 0
    
    for loc_idx, location_info in enumerate(sublocations, 1):
        print(f"\\n[{loc_idx}/{len(sublocations)}] 🏢 {location_info['city']} > {location_info['sublocation']} ({location_info['listings']} listings)")
        
        # Calculate max pages based on listings (assuming ~24 properties per page)
        try:
            listings_count = int(location_info["listings"].replace(",", ""))
            max_pages = min(args.max_pages, (listings_count // 24) + 2)  # +2 for safety buffer
        except:
            max_pages = args.max_pages
        
        print(f"  📄 Will scrape up to {max_pages} pages")
        
        # Extract city and sublocation slugs from URL
        # URL format: https://www.bayut.com/for-sale/property/dubai/dubai-marina/
        url_parts = location_info["url"].rstrip("/").split("/")
        city_slug = url_parts[-2]  # 'dubai'
        sublocation_slug = url_parts[-1]  # 'dubai-marina'
        
        # Prepare output dirs for this location (same structure as original)
        location_html_dir = Path(args.out_dir) / "html" / city_slug / sublocation_slug
        location_json_dir = Path(args.out_dir) / "json" / city_slug / sublocation_slug
        location_html_dir.mkdir(parents=True, exist_ok=True)
        location_json_dir.mkdir(parents=True, exist_ok=True)
        
        location_items = 0
        consecutive_empty_pages = 0
        
        for page_num in range(1, max_pages + 1):
            if consecutive_empty_pages >= 5:
                print(f"    ⏹️  Stopping after {consecutive_empty_pages} consecutive empty pages")
                break
                
            # Generate URL for this page (same pattern as original)
            url = f"https://www.bayut.com/for-sale/property/{city_slug}/{sublocation_slug}/page-{page_num}/?sort=date_desc"
            print(f"\\n    📄 Page {page_num}: {url}")
            
            try:
                r = sess.get(url, headers=headers, timeout=TIMEOUT)
                
                # Log detailed response info (same as original)
                print(f"      📊 Response: HTTP {r.status_code}, Size: {len(r.text)} bytes")
                
                if r.status_code != 200:
                    consecutive_empty_pages += 1
                    print(f"      ❌ HTTP {r.status_code}")
                    if r.status_code == 404:
                        print(f"      ℹ️  Page not found - likely end of listings")
                    continue
                
                html = r.text
                
                # Check if we got a valid HTML response (same as original)
                if len(html) < 1000:
                    consecutive_empty_pages += 1
                    print(f"      ⚠️  Suspiciously small response: {len(html)} bytes")
                    continue
                
                # Check for blocking indicators (same as original)
                if "captcha" in html.lower() or "cloudflare" in html.lower():
                    consecutive_empty_pages += 1
                    print(f"      ⚠️  Possible CAPTCHA or Cloudflare challenge detected!")
                    continue
                
                # Save to location-specific directories (same as original)
                save_html(location_html_dir, page_num, html)
                
                ldjson = extract_single_ldjson(html)
                save_json(location_json_dir, page_num, ldjson)
                
                # Split into per-item docs (same as original logic)
                fetched_at = dt.datetime.utcnow().isoformat() + "Z"
                items = []
                if isinstance(ldjson, dict):
                    elts = ldjson.get("itemListElement") or []
                    for el in elts:
                        try:
                            doc = doc_from_item(el, page_num=page_num, fetched_at=fetched_at, location_info=location_info)
                            items.append(doc)
                        except Exception as e:
                            # Skip malformed entries, continue (same as original)
                            print(f"      ⚠️  item parse warning: {e}")
                
                if not items:
                    consecutive_empty_pages += 1
                    print(f"      ℹ️  No itemListElement entries found on this page")
                    continue
                
                consecutive_empty_pages = 0  # Reset on success
                
                # MongoDB upsert (same as original)
                res = bulk_upsert_items(coll, items)
                n_upserted = (res.upserted_count or 0)
                n_modified = (res.modified_count or 0)
                n_matched  = (res.matched_count or 0)
                location_items += len(items)
                total_items += len(items)
                
                print(f"      ✅ Inserted/Upserted {len(items)} items "
                      f"(upserted={n_upserted}, matched={n_matched}, modified={n_modified})")
                
                # Rate limiting (same as original)
                if page_num < max_pages:
                    time.sleep(args.delay)
                    
            except Exception as e:
                consecutive_empty_pages += 1
                print(f"      ❌ Error on page {page_num}: {e}")
                continue
        
        print(f"  📊 {location_info['city']} > {location_info['sublocation']}: {location_items} items scraped")
        
        # Delay between locations
        if loc_idx < len(sublocations):
            print(f"  ⏱️  Inter-location delay: {args.delay}s")
            time.sleep(args.delay)
    
    print(f"\\n🎉 All sub-locations complete!")
    print(f"📊 Sub-locations processed: {len(sublocations)}")
    print(f"📊 Total items scraped: {total_items}")
    print(f"📂 Output dir: {Path(args.out_dir).resolve()}")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Bayut CSV-driven Scraper with same logic as bayut_ldjson_to_mongo.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all Dubai sub-locations:
  python bayut_csv_scraper.py --csv bayut_sublocations_all_cities.csv --cities "Dubai"
  
  # Scrape high-volume sub-locations only:
  python bayut_csv_scraper.py --csv bayut_sublocations_all_cities.csv --min-listings 1000
  
  # Scrape multiple cities with custom page limit:
  python bayut_csv_scraper.py --csv bayut_sublocations_all_cities.csv --cities "Dubai,Abu Dhabi" --max-pages 20
        """
    )
    
    parser.add_argument("--csv", default="bayut_sublocations_all_cities.csv", help="CSV file with sub-location data")
    parser.add_argument("--cities", help="Comma-separated cities to scrape (e.g., 'Dubai,Abu Dhabi')")
    parser.add_argument("--min-listings", type=int, help="Only scrape sub-locations with >= this many listings")
    parser.add_argument("--max-pages", type=int, default=50, help="Maximum pages per sub-location (default: 50)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds")
    
    parser.add_argument("--out-dir", default="out", help="Output base directory")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="bayut_production", help="MongoDB database name")
    parser.add_argument("--collection", default="properties", help="MongoDB collection name")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        print("\\n🛑 Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\\n❌ Fatal error: {e}")
        sys.exit(1)