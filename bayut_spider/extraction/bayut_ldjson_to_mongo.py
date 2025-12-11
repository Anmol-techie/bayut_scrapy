#!/usr/bin/env python3
"""
Bayut LD+JSON → Mongo (with HTML + JSON archiving)

- Crawls listing pages like:
    https://www.bayut.com/for-sale/property/dubai/page-{i}/

- For each page i:
  1) Save raw HTML to      out/html/page_{i}.html
  2) Extract the SINGLE <script type="application/ld+json"> blob as-is and save:
                           out/json/page_{i}.json
  3) Split itemListElement -> one document per item (usually ~24) and upsert into MongoDB

Upsert key: property_id parsed from mainEntity.url "details-<id>.html".
If missing (rare), falls back to hashing the main URL.

CLI:
  python bayut_ldjson_to_mongo.py \
    --start 1 --end 50 \
    --base-url "https://www.bayut.com/for-sale/property/dubai/page-{}" \
    --mongo-uri "mongodb://localhost:27017" \
    --db bayut_production --collection listings

Env:
  COOKIE_STR          optional cookie string to pass as 'cookie' header
  BAYUT_UA            optional custom User-Agent
  TIMEOUT_SEC         per-request timeout (default 25)

Notes:
- This script intentionally keeps the LD+JSON **exact** (no transformations) in per-page files,
  while storing each item as its own MongoDB document with a small amount of added metadata.
"""

import argparse
import csv
import datetime as dt
from datetime import datetime
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
from pymongo import MongoClient, ReplaceOne, UpdateOne, ASCENDING
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------- Defaults ------------------------------

# Available locations in UAE
AVAILABLE_LOCATIONS = [
    "dubai", "abu-dhabi", "sharjah", "ajman", "ras-al-khaimah", 
    "fujairah", "umm-al-quwain", "al-ain"
]

DEF_BASE_URL = "https://www.bayut.com/for-sale/property/{location}/page-{{}}/?sort=date_desc"
DEF_OUT_DIR  = Path("out")
DEF_HTML_DIR = DEF_OUT_DIR / "html"
DEF_JSON_DIR = DEF_OUT_DIR / "json"

UA_DEFAULT = (
    os.getenv("BAYUT_UA")
    or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)
COOKIE_STR = os.getenv("COOKIE_STR", "").strip()
TIMEOUT = int(os.getenv("TIMEOUT_SEC", "25"))

HEADERS = {
    "User-Agent": UA_DEFAULT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}

DETAIL_ID_RE = re.compile(r"details-(\d+)\.html")

# --------------------------- HTTP session ----------------------------

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
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

# --------------------------- Extraction ------------------------------

def load_json_lenient(txt: str) -> Any:
    """Parse JSON, tolerating trailing commas (best-effort)."""
    try:
        return json.loads(txt)
    except Exception:
        fixed = re.sub(r",(\s*[}\]])", r"\1", txt)
        return json.loads(fixed)

def extract_single_ldjson(html: str) -> Any:
    """
    Extract the LD+JSON containing property listings.
    New Bayut structure: Look for JSON with itemListElement containing property data.
    """
    soup = BeautifulSoup(html, "lxml")
    tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    
    if not tags:
        raise RuntimeError("No <script type='application/ld+json'> found.")
    
    # Look for JSON with itemListElement property data (new structure)
    for tag in tags:
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        
        try:
            data = load_json_lenient(raw)
            
            # Check if this has itemListElement with property data
            if isinstance(data, dict) and "itemListElement" in data:
                items = data.get("itemListElement", [])
                if items and len(items) > 0:
                    first_item = items[0] if isinstance(items, list) else None
                    # Check if it has property data structure (mainEntity indicates property listings)
                    if (first_item and 
                        isinstance(first_item, dict) and 
                        "mainEntity" in first_item):
                        return data
                        
        except Exception:
            # Skip malformed JSON blocks
            continue
    
    # Fallback: Look for any JSON with @type ItemList (original structure)
    for tag in tags:
        raw = (tag.string or tag.text or "").strip()
        if not raw:
            continue
        
        try:
            data = load_json_lenient(raw)
            if isinstance(data, dict):
                dtype = data.get("@type")
                if dtype == "ItemList" or (isinstance(dtype, list) and "ItemList" in dtype):
                    if "itemListElement" in data:
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

def doc_from_item(item: Dict[str, Any], page_num: int, fetched_at: datetime, location_info=None, location: str = None, purpose: str = "for-sale") -> Dict[str, Any]:
    """
    Convert one itemListElement entry into a Mongo-ready document.
    We keep the original shape inside `raw_item` for lossless storage.
    
    Args:
        location_info: Dict with CSV location info (for CSV mode)
        location: String location (for regular location mode)
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

    # Build document with location info and purpose
    doc = {
        "_upsert_key": pid,              # used only for upsert key in code below
        "property_id": pid,              # persisted
        "position": pos,
        "detailed_url": url,             # Changed from details_url to detailed_url
        "page_number": page_num,
        "fetched_at": fetched_at,        # Now a datetime object
        "raw_item": item,                # full original item (ItemPage + mainEntity)
        "price": price,                  # Extract price for tracking
        "purpose": purpose,              # for-sale or for-rent
    }
    
    # Add location info based on mode
    if location_info:
        # CSV mode - full location hierarchy
        doc.update({
            "city": location_info["city"],
            "sublocation": location_info["sublocation"],
            "location_url": location_info["url"],
            "estimated_listings": location_info["listings"],
        })
    elif location:
        # Regular mode - just location string
        doc["location"] = location
    
    return doc

# ----------------------------- Mongo ---------------------------------

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
        # Create appearance record  
        # Handle both CSV mode (city/sublocation) and regular mode (location)
        location_str = d.get("location") or f"{d.get('city', '')}/{d.get('sublocation', '')}"
        appearance = {
            "page_number": d["page_number"],
            "position": d["position"],
            "location": location_str,
            "price": d.get("price"),
            "scraped_at": d["fetched_at"]  # Already a datetime object
        }
        
        # Use UpdateOne instead of ReplaceOne to track appearances
        ops.append(
            UpdateOne(
                {"property_id": d["_upsert_key"]},
                {
                    "$set": {
                        "property_id": d["property_id"],
                        "detailed_url": d["detailed_url"],  # Changed from details_url
                        "last_raw_item": d["raw_item"],
                        "last_seen": d["fetched_at"],  # DateTime object
                        "last_page": d["page_number"],
                        "last_position": d["position"],
                        "last_location": location_str,
                        "current_price": d.get("price"),
                        "purpose": d.get("purpose", "for-sale"),  # Add purpose field
                    },
                    "$addToSet": {
                        "locations_seen": location_str  # Track all locations where this property appeared
                    },
                    "$push": {
                        "appearances": {
                            "$each": [appearance],
                            "$slice": -100  # Keep last 100 appearances
                        }
                    },
                    "$setOnInsert": {
                        "first_seen": d["fetched_at"],  # DateTime object
                        "first_page": d["page_number"],
                        "first_location": location_str,
                        "detail_scraped": False,
                        "created_at": d["fetched_at"]  # DateTime object
                    }
                },
                upsert=True
            )
        )
    
    res = coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
    return res

# ------------------------------ IO -----------------------------------

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

# ------------------------------ Main ---------------------------------

def run_csv_mode(args):
    """CSV-driven mode: loop through sublocations from CSV"""
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
    
    # MongoDB connection
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[args.db]
    coll = db[args.collection]
    ensure_indexes(coll)
    print(f"🗄️  Mongo connected: {args.mongo_uri} → {args.db}.{args.collection}")
    
    # HTTP session
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
        url_parts = location_info["url"].rstrip("/").split("/")
        city_slug = url_parts[-2]  # 'dubai'
        sublocation_slug = url_parts[-1]  # 'dubai-marina'
        
        # Prepare output dirs for this location
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
                
            # Generate URL for this page
            url = f"https://www.bayut.com/for-sale/property/{city_slug}/{sublocation_slug}/page-{page_num}/?sort=date_desc"
            print(f"\\n    📄 Page {page_num}: {url}")
            
            try:
                r = sess.get(url, headers=headers, timeout=TIMEOUT)
                
                print(f"      📊 Response: HTTP {r.status_code}, Size: {len(r.text)} bytes")
                
                if r.status_code != 200:
                    consecutive_empty_pages += 1
                    print(f"      ❌ HTTP {r.status_code}")
                    if r.status_code == 404:
                        print(f"      ℹ️  Page not found - likely end of listings")
                    continue
                
                html = r.text
                
                # Check response validity
                if len(html) < 1000:
                    consecutive_empty_pages += 1
                    print(f"      ⚠️  Small response: {len(html)} bytes")
                    continue
                
                # Save files first
                save_html(location_html_dir, page_num, html)
                
                # Try to extract LD+JSON - this is the real test for blocking
                ldjson = extract_single_ldjson(html)
                save_json(location_json_dir, page_num, ldjson)
                
                # Check for actual blocking by testing if we got valid data
                if not ldjson:
                    consecutive_empty_pages += 1
                    print(f"      ⚠️  No LD+JSON data found - possible blocking or end of pages")
                    continue
                
                # Process items (same logic as original)
                fetched_at = datetime.utcnow()  # Use datetime object instead of string
                items = []
                if isinstance(ldjson, dict):
                    elts = ldjson.get("itemListElement") or []
                    for el in elts:
                        try:
                            # Determine purpose from URL
                            purpose = "for-sale" if "/for-sale/" in url else "to-rent" if "/to-rent/" in url else "for-sale"
                            doc = doc_from_item(el, page_num=page_num, fetched_at=fetched_at, location_info=location_info, purpose=purpose)
                            items.append(doc)
                        except Exception as e:
                            print(f"      ⚠️  item parse warning: {e}")
                
                if not items:
                    consecutive_empty_pages += 1
                    print(f"      ℹ️  No items found")
                    continue
                
                consecutive_empty_pages = 0  # Reset on success
                
                # MongoDB upsert (same as original)
                res = bulk_upsert_items(coll, items)
                n_upserted = (res.upserted_count or 0)
                n_modified = (res.modified_count or 0)
                n_matched  = (res.matched_count or 0)
                location_items += len(items)
                total_items += len(items)
                
                print(f"      ✅ {len(items)} items (upserted={n_upserted}, matched={n_matched}, modified={n_modified})")
                
                # Rate limiting
                if page_num < max_pages:
                    time.sleep(args.delay)
                    
            except Exception as e:
                consecutive_empty_pages += 1
                print(f"      ❌ Error: {e}")
                continue
        
        print(f"  📊 {location_info['city']} > {location_info['sublocation']}: {location_items} items scraped")
        
        # Delay between locations
        if loc_idx < len(sublocations):
            time.sleep(args.delay)
    
    print(f"\\n🎉 CSV mode complete!")
    print(f"📊 Sub-locations processed: {len(sublocations)}")
    print(f"📊 Total items scraped: {total_items}")

def run(args):
    # Check if CSV mode or regular location mode
    if args.csv:
        return run_csv_mode(args)
    else:
        return run_location_mode(args)

def run_location_mode(args):
    # Parse locations
    locations = [loc.strip() for loc in args.locations.split(',')] if args.locations else ["dubai"]
    
    # Validate locations
    invalid_locations = [loc for loc in locations if loc not in AVAILABLE_LOCATIONS]
    if invalid_locations:
        print(f"❌ Invalid locations: {invalid_locations}")
        print(f"Available locations: {', '.join(AVAILABLE_LOCATIONS)}")
        return
    
    print(f"🌍 Processing locations: {', '.join(locations)}")
    
    # Mongo
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[args.db]
    coll = db[args.collection]
    ensure_indexes(coll)
    print(f"🗄️  Mongo connected: {args.mongo_uri} → {args.db}.{args.collection}")

    # HTTP session
    sess = get_session()
    headers = dict(HEADERS)
    if COOKIE_STR:
        headers["cookie"] = COOKIE_STR

    total_items = 0
    
    for location in locations:
        print(f"\n🏙️  === PROCESSING LOCATION: {location.upper()} ===")
        
        # Prepare output dirs for this location
        location_html_dir = Path(args.out_dir) / "html" / location
        location_json_dir = Path(args.out_dir) / "json" / location
        location_html_dir.mkdir(parents=True, exist_ok=True)
        location_json_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate URL template for this location
        base_url = DEF_BASE_URL.format(location=location)
        
        location_items = 0
        for i in range(args.start, args.end + 1):
            url = base_url.format(i)
            print(f"\n➡️  {location} Page {i}: {url}")
            try:
                r = sess.get(url, headers=headers, timeout=TIMEOUT)
                
                # Log detailed response info
                print(f"  📊 Response: HTTP {r.status_code}, Size: {len(r.text)} bytes, Headers: {dict(r.headers).get('content-type', 'unknown')}")
                
                if r.status_code != 200:
                    print(f"  ❌ HTTP {r.status_code}")
                    if r.status_code == 429:
                        print(f"  ⚠️  Rate limited! Consider adding delays")
                    elif r.status_code == 403:
                        print(f"  ⚠️  Forbidden! May need different headers or cookies")
                    elif r.status_code == 503:
                        print(f"  ⚠️  Service unavailable! Server might be blocking")
                    elif r.status_code == 404:
                        print(f"  ℹ️  Page not found - might be end of listings for {location}")
                    continue

                html = r.text
                
                # Check if we got a valid HTML response
                if len(html) < 1000:
                    print(f"  ⚠️  Suspiciously small response: {len(html)} bytes")
                    print(f"  📝 First 500 chars: {html[:500]}")
                
                # Save to location-specific directories
                save_html(location_html_dir, i, html)
                print(f"  💾 Saved HTML → {location_html_dir / f'page_{i}.html'} ({len(html)} bytes)")

                ldjson = extract_single_ldjson(html)
                save_json(location_json_dir, i, ldjson)
                print(f"  💾 Saved LD+JSON → {location_json_dir / f'page_{i}.json'}")
                
                # Check for actual blocking by testing if we got valid data
                if not ldjson:
                    print(f"  ⚠️  No LD+JSON data found - possible blocking or end of pages")
                    continue

                # Split into per-item docs
                fetched_at = datetime.utcnow()  # Use datetime object instead of string
                items = []
                if isinstance(ldjson, dict):
                    elts = ldjson.get("itemListElement") or []
                    for el in elts:
                        try:
                            # Determine purpose from URL
                            purpose = "for-sale" if "/for-sale/" in url else "to-rent" if "/to-rent/" in url else "for-sale"
                            doc = doc_from_item(el, page_num=i, fetched_at=fetched_at, location=location, purpose=purpose)
                            items.append(doc)
                        except Exception as e:
                            # Skip malformed entries, continue
                            print(f"  ⚠️  item parse warning: {e}")

                if not items:
                    print(f"  ℹ️  No itemListElement entries found on this page for {location}.")
                    continue

                res = bulk_upsert_items(coll, items)
                n_upserted = (res.upserted_count or 0)
                n_modified = (res.modified_count or 0)
                n_matched  = (res.matched_count or 0)
                location_items += len(items)
                total_items += len(items)

                print(f"  ✅ Inserted/Upserted {len(items)} items for {location} "
                      f"(upserted={n_upserted}, matched={n_matched}, modified={n_modified})")

            except Exception as e:
                print(f"  ❌ Error on {location} page {i}: {e}")
                
                # If this was an extraction error, let's examine the HTML
                if "No <script type='application/ld+json'> found" in str(e):
                    html_file = location_html_dir / f"page_{i}.html"
                    if html_file.exists():
                        with open(html_file) as f:
                            content = f.read()
                        print(f"  🔍 HTML file size: {len(content)} bytes")
                        
                        # Check for common blocking patterns
                        if len(content) < 5000:
                            print(f"  📝 HTML preview: {content[:500]}...")
                        
                        if "access denied" in content.lower() or "blocked" in content.lower():
                            print(f"  🚫 Access denied detected!")
                        
                        # Count script tags
                        script_count = content.count("<script")
                        ldjson_count = content.count('type="application/ld+json"')
                        print(f"  📊 Scripts: {script_count}, LD+JSON scripts: {ldjson_count}")

            # Add delay between requests to avoid rate limiting
            if i < args.end or location != locations[-1]:  # Don't delay after last page of last location
                delay = args.delay
                print(f"  ⏱️  Sleeping {delay}s to avoid rate limiting...")
                time.sleep(delay)
        
        print(f"\n📊 Location {location.upper()} complete: {location_items} items processed")

    print(f"\n🎉 All locations complete! Pages: {args.start}–{args.end}. Total items processed: {total_items}.")
    print(f"📂 Output dir: {Path(args.out_dir).resolve()}")

def parse_args():
    p = argparse.ArgumentParser(
        description="Bayut LD+JSON archiver + Mongo importer - supports both location and CSV modes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  ## Regular Location Mode:
  # Scrape Dubai pages 1-50:
  python bayut_ldjson_to_mongo.py --locations dubai --start 1 --end 50
  
  # Scrape multiple locations:
  python bayut_ldjson_to_mongo.py --locations "dubai,abu-dhabi,sharjah" --start 1 --end 20
  
  ## CSV Sub-location Mode:
  # Scrape all Dubai sub-locations:
  python bayut_ldjson_to_mongo.py --csv bayut_sublocations_all_cities.csv --cities "Dubai"
  
  # Scrape high-volume sub-locations only:
  python bayut_ldjson_to_mongo.py --csv bayut_sublocations_all_cities.csv --min-listings 1000
  
  # Scrape multiple cities with custom limits:
  python bayut_ldjson_to_mongo.py --csv bayut_sublocations_all_cities.csv --cities "Dubai,Abu Dhabi" --max-pages 20
        """
    )
    
    # Mode selection
    p.add_argument("--csv", help="CSV file with sub-location data (enables CSV mode)")
    
    # Location mode arguments
    p.add_argument("--locations", default="dubai", 
                   help=f"[Location mode] Comma-separated locations. Available: {', '.join(AVAILABLE_LOCATIONS)}")
    p.add_argument("--start", type=int, default=1, help="[Location mode] Start page number")
    p.add_argument("--end", type=int, default=3, help="[Location mode] End page number")
    
    # CSV mode arguments
    p.add_argument("--cities", help="[CSV mode] Comma-separated cities to filter (e.g., 'Dubai,Abu Dhabi')")
    p.add_argument("--min-listings", type=int, help="[CSV mode] Only scrape sub-locations with >= this many listings")
    p.add_argument("--max-pages", type=int, default=50, help="[CSV mode] Maximum pages per sub-location")
    
    # Common arguments
    p.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds")
    p.add_argument("--out-dir", default=str(DEF_OUT_DIR), help="Output base directory")
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    p.add_argument("--db", default="bayut_production", help="MongoDB database name")
    p.add_argument("--collection", default="properties", help="MongoDB collection name")
    
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted.")
        sys.exit(130)
