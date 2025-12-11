#!/usr/bin/env python3
"""
Bayut Incremental Property Scraper
Daily scraper for new properties from UAE-wide listings
Stops when consecutive existing properties are found
"""

import os
import sys
import time
import json
import signal
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import DuplicateKeyError
from bs4 import BeautifulSoup
import hashlib
import re

# Configuration
DEFAULT_DELAY = 2.0  # seconds between requests
DEFAULT_TIMEOUT = 30  # request timeout
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
CONSECUTIVE_EXISTING_LIMIT = 2  # Stop after this many consecutive existing properties

# UAE-wide URL template
UAE_BASE_URL = "https://www.bayut.com/for-sale/property/uae/page-{}/?sort=date_desc"

# Global flag for graceful shutdown
SHUTDOWN_REQUESTED = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global SHUTDOWN_REQUESTED
    print(f"\n⚠️  Shutdown requested... Will stop after current page")
    SHUTDOWN_REQUESTED = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class BayutIncrementalScraper:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                 db_name="bayut_production",
                 delay=DEFAULT_DELAY,
                 save_html=False,
                 html_dir="incremental_html"):
        """
        Initialize the incremental scraper
        """
        self.delay = delay
        self.save_html = save_html
        self.html_dir = Path(html_dir)
        
        # MongoDB setup
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.properties_coll = self.db['sublocation_properties']
        
        # Test connection
        self.client.admin.command("ping")
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        })
        
        # Statistics
        self.stats = {
            'pages_scraped': 0,
            'new_properties': 0,
            'existing_properties': 0,
            'consecutive_existing': 0,
            'start_time': None,
            'errors': []
        }
        
        # Regex for property ID extraction
        self.property_id_re = re.compile(r"details-(\d+)\.html")
    
    def load_json_lenient(self, txt: str) -> Any:
        """Parse JSON, tolerating trailing commas (best-effort)."""
        try:
            return json.loads(txt)
        except Exception:
            fixed = re.sub(r",(\s*[}\]])", r"\1", txt)
            return json.loads(fixed)
    
    def extract_single_ldjson(self, html: str) -> Any:
        """Extract the LD+JSON containing property listings."""
        soup = BeautifulSoup(html, "lxml")
        tags = soup.find_all("script", attrs={"type": "application/ld+json"})
        
        if not tags:
            raise RuntimeError("No <script type='application/ld+json'> found.")
        
        # Look for JSON with itemListElement property data
        for tag in tags:
            raw = (tag.string or tag.text or "").strip()
            if not raw:
                continue
            
            try:
                data = self.load_json_lenient(raw)
                
                # Check if this has itemListElement with property data
                if isinstance(data, dict) and "itemListElement" in data:
                    items = data.get("itemListElement", [])
                    if items and len(items) > 0:
                        first_item = items[0] if isinstance(items, list) else None
                        # Check if it has property data structure
                        if (first_item and 
                            isinstance(first_item, dict) and 
                            "mainEntity" in first_item):
                            return data
                            
            except Exception:
                continue
        
        # Fallback: Look for any JSON with @type ItemList
        for tag in tags:
            raw = (tag.string or tag.text or "").strip()
            if not raw:
                continue
            
            try:
                data = self.load_json_lenient(raw)
                if isinstance(data, dict):
                    dtype = data.get("@type")
                    if dtype == "ItemList" or (isinstance(dtype, list) and "ItemList" in dtype):
                        if "itemListElement" in data:
                            return data
            except Exception:
                continue
        
        raise RuntimeError("No valid LD+JSON found in any script tags.")
    
    def property_id_from_url(self, url: Optional[str]) -> Optional[str]:
        """Extract property ID from URL"""
        if not url:
            return None
        m = self.property_id_re.search(url)
        if m:
            return m.group(1)
        return None
    
    def property_exists(self, property_id: str) -> bool:
        """Check if property already exists in database"""
        return self.properties_coll.find_one({'property_id': property_id}) is not None
    
    def extract_property_from_item(self, item: Dict[str, Any], page_num: int) -> Optional[Dict[str, Any]]:
        """Extract property data from an itemListElement entry"""
        try:
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
            
            pid = self.property_id_from_url(url)
            
            # Fallback key if no property_id is available
            if not pid:
                base = (url or json.dumps(main, ensure_ascii=False))[:256]
                pid = "hash_" + hashlib.md5(base.encode("utf-8", errors="ignore")).hexdigest()
            
            # Build document
            doc = {
                "_upsert_key": pid,
                "property_id": pid,
                "position": pos,
                "detailed_url": url,
                "page_number": page_num,
                "fetched_at": datetime.utcnow(),
                "raw_item": item,
                "price": price,
                "purpose": "for-sale",  # UAE-wide listings are for-sale
                "location": "UAE",  # General UAE location
                "detail_scraped": False,
                "created_at": datetime.utcnow()
            }
            
            return doc
            
        except Exception as e:
            print(f"    ⚠️  Error extracting property: {e}")
            return None
    
    def process_page(self, page_num: int) -> Dict[str, int]:
        """
        Process a single page and return statistics
        Returns: {'new': count, 'existing': count, 'errors': count}
        """
        url = UAE_BASE_URL.format(page_num)
        print(f"\n📄 Page {page_num}: {url}")
        
        try:
            response = self.session.get(url, timeout=DEFAULT_TIMEOUT)
            
            if response.status_code != 200:
                print(f"  ❌ HTTP {response.status_code}")
                return {'new': 0, 'existing': 0, 'errors': 1}
            
            html = response.text
            
            # Check response validity
            if len(html) < 1000:
                print(f"  ⚠️  Small response: {len(html)} bytes")
                return {'new': 0, 'existing': 0, 'errors': 1}
            
            # Save HTML if requested
            if self.save_html:
                self._save_html(html, page_num)
            
            # Extract LD+JSON
            ldjson = self.extract_single_ldjson(html)
            
            if not ldjson:
                print(f"  ⚠️  No LD+JSON data found")
                return {'new': 0, 'existing': 0, 'errors': 1}
            
            # Process properties
            return self._process_properties(ldjson, page_num)
            
        except Exception as e:
            print(f"  ❌ Error processing page {page_num}: {e}")
            self.stats['errors'].append({'page': page_num, 'error': str(e)})
            return {'new': 0, 'existing': 0, 'errors': 1}
    
    def _save_html(self, html: str, page_num: int):
        """Save HTML content to file"""
        self.html_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.html_dir / f"uae_page_{page_num}.html"
        file_path.write_text(html, encoding='utf-8')
    
    def _process_properties(self, ldjson: Dict[str, Any], page_num: int) -> Dict[str, int]:
        """Process properties from LD+JSON data"""
        result = {'new': 0, 'existing': 0, 'errors': 0}
        
        if not isinstance(ldjson, dict):
            return result
        
        elts = ldjson.get("itemListElement") or []
        if not elts:
            print(f"  ℹ️  No items found on page {page_num}")
            return result
        
        new_properties = []
        consecutive_existing = 0
        
        for el in elts:
            try:
                # Extract property data
                prop_doc = self.extract_property_from_item(el, page_num)
                if not prop_doc:
                    result['errors'] += 1
                    continue
                
                property_id = prop_doc['property_id']
                
                # Check if property exists
                if self.property_exists(property_id):
                    print(f"    ⏭️  Property {property_id} already exists")
                    result['existing'] += 1
                    consecutive_existing += 1
                    
                    # Check if we should stop
                    if consecutive_existing >= CONSECUTIVE_EXISTING_LIMIT:
                        print(f"    🛑 Found {consecutive_existing} consecutive existing properties - stopping")
                        self.stats['consecutive_existing'] = consecutive_existing
                        break
                else:
                    print(f"    ✨ New property: {property_id}")
                    new_properties.append(prop_doc)
                    result['new'] += 1
                    consecutive_existing = 0  # Reset counter
                    
            except Exception as e:
                print(f"    ⚠️  Error processing property: {e}")
                result['errors'] += 1
        
        # Bulk insert new properties
        if new_properties:
            self._bulk_insert_properties(new_properties)
            print(f"  ✅ Inserted {len(new_properties)} new properties")
        
        # Update global consecutive count
        if consecutive_existing >= CONSECUTIVE_EXISTING_LIMIT:
            self.stats['consecutive_existing'] = consecutive_existing
        
        return result
    
    def _bulk_insert_properties(self, properties: List[Dict[str, Any]]):
        """Bulk insert new properties into MongoDB"""
        ops = []
        for prop in properties:
            # Create appearance record
            appearance = {
                "page_number": prop["page_number"],
                "position": prop["position"],
                "location": prop["location"],
                "price": prop.get("price"),
                "scraped_at": prop["fetched_at"]
            }
            
            ops.append(
                UpdateOne(
                    {"property_id": prop["_upsert_key"]},
                    {
                        "$set": {
                            "property_id": prop["property_id"],
                            "detailed_url": prop["detailed_url"],
                            "last_raw_item": prop["raw_item"],
                            "last_seen": prop["fetched_at"],
                            "last_page": prop["page_number"],
                            "last_position": prop["position"],
                            "last_location": prop["location"],
                            "current_price": prop.get("price"),
                            "purpose": prop["purpose"],
                            "detail_scraped": False
                        },
                        "$addToSet": {
                            "locations_seen": prop["location"]
                        },
                        "$push": {
                            "appearances": {
                                "$each": [appearance],
                                "$slice": -100
                            }
                        },
                        "$setOnInsert": {
                            "first_seen": prop["fetched_at"],
                            "first_page": prop["page_number"],
                            "first_location": prop["location"],
                            "created_at": prop["fetched_at"]
                        }
                    },
                    upsert=True
                )
            )
        
        # Execute bulk write
        self.properties_coll.bulk_write(ops, ordered=False)
    
    def run(self, max_pages: int = 50) -> Dict[str, Any]:
        """
        Main scraping loop - stops when consecutive existing properties found
        """
        global SHUTDOWN_REQUESTED
        
        self.stats['start_time'] = datetime.now()
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting Bayut Incremental Scraper")
        print(f"{'='*60}")
        print(f"🌍 URL Pattern: {UAE_BASE_URL}")
        print(f"⏱️  Delay: {self.delay} seconds")
        print(f"🛑 Stop after {CONSECUTIVE_EXISTING_LIMIT} consecutive existing properties")
        print(f"📄 Max pages: {max_pages}")
        print(f"{'='*60}\n")
        
        page_num = 1
        should_stop = False
        
        while page_num <= max_pages and not should_stop and not SHUTDOWN_REQUESTED:
            # Process page
            page_result = self.process_page(page_num)
            
            # Update statistics
            self.stats['pages_scraped'] += 1
            self.stats['new_properties'] += page_result['new']
            self.stats['existing_properties'] += page_result['existing']
            
            # Check if we should stop (consecutive existing properties found)
            if self.stats['consecutive_existing'] >= CONSECUTIVE_EXISTING_LIMIT:
                print(f"\n🛑 Stopping: Found {self.stats['consecutive_existing']} consecutive existing properties")
                should_stop = True
                break
            
            # Progress update
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            rate = page_num / elapsed if elapsed > 0 else 0
            
            print(f"  📊 Page {page_num} complete: {page_result['new']} new, {page_result['existing']} existing")
            print(f"  📈 Overall: {self.stats['new_properties']} new, {self.stats['existing_properties']} existing ({rate:.2f} pages/sec)")
            
            # Delay before next page
            if page_num < max_pages and not should_stop and not SHUTDOWN_REQUESTED:
                time.sleep(self.delay)
            
            page_num += 1
        
        # Print final statistics
        self._print_statistics()
        
        return {
            'pages_scraped': self.stats['pages_scraped'],
            'new_properties': self.stats['new_properties'],
            'existing_properties': self.stats['existing_properties'],
            'stopped_early': should_stop or SHUTDOWN_REQUESTED,
            'reason': 'consecutive_existing' if should_stop else 'shutdown' if SHUTDOWN_REQUESTED else 'completed'
        }
    
    def _print_statistics(self):
        """Print final statistics"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print(f"\n{'='*60}")
        print(f"📊 INCREMENTAL SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Pages scraped: {self.stats['pages_scraped']}")
        print(f"✨ New properties: {self.stats['new_properties']}")
        print(f"⏭️  Existing properties: {self.stats['existing_properties']}")
        print(f"⏱️  Total time: {timedelta(seconds=int(elapsed))}")
        print(f"📈 Average rate: {self.stats['pages_scraped'] / elapsed:.2f} pages/second" if elapsed > 0 else "")
        
        if self.stats['errors']:
            print(f"\n⚠️  Errors: {len(self.stats['errors'])}")
        
        print(f"{'='*60}\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Bayut Incremental Property Scraper - Daily new property detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard daily run
  python bayut_incremental_scraper.py
  
  # Custom delay
  python bayut_incremental_scraper.py --delay 3
  
  # Save HTML files
  python bayut_incremental_scraper.py --save-html
  
  # Increase max pages
  python bayut_incremental_scraper.py --max-pages 100
        """
    )
    
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests in seconds (default: 2.0)')
    parser.add_argument('--max-pages', type=int, default=50,
                       help='Maximum pages to scrape (default: 50)')
    parser.add_argument('--save-html', action='store_true',
                       help='Save HTML files locally')
    parser.add_argument('--html-dir', default='incremental_html',
                       help='Directory to save HTML files')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017/',
                       help='MongoDB connection URI')
    parser.add_argument('--db', default='bayut_production',
                       help='Database name')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = BayutIncrementalScraper(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        delay=args.delay,
        save_html=args.save_html,
        html_dir=args.html_dir
    )
    
    # Run scraper
    result = scraper.run(max_pages=args.max_pages)
    
    # Exit with appropriate code
    if result['new_properties'] > 0:
        print(f"✅ Found {result['new_properties']} new properties")
        sys.exit(0)  # Success
    else:
        print("ℹ️  No new properties found")
        sys.exit(0)  # Still success, just no new data


if __name__ == "__main__":
    main()