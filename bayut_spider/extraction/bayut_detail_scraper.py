#!/usr/bin/env python3
"""
Bayut Property Detail Scraper
Fetches detailed property data from detailed_url and stores in property_details collection
Sequential processing with 2-second delays to avoid rate limiting
"""

import os
import sys
import time
import json
import signal
import argparse
import requests
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import DuplicateKeyError
from bs4 import BeautifulSoup

# Import the extractor
from z_bayut_complete_extractor import BayutPropertyExtractor

# Configuration
DEFAULT_DELAY = 0  # NO DELAY for maximum speed
DEFAULT_BATCH_SIZE = 100  # checkpoint every N properties
DEFAULT_TIMEOUT = 30  # request timeout
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

# Anti-bot detection configuration
BOT_DETECTION_THRESHOLD = 3  # Number of consecutive bot challenges before cooldown
INITIAL_COOLDOWN = 15 * 60  # 15 minutes initial cooldown
COOLDOWN_MULTIPLIER = 1.5  # Multiply cooldown by 1.5x each time (infinite cycle)

# Global flag for graceful shutdown
SHUTDOWN_REQUESTED = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global SHUTDOWN_REQUESTED
    print(f"\n⚠️  Shutdown requested... Will stop after current property")
    SHUTDOWN_REQUESTED = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class BayutDetailScraper:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                 db_name="bayut_production",
                 delay=DEFAULT_DELAY,
                 save_html=False,
                 html_dir="detail_html"):
        """
        Initialize the detail scraper
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            delay: Delay between requests in seconds
            save_html: Whether to save HTML files
            html_dir: Directory to save HTML files
        """
        self.delay = delay
        self.save_html = save_html
        self.html_dir = Path(html_dir)
        
        # MongoDB setup
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.properties_coll = self.db['sublocation_properties']
        self.details_coll = self.db['property_details']
        
        # Create indexes
        self._create_indexes()
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        })
        
        # Statistics
        self.stats = {
            'total': 0,
            'processed': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'errors': [],
            'bot_challenges': 0,
            'cooldowns': 0
        }
        
        # Anti-bot detection
        self.consecutive_challenges = 0
        self.current_cooldown = INITIAL_COOLDOWN
        self.cooldown_count = 0
        self.last_challenge_time = None
        
        # Property extractor
        self.extractor = BayutPropertyExtractor()
    
    def _create_indexes(self):
        """Create indexes for the collections"""
        try:
            # Indexes for property_details collection
            self.details_coll.create_index([("property_id", ASCENDING)], unique=True, background=True)
            self.details_coll.create_index([("scraped_at", ASCENDING)], background=True)
            self.details_coll.create_index([("extraction_success", ASCENDING)], background=True)
            print("✅ Indexes created/verified for property_details collection")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")
    
    def fetch_property_html(self, url: str, property_id: str) -> Optional[str]:
        """
        Fetch HTML content from a property URL
        
        Args:
            url: Property detail URL
            property_id: Property ID for tracking
            
        Returns:
            HTML content or None if failed
        """
        try:
            response = self.session.get(url, timeout=DEFAULT_TIMEOUT)
            
            if response.status_code == 200:
                html = response.text
                
                # Save HTML if requested
                if self.save_html:
                    self._save_html(html, property_id)
                
                return html
            else:
                print(f"  ❌ HTTP {response.status_code} for property {property_id}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error for property {property_id}: {e}")
            return None
    
    def _save_html(self, html: str, property_id: str):
        """Save HTML content to file"""
        self.html_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.html_dir / f"property_{property_id}.html"
        file_path.write_text(html, encoding='utf-8')
    
    def _detect_bot_challenge(self, html: str, extracted_data: Dict[str, Any]) -> bool:
        """
        Detect if the response is a bot challenge/empty response
        
        Args:
            html: HTML content
            extracted_data: Extracted property data
            
        Returns:
            True if bot challenge detected, False otherwise
        """
        # Check for minimal extracted data (indicates challenge/blocked content)
        essential_fields = ['price', 'bedrooms', 'headline', 'locality']
        missing_fields = sum(1 for field in essential_fields if not extracted_data.get(field))
        
        # If most essential fields are missing, likely a challenge
        if missing_fields >= 3:
            return True
            
        # Check for common anti-bot patterns in HTML
        html_lower = html.lower()
        bot_indicators = [
            'please verify you are human',
            'captcha',
            'robot',
            'cloudflare',
            'access denied',
            'blocked',
            'rate limit',
            'too many requests',
            'suspicious activity'
        ]
        
        for indicator in bot_indicators:
            if indicator in html_lower:
                return True
                
        return False
    
    def _handle_cooldown(self):
        """
        Handle infinite cooldown periods with 1.5x multiplier
        Never stops, always increases cooldown by 1.5x
        """
        global SHUTDOWN_REQUESTED
        cooldown_minutes = self.current_cooldown // 60
        
        print(f"\n🤖 Bot detection triggered! Starting cooldown #{self.cooldown_count + 1}")
        print(f"⏰ Cooldown period: {cooldown_minutes:.1f} minutes ({self.current_cooldown} seconds)")
        print(f"🔄 Script will automatically resume after cooldown...")
        print(f"📈 Next cooldown will be: {(self.current_cooldown * COOLDOWN_MULTIPLIER) // 60:.1f} minutes")
        print(f"💡 Press Ctrl+C to stop the script during cooldown")
        
        self.stats['cooldowns'] += 1
        self.cooldown_count += 1
        
        # Sleep for cooldown period with interruptible sleep
        cooldown_end = time.time() + self.current_cooldown
        while time.time() < cooldown_end and not SHUTDOWN_REQUESTED:
            remaining = int(cooldown_end - time.time())
            if remaining > 0:
                print(f"\r⏳ Cooldown remaining: {remaining // 60}m {remaining % 60}s ", end="", flush=True)
                time.sleep(1)
        
        if SHUTDOWN_REQUESTED:
            print(f"\n🛑 Shutdown requested during cooldown")
            return False
        
        # Increase cooldown for next time (infinite cycle with 1.5x multiplier)
        self.current_cooldown = int(self.current_cooldown * COOLDOWN_MULTIPLIER)
        
        # Reset bot detection counters
        self.consecutive_challenges = 0
        self.last_challenge_time = None
        
        print(f"\n✅ Cooldown complete! Resuming scraping...")
        return True
    
    def _restart_script(self):
        """
        Restart the current script with same arguments
        """
        print(f"\n🔄 Restarting script after cooldown...")
        
        # Get current script arguments
        current_args = sys.argv
        
        # Execute the script with same arguments
        subprocess.Popen([sys.executable] + current_args)
        
        # Exit current process
        sys.exit(0)
    
    def process_property(self, property_doc: Dict[str, Any]) -> bool:
        """
        Process a single property - fetch and extract details
        
        Args:
            property_doc: Property document from sublocation_properties
            
        Returns:
            True if successful, False otherwise
        """
        property_id = property_doc['property_id']
        url = property_doc.get('detailed_url')
        
        if not url:
            print(f"  ⚠️  No detailed_url for property {property_id}")
            return False
        
        # Check if already scraped with successful data extraction
        existing = self.details_coll.find_one({'property_id': property_id, 'extraction_success': True})
        if existing:
            print(f"  ⏭️  Property {property_id} already scraped")
            self.stats['skipped'] += 1
            return True
        
        # Fetch HTML
        html = self.fetch_property_html(url, property_id)
        if not html:
            self.stats['failed'] += 1
            # Store failure record using upsert
            self.details_coll.update_one(
                {'property_id': property_id},
                {'$set': {
                    'property_id': property_id,
                    'url': url,
                    'scraped_at': datetime.now(timezone.utc),
                    'extraction_success': False,
                    'error': 'Failed to fetch HTML'
                }},
                upsert=True
            )
            return False
        
        # Extract data
        try:
            extracted_data = self.extractor.extract_all(html, f"property_{property_id}.html")
            
            # Check for bot challenge/empty response
            is_bot_challenge = self._detect_bot_challenge(html, extracted_data)
            
            if is_bot_challenge:
                self.consecutive_challenges += 1
                self.stats['bot_challenges'] += 1
                self.last_challenge_time = datetime.now(timezone.utc)
                
                print(f"  🤖 Bot challenge detected for property {property_id} (consecutive: {self.consecutive_challenges})")
                
                # Store challenge record using upsert
                self.details_coll.update_one(
                    {'property_id': property_id},
                    {'$set': {
                        'property_id': property_id,
                        'url': url,
                        'scraped_at': datetime.now(timezone.utc),
                        'extraction_success': False,
                        'bot_challenge': True,
                        'extracted_data': extracted_data
                    }},
                    upsert=True
                )
                
                # Check if we need to trigger cooldown
                if self.consecutive_challenges >= BOT_DETECTION_THRESHOLD:
                    print(f"🚨 Bot detection threshold reached ({BOT_DETECTION_THRESHOLD} consecutive challenges)")
                    if not self._handle_cooldown():  # Check if shutdown was requested
                        return False
                
                return False
            else:
                # Reset challenge counter on successful extraction
                self.consecutive_challenges = 0
                self.last_challenge_time = None
            
            # Prepare document for MongoDB
            detail_doc = {
                'property_id': property_id,
                'url': url,
                'scraped_at': datetime.now(timezone.utc),
                'extraction_success': True,
                'extracted_data': extracted_data
            }
            
            # Insert/update property_details using upsert
            self.details_coll.update_one(
                {'property_id': property_id},
                {'$set': detail_doc},
                upsert=True
            )
            
            # Update main collection
            self.properties_coll.update_one(
                {'property_id': property_id},
                {'$set': {'detail_scraped': True, 'detail_scraped_at': datetime.now(timezone.utc)}}
            )
            
            self.stats['success'] += 1
            
            # Print key extracted info
            price = extracted_data.get('price') or extracted_data.get('pricing_details', {}).get('price')
            bedrooms = extracted_data.get('bedrooms')
            location = extracted_data.get('locality')
            
            print(f"  ✅ Property {property_id}: {bedrooms}BR, AED {price:,.0f} in {location}" if price else f"  ✅ Property {property_id} scraped")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Extraction error for property {property_id}: {e}")
            self.stats['failed'] += 1
            self.stats['errors'].append({'property_id': property_id, 'error': str(e)})
            
            # Store failure record using upsert
            self.details_coll.update_one(
                {'property_id': property_id},
                {'$set': {
                    'property_id': property_id,
                    'url': url,
                    'scraped_at': datetime.now(timezone.utc),
                    'extraction_success': False,
                    'error': str(e)
                }},
                upsert=True
            )
            
            return False
    
    def run(self, limit: Optional[int] = None, skip: int = 0):
        """
        Main scraping loop
        
        Args:
            limit: Maximum number of properties to scrape (None for all)
            skip: Number of properties to skip
        """
        global SHUTDOWN_REQUESTED
        
        self.stats['start_time'] = datetime.now()
        
        # Count properties to scrape
        query = {'detail_scraped': {'$ne': True}, 'detailed_url': {'$exists': True, '$ne': None}}
        total_to_scrape = self.properties_coll.count_documents(query)
        
        if limit:
            total_to_scrape = min(total_to_scrape, limit)
        
        self.stats['total'] = total_to_scrape
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting Bayut Detail Scraper")
        print(f"{'='*60}")
        print(f"📊 Properties to scrape: {total_to_scrape:,}")
        print(f"⚡ Speed mode: {'NO DELAYS - Maximum speed!' if self.delay == 0 else f'{self.delay} seconds delay'}")
        print(f"💾 Save HTML: {'Yes' if self.save_html else 'No'}")
        if limit:
            print(f"🎯 Limit: {limit:,} properties")
        if skip:
            print(f"⏭️  Skipping first {skip:,} properties")
        
        # Estimate time
        estimated_hours = (total_to_scrape * (self.delay + 2)) / 3600  # +2 sec for processing
        print(f"⏰ Estimated time: {estimated_hours:.1f} hours")
        print(f"{'='*60}\n")
        
        # Get properties to scrape
        cursor = self.properties_coll.find(query).skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        batch_count = 0
        batch_start = datetime.now()
        
        for property_doc in cursor:
            if SHUTDOWN_REQUESTED:
                print("\n🛑 Shutdown requested, stopping...")
                break
            
            self.stats['processed'] += 1
            batch_count += 1
            
            # Progress update
            progress = (self.stats['processed'] / total_to_scrape) * 100
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
            eta_seconds = (total_to_scrape - self.stats['processed']) / rate if rate > 0 else 0
            eta = timedelta(seconds=int(eta_seconds))
            
            print(f"\n[{self.stats['processed']}/{total_to_scrape}] ({progress:.1f}%) - ETA: {eta}")
            print(f"Processing: {property_doc['property_id']} - {property_doc.get('detailed_url')}")
            
            # Process the property
            self.process_property(property_doc)
            
            # Checkpoint every batch
            if batch_count >= DEFAULT_BATCH_SIZE:
                batch_time = (datetime.now() - batch_start).total_seconds()
                print(f"\n📊 Batch of {batch_count} completed in {batch_time:.1f}s")
                print(f"   Success: {self.stats['success']}, Failed: {self.stats['failed']}, Skipped: {self.stats['skipped']}")
                print(f"   Bot challenges: {self.stats['bot_challenges']}, Cooldowns: {self.stats['cooldowns']}")
                if self.consecutive_challenges > 0:
                    print(f"   ⚠️  Current consecutive challenges: {self.consecutive_challenges}/{BOT_DETECTION_THRESHOLD}")
                batch_count = 0
                batch_start = datetime.now()
            
            # Delay before next request (except for last item)
            if self.stats['processed'] < total_to_scrape and not SHUTDOWN_REQUESTED:
                time.sleep(self.delay)
        
        # Final statistics
        self._print_statistics()
    
    def _print_statistics(self):
        """Print final statistics"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print(f"\n{'='*60}")
        print(f"📊 SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total processed: {self.stats['processed']:,}")
        print(f"✅ Success: {self.stats['success']:,}")
        print(f"❌ Failed: {self.stats['failed']:,}")
        print(f"⏭️  Skipped: {self.stats['skipped']:,}")
        print(f"🤖 Bot challenges: {self.stats['bot_challenges']:,}")
        print(f"❄️  Cooldowns: {self.stats['cooldowns']:,}")
        print(f"⏱️  Total time: {timedelta(seconds=int(elapsed))}")
        print(f"📈 Average rate: {self.stats['processed'] / elapsed:.2f} properties/second" if elapsed > 0 else "")
        
        if self.stats['errors']:
            print(f"\n⚠️  Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                print(f"  - {error['property_id']}: {error['error']}")
        
        print(f"{'='*60}\n")
    
    def resume(self):
        """Resume scraping from where it left off"""
        # Count already scraped
        scraped_count = self.details_coll.count_documents({})
        print(f"📊 Already scraped: {scraped_count:,} properties")
        print(f"📊 Resuming from position {scraped_count + 1}...")
        self.run(skip=0)  # Will automatically skip already scraped
    
    def verify_data(self, sample_size=5):
        """Verify extracted data with samples"""
        print(f"\n🔍 Verifying extracted data (sample size: {sample_size})...")
        
        samples = self.details_coll.find({'extraction_success': True}).limit(sample_size)
        
        for doc in samples:
            data = doc['extracted_data']
            print(f"\n📄 Property: {doc['property_id']}")
            print(f"  - Title: {data.get('headline', 'N/A')}")
            print(f"  - Price: AED {data.get('price', 'N/A'):,}" if data.get('price') else "  - Price: N/A")
            print(f"  - Bedrooms: {data.get('bedrooms', 'N/A')}")
            print(f"  - Location: {data.get('locality', 'N/A')}")
            print(f"  - Amenities: {data.get('total_amenities_count', 0)}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Bayut Property Detail Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all properties
  python bayut_detail_scraper.py
  
  # Scrape with custom delay
  python bayut_detail_scraper.py --delay 3
  
  # Scrape limited number for testing
  python bayut_detail_scraper.py --limit 100
  
  # Save HTML files
  python bayut_detail_scraper.py --save-html --html-dir detail_html
  
  # Resume from interruption
  python bayut_detail_scraper.py --resume
  
  # Verify extracted data
  python bayut_detail_scraper.py --verify
        """
    )
    
    parser.add_argument('--delay', type=float, default=2.0, 
                       help='Delay between requests in seconds (default: 2.0)')
    parser.add_argument('--limit', type=int, 
                       help='Maximum number of properties to scrape')
    parser.add_argument('--skip', type=int, default=0,
                       help='Number of properties to skip')
    parser.add_argument('--save-html', action='store_true',
                       help='Save HTML files locally')
    parser.add_argument('--html-dir', default='detail_html',
                       help='Directory to save HTML files')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017/',
                       help='MongoDB connection URI')
    parser.add_argument('--db', default='bayut_production',
                       help='Database name')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last position')
    parser.add_argument('--verify', action='store_true',
                       help='Verify extracted data with samples')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = BayutDetailScraper(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        delay=args.delay,
        save_html=args.save_html,
        html_dir=args.html_dir
    )
    
    # Run appropriate action
    if args.verify:
        scraper.verify_data()
    elif args.resume:
        scraper.resume()
    else:
        scraper.run(limit=args.limit, skip=args.skip)


if __name__ == "__main__":
    main()