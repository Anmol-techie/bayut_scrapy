#!/usr/bin/env python3
"""
Bayut Property Detail Scraper - Async Concurrent Version
Ultra-fast concurrent scraping with intelligent bot detection and infinite cooldown cycles
"""

import os
import sys
import time
import json
import signal
import argparse
import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# Import the extractor
from z_bayut_complete_extractor import BayutPropertyExtractor

# Configuration
MAX_CONCURRENT_REQUESTS = 50  # Process 50 properties simultaneously
CONNECTION_LIMIT = 100  # Max connections in pool
REQUEST_TIMEOUT = 30  # seconds
DEFAULT_BATCH_SIZE = 500  # Larger batch for async processing
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

# Anti-bot detection configuration
BOT_DETECTION_THRESHOLD = 5  # Increased threshold for concurrent requests
INITIAL_COOLDOWN = 15 * 60  # 15 minutes initial cooldown
COOLDOWN_MULTIPLIER = 1.5  # Multiply cooldown by 1.5x each time

# Global flags
SHUTDOWN_REQUESTED = False
IN_COOLDOWN = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global SHUTDOWN_REQUESTED
    print(f"\n⚠️  Shutdown requested... Will stop after current batch")
    SHUTDOWN_REQUESTED = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class BayutAsyncDetailScraper:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                 db_name="bayut_production",
                 save_html=False,
                 html_dir="detail_html"):
        """
        Initialize the async detail scraper
        """
        self.save_html = save_html
        self.html_dir = Path(html_dir)
        
        # MongoDB setup (will be initialized async)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.properties_coll = None
        self.details_coll = None
        
        # HTTP session (will be initialized async)
        self.session = None
        self.connector = None
        
        # Thread pool for CPU-bound extraction
        self.executor = ThreadPoolExecutor(max_workers=10)
        
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
    
    async def initialize(self):
        """Initialize async components"""
        # MongoDB async client
        self.client = AsyncIOMotorClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.db_name]
        self.properties_coll = self.db['sublocation_properties']
        self.details_coll = self.db['property_details']
        
        # Create indexes
        await self._create_indexes()
        
        # HTTP session with connection pooling
        self.connector = aiohttp.TCPConnector(
            limit=CONNECTION_LIMIT,
            limit_per_host=30,
            ttl_dns_cache=300
        )
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            headers={
                'User-Agent': DEFAULT_USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            }
        )
    
    async def _create_indexes(self):
        """Create indexes for the collections"""
        try:
            await self.details_coll.create_index([("property_id", 1)], unique=True, background=True)
            await self.details_coll.create_index([("scraped_at", 1)], background=True)
            await self.details_coll.create_index([("extraction_success", 1)], background=True)
            print("✅ Indexes created/verified for property_details collection")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")
    
    async def fetch_property_html(self, url: str, property_id: str) -> Optional[str]:
        """
        Fetch HTML content from a property URL asynchronously
        """
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Save HTML if requested (async)
                    if self.save_html:
                        await self._save_html(html, property_id)
                    
                    return html
                else:
                    print(f"  ❌ HTTP {response.status} for property {property_id}")
                    return None
                    
        except asyncio.TimeoutError:
            print(f"  ⏱️ Timeout for property {property_id}")
            return None
        except Exception as e:
            print(f"  ❌ Request error for property {property_id}: {e}")
            return None
    
    async def _save_html(self, html: str, property_id: str):
        """Save HTML content to file asynchronously"""
        self.html_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.html_dir / f"property_{property_id}.html"
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, file_path.write_text, html, 'utf-8')
    
    def _detect_bot_challenge(self, html: str, extracted_data: Dict[str, Any]) -> bool:
        """
        Detect if the response is a bot challenge/empty response
        """
        # Check for minimal extracted data
        essential_fields = ['price', 'bedrooms', 'headline', 'locality']
        missing_fields = sum(1 for field in essential_fields if not extracted_data.get(field))
        
        if missing_fields >= 3:
            return True
            
        # Check for common anti-bot patterns
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
    
    async def _handle_cooldown(self):
        """
        Handle infinite cooldown periods with 1.5x multiplier
        """
        global IN_COOLDOWN
        IN_COOLDOWN = True
        
        cooldown_minutes = self.current_cooldown // 60
        
        print(f"\n🤖 Bot detection triggered! Starting cooldown #{self.cooldown_count + 1}")
        print(f"⏰ Cooldown period: {cooldown_minutes:.1f} minutes ({self.current_cooldown} seconds)")
        print(f"🔄 Script will automatically resume after cooldown...")
        print(f"📈 Next cooldown will be: {(self.current_cooldown * COOLDOWN_MULTIPLIER) // 60:.1f} minutes")
        
        self.stats['cooldowns'] += 1
        self.cooldown_count += 1
        
        # Sleep for cooldown period
        await asyncio.sleep(self.current_cooldown)
        
        # Increase cooldown for next time (infinite cycle with 1.5x multiplier)
        self.current_cooldown = int(self.current_cooldown * COOLDOWN_MULTIPLIER)
        
        # Reset bot detection counters
        self.consecutive_challenges = 0
        self.last_challenge_time = None
        
        IN_COOLDOWN = False
        print(f"✅ Cooldown complete! Resuming scraping...")
        return True
    
    async def process_property(self, property_doc: Dict[str, Any]) -> bool:
        """
        Process a single property asynchronously
        """
        property_id = property_doc['property_id']
        url = property_doc.get('detailed_url')
        
        if not url:
            return False
        
        # Check if already scraped with successful data extraction
        existing = await self.details_coll.find_one({'property_id': property_id, 'extraction_success': True})
        if existing:
            self.stats['skipped'] += 1
            return True
        
        # Fetch HTML
        html = await self.fetch_property_html(url, property_id)
        if not html:
            self.stats['failed'] += 1
            # Store failure record
            await self.details_coll.update_one(
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
        
        # Extract data (CPU-bound, run in thread pool)
        try:
            loop = asyncio.get_event_loop()
            extracted_data = await loop.run_in_executor(
                self.executor,
                self.extractor.extract_all,
                html,
                f"property_{property_id}.html"
            )
            
            # Check for bot challenge
            is_bot_challenge = self._detect_bot_challenge(html, extracted_data)
            
            if is_bot_challenge:
                self.consecutive_challenges += 1
                self.stats['bot_challenges'] += 1
                self.last_challenge_time = datetime.now(timezone.utc)
                
                print(f"  🤖 Bot challenge detected for {property_id} (consecutive: {self.consecutive_challenges})")
                
                # Store challenge record
                await self.details_coll.update_one(
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
                    await self._handle_cooldown()
                
                return False
            else:
                # Reset challenge counter on successful extraction
                self.consecutive_challenges = 0
                self.last_challenge_time = None
            
            # Store successful extraction
            detail_doc = {
                'property_id': property_id,
                'url': url,
                'scraped_at': datetime.now(timezone.utc),
                'extraction_success': True,
                'extracted_data': extracted_data
            }
            
            await self.details_coll.update_one(
                {'property_id': property_id},
                {'$set': detail_doc},
                upsert=True
            )
            
            # Update main collection
            await self.properties_coll.update_one(
                {'property_id': property_id},
                {'$set': {'detail_scraped': True, 'detail_scraped_at': datetime.now(timezone.utc)}}
            )
            
            self.stats['success'] += 1
            
            # Print key extracted info
            price = extracted_data.get('price') or extracted_data.get('pricing_details', {}).get('price')
            bedrooms = extracted_data.get('bedrooms')
            location = extracted_data.get('locality')
            
            if price:
                print(f"  ✅ {property_id}: {bedrooms}BR, AED {price:,.0f} in {location}")
            else:
                print(f"  ✅ {property_id} scraped")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Error for {property_id}: {e}")
            self.stats['failed'] += 1
            
            await self.details_coll.update_one(
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
    
    async def process_batch(self, properties: List[Dict[str, Any]]):
        """
        Process a batch of properties concurrently
        """
        tasks = [self.process_property(prop) for prop in properties]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                print(f"  ❌ Batch processing error: {result}")
            self.stats['processed'] += 1
            
            # Update progress
            if self.stats['processed'] % 100 == 0:
                elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
                rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
                print(f"\n📊 Progress: {self.stats['processed']}/{self.stats['total']} "
                      f"({self.stats['processed']/self.stats['total']*100:.1f}%) "
                      f"- Rate: {rate:.1f} props/sec")
                print(f"   ✅ Success: {self.stats['success']}, ❌ Failed: {self.stats['failed']}, "
                      f"⏭️ Skipped: {self.stats['skipped']}, 🤖 Challenges: {self.stats['bot_challenges']}")
    
    async def run(self, limit: Optional[int] = None, skip: int = 0):
        """
        Main async scraping loop with concurrent processing
        """
        global SHUTDOWN_REQUESTED, IN_COOLDOWN
        
        await self.initialize()
        
        self.stats['start_time'] = datetime.now()
        
        # Count properties to scrape
        query = {'detail_scraped': {'$ne': True}, 'detailed_url': {'$exists': True, '$ne': None}}
        total_to_scrape = await self.properties_coll.count_documents(query)
        
        if limit:
            total_to_scrape = min(total_to_scrape, limit)
        
        self.stats['total'] = total_to_scrape
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting Bayut Async Detail Scraper (ULTRA-FAST MODE)")
        print(f"{'='*60}")
        print(f"📊 Properties to scrape: {total_to_scrape:,}")
        print(f"⚡ Concurrent requests: {MAX_CONCURRENT_REQUESTS}")
        print(f"🔄 Connection pool size: {CONNECTION_LIMIT}")
        print(f"⏱️  NO DELAYS - Maximum speed!")
        if limit:
            print(f"🎯 Limit: {limit:,} properties")
        if skip:
            print(f"⏭️  Skipping first {skip:,} properties")
        
        # Estimate time (much faster now)
        estimated_minutes = (total_to_scrape / MAX_CONCURRENT_REQUESTS) * 2 / 60  # ~2 sec per batch
        print(f"⏰ Estimated time: {estimated_minutes:.1f} minutes (at max speed)")
        print(f"{'='*60}\n")
        
        # Process in batches
        batch = []
        cursor = self.properties_coll.find(query).skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        async for property_doc in cursor:
            if SHUTDOWN_REQUESTED:
                print("\n🛑 Shutdown requested, processing final batch...")
                if batch:
                    await self.process_batch(batch)
                break
            
            batch.append(property_doc)
            
            # Process batch when it reaches the concurrent limit
            if len(batch) >= MAX_CONCURRENT_REQUESTS:
                await self.process_batch(batch)
                batch = []
                
                # Check if we're in cooldown
                while IN_COOLDOWN:
                    await asyncio.sleep(1)
        
        # Process remaining batch
        if batch and not SHUTDOWN_REQUESTED:
            await self.process_batch(batch)
        
        # Cleanup
        await self.cleanup()
        
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
            for error in self.stats['errors'][:5]:
                print(f"  - {error['property_id']}: {error['error']}")
        
        print(f"{'='*60}\n")
    
    async def cleanup(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()
        if self.executor:
            self.executor.shutdown(wait=False)


async def main_async():
    """Main async entry point"""
    parser = argparse.ArgumentParser(
        description="Bayut Property Detail Scraper - Async Ultra-Fast Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all properties at maximum speed
  python bayut_detail_scraper_async.py
  
  # Scrape limited number for testing
  python bayut_detail_scraper_async.py --limit 1000
  
  # Save HTML files
  python bayut_detail_scraper_async.py --save-html --html-dir detail_html
  
  # Custom MongoDB URI
  python bayut_detail_scraper_async.py --mongo-uri mongodb://localhost:27017/ --db bayut_production
        """
    )
    
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
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = BayutAsyncDetailScraper(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        save_html=args.save_html,
        html_dir=args.html_dir
    )
    
    # Run scraper
    await scraper.run(limit=args.limit, skip=args.skip)


def main():
    """Main entry point"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()