# Daily Bayut Update Cron System

Automated daily scraping system for new Bayut properties with detailed data extraction.

## 🏗️ System Overview

The system consists of two main components that run daily:

1. **Incremental Scraper** (`bayut_incremental_scraper.py`)
   - Scrapes UAE-wide listings: `https://www.bayut.com/for-sale/property/uae/page-{}/`
   - Stops when 2 consecutive existing properties are found
   - Efficiently finds only new properties

2. **Detail Scraper** (`bayut_detail_scraper.py`)
   - Runs after incremental scraper
   - Extracts detailed data for properties with `detail_scraped = False`
   - Uses the comprehensive extraction from `z_bayut_complete_extractor.py`

## 📁 File Structure

```
extraction/
├── bayut_incremental_scraper.py    # Daily new property detection
├── bayut_detail_scraper.py         # Existing detailed scraper
├── daily_bayut_update.sh           # Main cron wrapper script
├── setup_cron.sh                   # Cron installation script
├── cron_logs/                      # Daily execution logs
│   ├── bayut_daily_YYYYMMDD_HHMMSS.log
│   └── cron_output.log
└── locks/                          # Process lock files
    └── bayut_daily_update.lock
```

## ⚙️ Setup Instructions

### 1. Install the Cron Job

```bash
# Navigate to the extraction directory
cd /Users/apple/Desktop/mcp/bayut_scrapy/bayut_spider/extraction

# Run the setup script
./setup_cron.sh
```

This will:
- ✅ Add daily cron job at 6:30 PM UAE time
- ✅ Create necessary directories
- ✅ Set proper file permissions
- ✅ Show current cron schedule

### 2. Manual Testing

Test the system before the first automated run:

```bash
# Test incremental scraper only
python3 bayut_incremental_scraper.py --max-pages 5

# Test complete daily update
./daily_bayut_update.sh
```

## 📊 How It Works

### Daily Execution Flow

```
6:30 PM UAE Time
       ↓
1. 📋 Incremental Scraper
   - Starts from page 1 (newest first)
   - Checks each property against database
   - Stops when 2 consecutive existing properties found
   - Inserts new properties with detail_scraped = False
       ↓
2. 🔍 Detail Scraper
   - Counts properties needing detail scraping
   - Runs detail extraction (max 100 properties/day)
   - Updates detail_scraped = True when complete
       ↓
3. 📊 Final Report
   - Logs statistics and performance
   - Cleans up old log files (30+ days)
```

### Smart Stopping Logic

The incremental scraper uses intelligent stopping:

```python
consecutive_existing = 0
for property in page_properties:
    if property_exists_in_db(property_id):
        consecutive_existing += 1
        if consecutive_existing >= 2:
            print("Found 2 consecutive existing properties - stopping")
            break
    else:
        consecutive_existing = 0  # Reset counter
        # Process new property
```

## 📈 Performance Characteristics

| Component | Speed | Efficiency |
|-----------|-------|------------|
| **Incremental Scraper** | ~12 properties/sec | High (batch processing) |
| **Detail Scraper** | ~0.5 properties/sec | Deep (50+ fields extracted) |

### Daily Time Estimates

- **New properties found**: 10-50 typical
- **Incremental scraping**: 5-25 minutes
- **Detail scraping**: 20-100 minutes (50x slower, but comprehensive)
- **Total daily runtime**: 25-125 minutes

## 🔍 Monitoring & Logs

### Log Files

All execution logs are stored in `cron_logs/`:

```bash
# View latest log
ls -lt cron_logs/bayut_daily_*.log | head -1

# Monitor real-time
tail -f cron_logs/bayut_daily_$(date +%Y%m%d)_*.log

# Check cron output
tail -f cron_logs/cron_output.log
```

### Database Monitoring

```python
# Check daily statistics
from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient('mongodb://localhost:27017/')
db = client['bayut_production']

# Properties added today
today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
new_today = db.sublocation_properties.count_documents({
    'created_at': {'$gte': today}
})

# Properties detailed today
detailed_today = db.property_details.count_documents({
    'scraped_at': {'$gte': today}
})

print(f"New properties today: {new_today}")
print(f"Detailed today: {detailed_today}")
```

## 🛠️ Maintenance

### Regular Tasks

1. **Monitor disk space** (logs and HTML files can grow)
2. **Check for rate limiting** (look for HTTP 429 errors in logs)
3. **Verify MongoDB health** (ensure it's running before 6:30 PM)
4. **Review performance** (check if detail scraper keeps up)

### Configuration Updates

Edit the cron schedule:
```bash
crontab -e
```

Modify delays or limits in the scripts:
- `bayut_incremental_scraper.py --delay 3.0` (slower requests)
- `bayut_detail_scraper.py --limit 50` (fewer details per day)

### Troubleshooting

**Common Issues:**

1. **MongoDB Connection Failed**
   ```bash
   # Check MongoDB status
   brew services list | grep mongodb
   # Start if needed
   brew services start mongodb-community
   ```

2. **Cron Job Not Running**
   ```bash
   # Check cron service
   sudo launchctl list | grep cron
   # View system logs
   tail -f /var/log/system.log | grep cron
   ```

3. **Rate Limiting**
   - Increase delays in scripts
   - Check for IP blocking
   - Consider using proxies

4. **Lock File Issues**
   ```bash
   # Remove stale lock if needed
   rm -f locks/bayut_daily_update.lock
   ```

## 🚨 Alerts & Notifications

### Email Notifications (Optional)

Edit `daily_bayut_update.sh` to add email alerts:

```bash
# Set your email
NOTIFY_EMAIL="your-email@example.com"
```

Requires `mail` command to be configured on macOS.

### Monitoring Scripts

Create custom monitoring:

```bash
# Check if cron ran today
#!/bin/bash
LOG_DIR="/Users/apple/Desktop/mcp/bayut_scrapy/bayut_spider/extraction/cron_logs"
TODAY=$(date +%Y%m%d)
if ls ${LOG_DIR}/bayut_daily_${TODAY}_*.log 1> /dev/null 2>&1; then
    echo "✅ Cron job ran today"
else
    echo "❌ No cron job execution found for today"
fi
```

## 📋 Cron Schedule Reference

Current schedule: `30 18 * * *` (6:30 PM daily)

Other schedule examples:
- `0 19 * * *` = 7:00 PM daily
- `30 18 * * 1-5` = 6:30 PM weekdays only
- `0 6,18 * * *` = 6 AM and 6 PM daily

## 🔧 Advanced Configuration

### Environment Variables

You can set these in the wrapper script:

```bash
export MONGO_URI="mongodb://localhost:27017/"
export BAYUT_UA="Custom-User-Agent-String"
export COOKIE_STR="session-cookies-if-needed"
```

### Performance Tuning

For high-volume scenarios:
- Increase `--max-pages` in incremental scraper
- Adjust `DAILY_LIMIT` in wrapper script
- Implement parallel processing (if needed)

## ✅ Success Indicators

The system is working correctly when:
- ✅ Daily logs are created in `cron_logs/`
- ✅ New properties appear in database with recent timestamps
- ✅ Detail scraper processes properties without errors
- ✅ No stale lock files remain
- ✅ Reasonable execution times (under 2 hours typical)

---

**Need help?** Check the logs first, then review this README for troubleshooting steps.