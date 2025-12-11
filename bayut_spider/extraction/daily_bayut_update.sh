#!/bin/bash

# Daily Bayut Update Script
# Runs incremental scraper followed by detail scraper for new properties
# Designed to run as a cron job at 6:30 PM UAE time

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="${SCRIPT_DIR}/cron_logs"
LOCKS_DIR="${SCRIPT_DIR}/locks"
LOG_FILE="${LOGS_DIR}/bayut_daily_$(date +'%Y%m%d_%H%M%S').log"
LOCK_FILE="${LOCKS_DIR}/bayut_daily_update.lock"
PYTHON_PATH="/opt/anaconda3/bin/python3"  # Adjust path as needed
DATE=$(date +'%Y-%m-%d %H:%M:%S')

# Email/notification settings (optional)
NOTIFY_EMAIL=""  # Set email if you want notifications

# Create directories
mkdir -p "${LOGS_DIR}" "${LOCKS_DIR}"

# Logging function
log() {
    echo "[$DATE] $1" | tee -a "${LOG_FILE}"
}

# Error handler
error_exit() {
    log "ERROR: $1"
    cleanup
    exit 1
}

# Cleanup function
cleanup() {
    if [ -f "${LOCK_FILE}" ]; then
        rm -f "${LOCK_FILE}"
        log "Removed lock file"
    fi
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Check if another instance is running
if [ -f "${LOCK_FILE}" ]; then
    LOCK_PID=$(cat "${LOCK_FILE}" 2>/dev/null || echo "")
    if [ -n "${LOCK_PID}" ] && kill -0 "${LOCK_PID}" 2>/dev/null; then
        error_exit "Another instance is already running (PID: ${LOCK_PID})"
    else
        log "Stale lock file found, removing it"
        rm -f "${LOCK_FILE}"
    fi
fi

# Create lock file
echo $$ > "${LOCK_FILE}"

# Start logging
log "=========================================="
log "🚀 Starting Daily Bayut Update"
log "=========================================="
log "Script directory: ${SCRIPT_DIR}"
log "Log file: ${LOG_FILE}"
log "Lock file: ${LOCK_FILE}"
log "Python path: ${PYTHON_PATH}"

# Check dependencies
log "Checking dependencies..."
if [ ! -f "${PYTHON_PATH}" ]; then
    error_exit "Python not found at ${PYTHON_PATH}"
fi

if [ ! -f "${SCRIPT_DIR}/bayut_incremental_scraper.py" ]; then
    error_exit "Incremental scraper not found"
fi

if [ ! -f "${SCRIPT_DIR}/bayut_detail_scraper.py" ]; then
    error_exit "Detail scraper not found"
fi

# Check MongoDB connection
log "Testing MongoDB connection..."
"${PYTHON_PATH}" -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27017/').admin.command('ping')" 2>>"${LOG_FILE}" || error_exit "MongoDB connection failed"

log "✅ All dependencies check passed"

# Step 1: Run incremental scraper for new properties
log ""
log "📋 STEP 1: Running incremental scraper for new properties"
log "=========================================="

INCREMENTAL_START=$(date +%s)

# Run incremental scraper and capture output
"${PYTHON_PATH}" "${SCRIPT_DIR}/bayut_incremental_scraper.py" \
    --delay 2.0 \
    --max-pages 100 \
    2>&1 | tee -a "${LOG_FILE}"

INCREMENTAL_EXIT_CODE=$?
INCREMENTAL_END=$(date +%s)
INCREMENTAL_DURATION=$((INCREMENTAL_END - INCREMENTAL_START))

if [ ${INCREMENTAL_EXIT_CODE} -eq 0 ]; then
    log "✅ Incremental scraper completed successfully in ${INCREMENTAL_DURATION}s"
else
    error_exit "❌ Incremental scraper failed with exit code ${INCREMENTAL_EXIT_CODE}"
fi

# Step 2: Count new properties that need detail scraping
log ""
log "📊 Checking for properties that need detail scraping..."

NEW_PROPERTIES_COUNT=$("${PYTHON_PATH}" -c "
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['bayut_production']
coll = db['sublocation_properties']
count = coll.count_documents({'detail_scraped': False})
print(count)
" 2>>"${LOG_FILE}")

log "Properties needing detail scraping: ${NEW_PROPERTIES_COUNT}"

# Step 3: Run detail scraper on new properties
if [ "${NEW_PROPERTIES_COUNT}" -gt 0 ]; then
    log ""
    log "🔍 STEP 2: Running detail scraper for ${NEW_PROPERTIES_COUNT} properties"
    log "=========================================="
    
    DETAIL_START=$(date +%s)
    
    # Calculate estimated time (2 seconds per property + processing)
    ESTIMATED_MINUTES=$(((NEW_PROPERTIES_COUNT * 3) / 60))  # 3 seconds per property (2s delay + 1s processing)
    log "⏰ Estimated time: ${ESTIMATED_MINUTES} minutes"
    
    # Run detail scraper with a reasonable limit for daily updates
    # Limit to 100 properties max per day to keep runtime manageable
    DAILY_LIMIT=100
    if [ "${NEW_PROPERTIES_COUNT}" -gt "${DAILY_LIMIT}" ]; then
        log "⚠️  Limiting to ${DAILY_LIMIT} properties for daily update"
        SCRAPE_LIMIT="--limit ${DAILY_LIMIT}"
    else
        SCRAPE_LIMIT=""
    fi
    
    "${PYTHON_PATH}" "${SCRIPT_DIR}/bayut_detail_scraper.py" \
        --delay 2.0 \
        ${SCRAPE_LIMIT} \
        2>&1 | tee -a "${LOG_FILE}"
    
    DETAIL_EXIT_CODE=$?
    DETAIL_END=$(date +%s)
    DETAIL_DURATION=$((DETAIL_END - DETAIL_START))
    
    if [ ${DETAIL_EXIT_CODE} -eq 0 ]; then
        log "✅ Detail scraper completed successfully in ${DETAIL_DURATION}s"
    else
        log "⚠️  Detail scraper exited with code ${DETAIL_EXIT_CODE}, but continuing..."
    fi
else
    log "ℹ️  No properties need detail scraping, skipping step 2"
    DETAIL_DURATION=0
fi

# Final statistics
log ""
log "📊 FINAL SUMMARY"
log "=========================================="

# Get updated counts
FINAL_STATS=$("${PYTHON_PATH}" -c "
from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient('mongodb://localhost:27017/')
db = client['bayut_production']
main_coll = db['sublocation_properties']
detail_coll = db['property_details']

# Count properties created today
today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = today + timedelta(days=1)

new_today = main_coll.count_documents({
    'created_at': {'\$gte': today, '\$lt': tomorrow}
})

detailed_today = detail_coll.count_documents({
    'scraped_at': {'\$gte': today, '\$lt': tomorrow}
})

total_properties = main_coll.count_documents({})
total_detailed = detail_coll.count_documents({'extraction_success': True})
pending_detail = main_coll.count_documents({'detail_scraped': False})

print(f'{new_today},{detailed_today},{total_properties},{total_detailed},{pending_detail}')
" 2>>"${LOG_FILE}")

IFS=',' read -r NEW_TODAY DETAILED_TODAY TOTAL_PROPERTIES TOTAL_DETAILED PENDING_DETAIL <<< "${FINAL_STATS}"

TOTAL_DURATION=$(($(date +%s) - $(date +%s --date="$(head -1 "${LOG_FILE}" | cut -d']' -f1 | tr -d '[')")))

log "🆕 New properties today: ${NEW_TODAY}"
log "🔍 Detailed today: ${DETAILED_TODAY}"
log "📊 Total properties: ${TOTAL_PROPERTIES}"
log "📋 Total detailed: ${TOTAL_DETAILED}"
log "⏳ Pending detail scraping: ${PENDING_DETAIL}"
log "⏱️  Total runtime: ${TOTAL_DURATION}s"

# Optional email notification
if [ -n "${NOTIFY_EMAIL}" ] && command -v mail >/dev/null 2>&1; then
    echo "Daily Bayut Update Complete
    
New properties: ${NEW_TODAY}
Detailed today: ${DETAILED_TODAY}
Total properties: ${TOTAL_PROPERTIES}
Runtime: ${TOTAL_DURATION}s

Log file: ${LOG_FILE}" | mail -s "Bayut Daily Update - $(date +'%Y-%m-%d')" "${NOTIFY_EMAIL}"
fi

log "=========================================="
log "✅ Daily Bayut update completed successfully"
log "=========================================="

# Cleanup old log files (keep last 30 days)
find "${LOGS_DIR}" -name "bayut_daily_*.log" -type f -mtime +30 -delete 2>/dev/null || true

exit 0