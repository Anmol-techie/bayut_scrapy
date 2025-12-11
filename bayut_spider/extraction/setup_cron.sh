#!/bin/bash

# Cron Job Setup Script for Daily Bayut Updates
# This script sets up the cron job to run daily at 6:30 PM UAE time

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRON_SCRIPT="${SCRIPT_DIR}/daily_bayut_update.sh"

echo "🕕 Setting up Bayut Daily Update Cron Job"
echo "========================================"

# Check if the main script exists
if [ ! -f "${CRON_SCRIPT}" ]; then
    echo "❌ Error: daily_bayut_update.sh not found at ${CRON_SCRIPT}"
    exit 1
fi

# Make sure the script is executable
chmod +x "${CRON_SCRIPT}"

# Check system timezone
echo "📍 System timezone check:"
date
ls -la /etc/localtime

echo ""
echo "📋 Current cron jobs:"
crontab -l 2>/dev/null || echo "No existing cron jobs"

echo ""
echo "🔧 Adding new cron job..."

# Create the cron entry
# 30 18 * * * = Daily at 6:30 PM
CRON_ENTRY="30 18 * * * ${CRON_SCRIPT} >> ${SCRIPT_DIR}/cron_logs/cron_output.log 2>&1"

# Add to crontab
(crontab -l 2>/dev/null; echo "${CRON_ENTRY}") | sort | uniq | crontab -

echo "✅ Cron job added successfully!"
echo ""
echo "📅 Schedule: Daily at 6:30 PM UAE time"
echo "📝 Script: ${CRON_SCRIPT}"
echo "📄 Logs: ${SCRIPT_DIR}/cron_logs/"

echo ""
echo "📋 Updated cron jobs:"
crontab -l

echo ""
echo "🔍 To monitor the cron job:"
echo "  - Check logs: ls -la ${SCRIPT_DIR}/cron_logs/"
echo "  - View latest log: tail -f ${SCRIPT_DIR}/cron_logs/bayut_daily_*.log"
echo "  - Remove cron job: crontab -e (then delete the line)"
echo "  - Test manually: ${CRON_SCRIPT}"

echo ""
echo "⚠️  Important Notes:"
echo "  - Ensure MongoDB is running before 6:30 PM daily"
echo "  - Check disk space regularly (logs and HTML files)"
echo "  - Monitor for rate limiting or blocking"
echo "  - First run will happen today at 6:30 PM"

echo ""
echo "✅ Cron setup complete!"