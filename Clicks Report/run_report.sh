#!/bin/bash
# run_report.sh — decides the target date by time (IST) and calls the
# Clicks Report API.
#
# Schedule (pm2 cron "0,15 10,15 * * *"):
#   10:00 AM -> yesterday's report
#   10:15 AM -> today's clicks (so far)
#   03:00 PM -> today's clicks (so far)
#   (10:15 at hour 15, i.e. 3:15 PM, is explicitly skipped)
#
# Usage:
#   ./run_report.sh                  # manual run -> today's date
#   ./run_report.sh 2026-09-05       # manual run -> explicit date
#   ./run_report.sh scheduled        # cron/pm2 mode -> date decided by clock,
#                                    # skips off-schedule launches

set -euo pipefail
export TZ="Asia/Kolkata"

cd "/home/dixit/Clicks Report"

API_BASE="${CLICKS_API_URL:-http://localhost:5000}"
# read API_KEY from .env (same file the Flask app uses)
API_KEY="$(grep -E '^API_KEY=' .env 2>/dev/null | cut -d= -f2- | tr -d '\r' || true)"

CURRENT_HOUR=$(date +%H)
CURRENT_MINUTE=$(date +%M)
MODE="${1:-manual}"

if [ "$MODE" = "scheduled" ]; then
    if [ "$CURRENT_HOUR" -eq 10 ]; then
        if [ "$CURRENT_MINUTE" -lt 10 ]; then
            # 10:00 launch -> yesterday's full report
            TARGET_DATE=$(date -d "yesterday" '+%Y-%m-%d')
            echo "Running 10:00 AM Morning Report for yesterday: $TARGET_DATE"
        elif [ "$CURRENT_MINUTE" -lt 30 ]; then
            # 10:15 launch -> today's clicks so far
            TARGET_DATE=$(date '+%Y-%m-%d')
            echo "Running 10:15 AM Report for today: $TARGET_DATE"
        else
            # a pm2 restart at e.g. 10:47 must not send a duplicate
            echo "Off-window 10:$CURRENT_MINUTE start — skipping."
            exit 0
        fi
    elif [ "$CURRENT_HOUR" -eq 15 ]; then
        if [ "$CURRENT_MINUTE" -gt 10 ]; then
            # covers the 3:15 PM tick from the shared cron — skip it
            echo "Skipping unintended 3:$CURRENT_MINUTE PM run..."
            exit 0
        fi
        TARGET_DATE=$(date '+%Y-%m-%d')
        echo "Running 3:00 PM Afternoon Report for today: $TARGET_DATE"
    else
        echo "Off-schedule start at $CURRENT_HOUR:$CURRENT_MINUTE — skipping."
        exit 0
    fi
elif [[ "$MODE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    TARGET_DATE="$MODE"
    echo "Manual run for explicit date: $TARGET_DATE"
else
    TARGET_DATE=$(date '+%Y-%m-%d')
    echo "Manual run. Defaulting to today: $TARGET_DATE"
fi

URL="$API_BASE/run?date=$TARGET_DATE&mode=async"
if [ -n "$API_KEY" ]; then
    URL="$URL&key=$API_KEY"
fi

echo "Calling: $API_BASE/run?date=$TARGET_DATE&mode=async"
RESPONSE=$(curl -sS --max-time 30 "$URL")
echo "API response: $RESPONSE"

echo "$RESPONSE" | grep -q '"job_id"' || { echo "API did not queue a job!"; exit 1; }