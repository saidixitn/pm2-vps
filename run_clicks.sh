#!/bin/bash

# 1. Get the current hour in 24-hour format (00 to 23)
CURRENT_HOUR=$(date +%H)

# 2. Decide the date based on the hour
if [ "$CURRENT_HOUR" -lt 12 ]; then
    # Morning run (e.g., 10 AM) -> Use yesterday's date
    TARGET_DATE=$(date -d "yesterday" '+%Y-%m-%d')
    echo "Running Morning Clicks Report for: $TARGET_DATE"
else
    # Afternoon run (e.g., 3 PM) -> Use today's date
    TARGET_DATE=$(date '+%Y-%m-%d')
    echo "Running Afternoon Clicks Report for: $TARGET_DATE"
fi

# 3. Trigger the FastAPI endpoint via local cURL
echo "Triggering API..."
curl -s -X POST "http://localhost:8000/jobget/run-clicks" \
     -H "accept: application/json" \
     -H "x-run-password: 8000" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "date=$TARGET_DATE"

echo ""
echo "✅ Request sent to API."
