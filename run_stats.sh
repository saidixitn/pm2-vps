# #!/bin/bash

# # Enforce strict mode and IST timezone for accurate day boundaries
# set -euo pipefail
# export TZ="Asia/Kolkata"

# cd /home/dixit

# # Get current hour (00-23) and minute (00-59)
# CURRENT_HOUR=$(date +%H)
# CURRENT_MINUTE=$(date +%M)

# # Determine the target date using both hour and minute
# if [ "$CURRENT_HOUR" -eq 10 ]; then
#     if [ "$CURRENT_MINUTE" -lt 10 ]; then
#         # Runs at 10:00 AM -> Yesterday
#         TARGET_DATE=$(date -d "yesterday" '+%Y-%m-%d')
#         echo "Running 10:00 AM Morning Report for yesterday: $TARGET_DATE"
#     else
#         # Runs at 10:15 AM -> Today
#         TARGET_DATE=$(date '+%Y-%m-%d')
#         echo "Running 10:15 AM Report for today: $TARGET_DATE"
#     fi
# elif [ "$CURRENT_HOUR" -eq 15 ]; then
#     # Prevent a 3:15 PM execution if PM2 cron triggers it
#     if [ "$CURRENT_MINUTE" -gt 10 ]; then
#         echo "Skipping unintended 3:15 PM run..."
#         exit 0
#     fi
    
#     # Runs at 3:00 PM -> Today
#     TARGET_DATE=$(date '+%Y-%m-%d')
#     echo "Running 3:00 PM Afternoon Report for today: $TARGET_DATE"
# else
#     # Fallback for any manual runs you do yourself
#     TARGET_DATE=$(date '+%Y-%m-%d')
#     echo "Manual run. Defaulting to today: $TARGET_DATE"
# fi

# # Run the python script (using global python3)
# python3 stats_email.py "$TARGET_DATE"
