# EarnHaus Google for Jobs Automation Pipeline (`earnhaus-google-jobs-automation`)

An automated Python pipeline that reads target job counts from Google Sheets, extracts job IDs across life cycle stages (Postings, Updates, Expires, DatePosted) from multiple MongoDB collections, enforces strict mutual exclusion (zero overlap), and populates Google Sheets.

## Features

- **Dynamic Count Configuration**: Reads target counts directly from the `domain details` tab of your Google Sheet.
- **Mutual Exclusion Guarantee**: Sequentially selects `Updates` $\rightarrow$ `Expires` $\rightarrow$ `DatePosted` ensuring no duplicate job IDs across categories.
- **Postings Verification**: Samples candidate jobs, filters against existing active jobs, and verifies today's scraped status.
- **High-Performance MongoDB Handler**: Uses MongoDB aggregation with `allowDiskUse=True` and reliable bulk batching.

## Project Structure

```text
.
├── main.py                 # Pipeline execution & mutual exclusion orchestration
├── mongo_handler.py        # MongoDB queries & aggregation handlers
├── sheets_handler.py       # Google Sheets API interaction (gspread)
├── config.py               # Constants, database names, and survey title exclusions
├── requirements.txt        # Python dependencies
├── credentials.sample.json # Sample Google Service Account key format
└── README.md
```

## Setup & Usage

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Google Credentials**:
   Place your Google Service Account JSON key as `credentials.json` in the root folder. Share your Google Sheet with the `client_email` address in `credentials.json` giving **Editor** access.

3. **Run Pipeline**:
   ```bash
   # Standard daily run (uses existing expires collection)
   python main.py --no-rebuild-expires

   # Full rebuild run (re-indexes expiry candidate jobs)
   python main.py

   # Dry run test (validates logic without modifying Google Sheets)
   python main.py --dry-run
   ```
