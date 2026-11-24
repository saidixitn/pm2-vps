# PM2 VPS

A centralized process management and automation framework for running backend daemons, telemetry pipelines, Google BigQuery sync jobs, and scheduled notification workers on a Linux VPS.

---

## What It Is

`pm2-vps` is an operational suite that supervises production background jobs on a Linux server. It manages continuous API services, scheduled domain and proxy expiration monitors, daily click aggregation pipelines, and data sync workers connecting MongoDB with Google BigQuery and Google Sheets.

All background tasks are declared in PM2 ecosystem configuration files, enabling automated error recovery, log management, and scheduled cron executions without relying on unmonitored crontabs.

---

## Why It Was Built

Deploying and maintaining multiple automated scripts on a production server without process supervision leads to:

- Silent breakdowns: Standalone cron jobs do not alert operators when a network timeout or database connection fails.
- Resource leaks: Long-running worker processes can accumulate memory without automated restart thresholds.
- Fragmented monitoring: Without centralized orchestration, inspecting logs and verifying task health requires manually hunting down process IDs and scattered files across the system.

This repository resolves these challenges by providing:
1. Declarative Process Lifecycle: Supervised daemons with automatic restart on failure and memory limits.
2. Direct Cloud Integrations: Synchronizes MongoDB metrics into Google BigQuery tables and Google Sheets dashboards.
3. Multi-Channel Alerting: Proactive notifications for expiring assets and daily metric summaries sent via SMTP and Telegram.
4. Clean Environment Separation: Decouples credentials and API keys into structured templates.

---

## How It Works

```
[ PM2 Process Manager ]
        |
        +---> [ API Daemons ]              ---> FastAPI health & configuration endpoints
        |
        +---> [ Analytics & BigQuery ]     ---> Streams click & view logs into BigQuery
        |
        +---> [ Asset Expiration Monitors ] ---> Checks domain & proxy validity
        |
        +---> [ Report Dispatchers ]        ---> Dispatches daily HTML digests & Telegram alerts
        |
        +---> [ Database Backups ]          ---> Nightly mongodump snapshots
```

### 1. Process Supervision
PM2 manages all running scripts as isolated processes. Configuration files declare whether each job runs continuously (e.g. `jobget-api`) or executes on a cron schedule (e.g. `daily-stats` running hourly).

### 2. BigQuery & Analytics Sync
Telemetry workers (`bigquery/hourly_views.py`, `search_console/searchconsole_bigquery.py`) aggregate operational records and stream performance metrics directly into Google BigQuery.

### 3. Expiration Tracking & Alerts
Domain and proxy monitors inspect database records to identify upcoming renewals. When an expiration threshold is reached, automated digests are dispatched to administrators.

### 4. Backup Routines
Automated backup scripts capture MongoDB database snapshots nightly to ensure reliable disaster recovery.

---

## Repository Structure

```
.
|-- .env.example              # Template for all environment variables
|-- credentials.sample.json   # Template for Google service account JSON
|-- ecosystem.config.json     # Primary PM2 multi-process configuration
|-- requirements.txt          # Python runtime dependencies
|-- main.py                   # FastAPI service for status checks & dynamic config
|-- banana.py                 # Telemetry collector & Google Sheets sync
|-- daily_stats.py            # Hourly click aggregation daemon
|-- clicks_service.py         # Link tracking and analytics worker
|-- stats_email.py            # Morning and afternoon email report generator
|-- domain_expires_new.py     # Domain validity monitor
|-- proxy_expires.py          # Proxy validity monitor
|-- proxy_faq.py              # Proxy pool verification & content generation
|-- backup_script.py          # Scheduled MongoDB backup script
|-- run_banana.sh             # Daily telemetry shell runner
|-- run_stats.sh              # Daily email digest shell runner
|-- bigquery/
|   |-- hourly_views.py       # BigQuery telemetry streaming worker
|   `-- bigquery.config.js    # PM2 configuration for BigQuery jobs
|-- search_console/
|   |-- searchconsole_bigquery.py # Search Console metrics importer
|   `-- credentials.sample.json
|-- Clicks Report/
|   |-- clicks_report.py      # Scheduled report dashboard API
|   `-- credentials.sample.json
|-- Proxy/
|   |-- config.py             # Proxy pipeline configuration
|   `-- credentials.sample.json
`-- docs/
    |-- ARCHITECTURE.md       # Detailed system design and component topology
    |-- DAEMONS.md            # Roster of all managed processes and schedules
    `-- SETUP.md              # Installation and deployment walkthrough
```

---

## Quickstart

### 1. Install Requirements

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
npm install -g pm2
```

### 2. Configure Environment

```bash
cp .env.example .env
cp credentials.sample.json credentials.json
```

Add your MongoDB connection strings, mail server credentials, and Telegram bot token to `.env`.

### 3. Launch Daemons

```bash
# Start all processes defined in ecosystem file
pm2 start ecosystem.config.json

# Check running processes
pm2 status

# Inspect live logs
pm2 logs
```

---

## Documentation

- [System Architecture](docs/ARCHITECTURE.md) - Deep dive into architecture and communication patterns.
- [Daemon Catalog](docs/DAEMONS.md) - Process-by-process list of entrypoints, schedules, and log paths.
- [Setup & Deployment](docs/SETUP.md) - Step-by-step instructions for deploying to a fresh server.

---

## License

This project is licensed under the MIT License.
