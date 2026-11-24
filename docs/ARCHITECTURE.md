# System Architecture: PM2 VPS Orchestration

## Overview

The `pm2-vps` repository manages the production process orchestration for telemetry, automated data pipelines, BigQuery sync, proxy validation, and daily notification daemons.

The system uses:
1. PM2 Process Manager: Manages daemon lifecycles, memory thresholds, process clustering, and scheduled cron jobs.
2. Python Daemons: Individual worker scripts handling data scraping, validation, database operations, and Google Sheets integration.
3. Storage & Analytics: MongoDB clusters, Google BigQuery, and Google Drive.
4. Notification Tier: Multi-channel delivery via SMTP (HTML email reports) and Telegram bots.

---

## High-Level Topology

```
+-------------------------------------------------------------+
|                      Ubuntu VPS Host                        |
|                                                             |
|   +-----------------------------------------------------+   |
|   |                    PM2 Daemon Fleet                 |   |
|   |                                                     |   |
|   |  - API Daemons: jobget-api, to_post_api             |   |
|   |  - Expiration Checkers: domain, proxy, client assets |   |
|   |  - Analytics Sync: BigQuery views, Google Sheets     |   |
|   |  - Notification Daemons: Stats emails, Telegram bots |   |
|   |  - Scrapers & Workers: FAQ generation, Serper Maps   |   |
|   +-----------------------------------------------------+   |
|                              |                              |
|                              v                              |
|   +-----------------------------------------------------+   |
|   |                  Python Runtimes                    |   |
|   |                                                     |   |
|   |  - pymongo, google-cloud-bigquery, gspread          |   |
|   |  - requests, pandas, telebot                        |   |
|   +-----------------------------------------------------+   |
+-------------------------------------------------------------+
                               |
        +----------------------+----------------------+
        |                      |                      |
        v                      v                      v
+---------------+      +---------------+      +---------------+
|    MongoDB    |      | Google Cloud  |      | Alerts Engine |
|   Clusters    |      | (BQ & Sheets) |      | (SMTP/Telegram)|
+---------------+      +---------------+      +---------------+
```

---

## Subsystem Details

### 1. Process Supervision (PM2)
- Declarative process configuration via `ecosystem.config.json` and modular config files (`daily_stats.config.js`, `clicks_service.config.js`, `proxy_faq.config.js`, `bigquery.config.js`).
- Automatic process resurrection after failure.
- Structured log paths separating stdout and stderr into dedicated logs.

### 2. BigQuery & Google Analytics Telemetry
- `bigquery/hourly_views.py`: Aggregates hourly page views and interaction metrics from distributed domain collections and streams them into BigQuery tables.
- `search_console/searchconsole_bigquery.py`: Syncs Google Search Console performance metrics (clicks, impressions, queries, positions) into BigQuery.

### 3. Expiration & Health Monitoring
- `domain_expires_new.py`, `proxy_expires.py`, `newearnhaus_expires.py`, `dc_expires.py`: Scan database records for upcoming domain and proxy renewal dates and dispatch alerts before expiration.

### 4. Daily Reports & Clicks Delivery
- `Clicks Report/clicks_report.py` and `clicks_service.py`: Generates scheduled performance digests across marketing domains, updating Google Sheets and delivering formatted emails to team leads.
