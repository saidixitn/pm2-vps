# PM2 Daemon Catalog

A complete listing of daemon processes defined across the repository.

---

## Daemon Roster

| Process | Script / Target | Runtime | Schedule | Function |
| :--- | :--- | :--- | :--- | :--- |
| `jobget-api` | `main.py` | `uvicorn` | Persistent (Port 8000) | REST API for remote health and config changes |
| `clicks-service` | `clicks_service.py` | Python 3 | Hourly / Scheduled | Tracks link click metrics across domains |
| `daily-stats` | `daily_stats.py` | Python 3 | Hourly cron (`0 * * * *`) | Computes real-time click and conversion stats |
| `bigquery-hourly` | `bigquery/hourly_views.py` | Python 3 | Hourly cron | Streams domain click and view logs into BigQuery |
| `searchconsole-bq` | `search_console/searchconsole_bigquery.py` | Python 3 | Daily cron | Pulls Search Console API data into BigQuery tables |
| `proxy-faq` | `proxy_faq.py` | Python 3 | Scheduled batch | Verifies proxy pools and generates localized content |
| `expiry-earnhaus` | `proxy_expires.py` | Python 3 | Daily | Checks proxy expiration dates |
| `expiry-directclients` | `dc_expires.py` | Python 3 | Daily | Checks client domain validity |
| `stats-email-daily` | `run_stats.sh` | Shell / Python | Twice daily | Sends 10 AM and 3 PM summary email digests |
| `banana-report` | `run_banana.sh` | Shell / Python | Daily (11:00 AM) | Collects metrics and pushes updates to Google Sheets |
| `mongo-creds-sync` | `new_creds.py` | Python 3 | Daily (03:30 UTC) | Syncs database credentials across services |
| `mongo-daily-backup` | `backup_script.py` | Python 3 | Nightly (02:00 UTC) | Captures compressed MongoDB collection dumps |

---

## Process Management Commands

```bash
# Start all processes defined in ecosystem file
pm2 start ecosystem.config.json

# Start a specific daemon
pm2 start ecosystem.config.json --only jobget-api

# Check status of all processes
pm2 status

# Stream live process logs
pm2 logs

# Stop or restart processes
pm2 restart all
pm2 stop all
```
