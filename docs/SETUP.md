# Setup & Deployment Guide

This guide walks through setting up the PM2 VPS automation infrastructure on Ubuntu/Debian or a local development environment.

---

## 1. Prerequisites

- Python 3.10+
- Node.js 18+ with `pm2` installed globally:
  ```bash
  npm install -g pm2
  ```
- MongoDB database (local or remote)
- Google Cloud Service Account (if using BigQuery or Google Sheets integration)

---

## 2. Environment Setup

### Clone Repository & Create Virtual Environment

```bash
git clone https://github.com/saidixitn/pm2-vps.git
cd pm2-vps

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configure Environment Variables

```bash
cp .env.example .env
```

Configure the connection variables in `.env`:
- `MONGO_URI`: Primary MongoDB connection string
- `SEMRUSH_MONGO_URI`: MongoDB connection string for SEMrush audit collections
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account JSON file
- `SMTP_USER` and `SMTP_PASS`: Credentials for automated email delivery
- `TELEGRAM_BOT_TOKEN`: Token for real-time Telegram alerts

### Service Account Credentials Setup

1. Copy `credentials.sample.json` to `credentials.json`:
   ```bash
   cp credentials.sample.json credentials.json
   ```
2. Replace placeholder fields with your GCP project ID and private key.
3. If using Google Sheets or BigQuery, ensure the service account email has the required IAM roles (`BigQuery Data Editor`, `BigQuery Job User`) or Sheet Editor permissions.

---

## 3. Running Services

```bash
# Launch entire fleet
pm2 start ecosystem.config.json

# Check dashboard
pm2 status

# Save configuration across server reboots
pm2 save
pm2 startup
```
