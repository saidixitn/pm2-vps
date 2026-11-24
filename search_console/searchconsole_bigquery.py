import os
import json
import time
import hashlib
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery

# ====================================================================
# LOGGING SETUP
# ====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

load_dotenv()

# ====================================================================
# CONFIGURATION
# ====================================================================
MONGO_CREDENTIALS_URI = os.getenv("MONGO_URI")

# Tracking DB
RUN_HISTORY_DB = "GSC"
RUN_HISTORY_COLL = "RunHistory"

CREDENTIALS_FILE = "credentials.json"
BQ_KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), 'keywords-to-titles-5765a727ea8f.json')

TARGET_COLLECTION = "aj_jp_service_accounts"
BQ_PROJECT_ID = 'keywords-to-titles'
BQ_DATASET_ID = 'useranalytics_data'
BQ_TABLE = 'gsc_output'
BQ_ACTIVE_DOMAINS_TABLE = 'ActiveDomains'

MAX_WORKERS = 10

# ====================================================================
# EMAIL CONFIGURATION (Loaded dynamically from .env)
# ====================================================================
SMTP_SENDER = os.getenv("SMTP_SENDER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Safely parse the comma-separated emails from .env into a Python list
smtp_receivers_env = os.getenv("SMTP_RECEIVERS", "")
SMTP_RECEIVERS = [email.strip() for email in smtp_receivers_env.split(",") if email.strip()]

BQ_SCHEMA = [
    bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Date", "TIMESTAMP"),
    bigquery.SchemaField("Query", "STRING"),
    bigquery.SchemaField("Page", "STRING"),
    bigquery.SchemaField("Clicks", "INTEGER"),
    bigquery.SchemaField("Impressions", "INTEGER"),
    bigquery.SchemaField("CTR", "FLOAT"),
    bigquery.SchemaField("Position", "FLOAT"),
    bigquery.SchemaField("Project_ID", "STRING"),
    bigquery.SchemaField("Site_URL", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
]

try:
    bq_creds = service_account.Credentials.from_service_account_file(BQ_KEY_FILE_PATH)
    bq_client = bigquery.Client(credentials=bq_creds, project=BQ_PROJECT_ID)
except Exception as e:
    logging.critical(f"❌ Failed to initialize BigQuery client. Check key file path: {e}")
    exit(1)


# ====================================================================
# EMAIL SENDER
# ====================================================================
def send_email(html_body: str):
    if not SMTP_SENDER or not SMTP_PASSWORD or not SMTP_RECEIVERS:
        logging.warning("⚠️ Email configuration missing in .env. Skipping email report.")
        return

    msg = MIMEMultipart("alternative")
    msg["From"] = f"GSC Sync Report <{SMTP_SENDER}>"
    msg["To"] = ", ".join(SMTP_RECEIVERS)
    msg["Subject"] = "GSC to BigQuery: Daily Sync Report"

    msg.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(SMTP_SENDER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        logging.info(f"📧 Summary Email Sent Successfully to {len(SMTP_RECEIVERS)} recipients!")
    except Exception as e:
        logging.error(f"❌ Email sending failed: {e}")


# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def get_active_domains():
    logging.info("📋 Fetching active domains from BigQuery...")
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_ACTIVE_DOMAINS_TABLE}`
        WHERE LOWER(CAST(activedomain AS STRING)) = 'true'
    """
    domains = []
    try:
        query_job = bq_client.query(query)
        for row in query_job:
            domains.append(dict(row.items()))
        logging.info(f"✅ Found {len(domains)} active domains in BigQuery.")
    except Exception as e:
        logging.error(f"❌ Error fetching active domains from BigQuery: {e}")
    return domains


def get_mongo_uri_for_domain(client, db_name):
    try:
        db = client.get_database("mongo_creds")
        creds_collection = db.get_collection("creds")
        search_term = db_name.replace("_prod", "")
        result = creds_collection.find_one(
            {"domain": {"$regex": f".*{search_term}.*", "$options": "i"}},
            {"mongo_uri": 1, "_id": 0}
        )
        return result["mongo_uri"] if result and "mongo_uri" in result else None
    except Exception:
        return None


def fetch_service_accounts(domain_doc, primary_client):
    accounts = []
    seen_projects = set()
    json_contents = domain_doc.get("jsonContents")

    if json_contents:
        try:
            if isinstance(json_contents, str):
                sa_json = json.loads(json_contents)
            else:
                sa_json = json_contents

            if "project_id" in sa_json and "private_key" in sa_json:
                pid = sa_json["project_id"]
                seen_projects.add(pid)
                accounts.append({"project_id": pid, "credentials": sa_json})
                return accounts
        except Exception:
            pass 

    db_name = domain_doc.get("db")
    if not db_name:
        return accounts

    target_uri = get_mongo_uri_for_domain(primary_client, db_name)
    if not target_uri:
        target_uri = MONGO_CREDENTIALS_URI

    try:
        target_client = MongoClient(target_uri, serverSelectionTimeoutMS=5000)
        db = target_client.get_database(db_name)
        collection = db.get_collection(TARGET_COLLECTION)

        for doc in collection.find({}):
            sa_json = None
            if "jsonContents" in doc and doc["jsonContents"]:
                try:
                    sa_json = json.loads(doc["jsonContents"]) if isinstance(doc["jsonContents"], str) else doc["jsonContents"]
                except:
                    continue
            else:
                sa_json = {k: v for k, v in doc.items() if k != "_id"}

            if sa_json and "project_id" in sa_json and "private_key" in sa_json:
                pid = sa_json["project_id"]
                if pid not in seen_projects:
                    seen_projects.add(pid)
                    accounts.append({"project_id": pid, "credentials": sa_json})

        target_client.close()
    except OperationFailure as e:
        if e.code == 13:
            logging.debug(f"[{domain_doc.get('domain')}] Access denied to DB {db_name}. Skipping fallback auth fetch.")
        else:
            logging.debug(f"[{domain_doc.get('domain')}] DB Operation Failure: {e}")
    except Exception as e:
        logging.debug(f"[{domain_doc.get('domain')}] Failed downloading API profiles: {e}")

    return accounts


def get_missing_date_ranges(domain, start_date_str):
    if not start_date_str:
        start_date_str = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    else:
        start_date_str = str(start_date_str).split(' ')[0]

    start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_dt = datetime.now() - timedelta(days=2)

    if start_dt > end_dt:
        start_dt = end_dt

    existing_dates = set()
    query = f"""
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d', Date) as date_str 
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE}` 
        WHERE domain = @domain 
        GROUP BY date_str
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("domain", "STRING", domain)]
    )

    try:
        query_job = bq_client.query(query, job_config=job_config)
        for row in query_job:
            existing_dates.add(row.date_str)
    except Exception as e:
        logging.warning(f"[{domain}] ⚠️ Could not fetch existing dates from BQ: {e}")

    missing_dates = []
    curr = start_dt
    while curr <= end_dt:
        d_str = curr.strftime('%Y-%m-%d')
        if d_str not in existing_dates or (end_dt - curr).days <= 2:
            missing_dates.append(curr)
        curr += timedelta(days=1)

    ranges = []
    if not missing_dates:
        return ranges

    range_start = missing_dates[0]
    range_end = missing_dates[0]

    for d in missing_dates[1:]:
        if (d - range_end).days == 1:
            range_end = d
        else:
            ranges.append((range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))
            range_start = d
            range_end = d
    ranges.append((range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))

    return ranges


def delete_overlapping_data(domain, date_ranges):
    """Deletes existing BQ data for the exact date ranges being refreshed to strictly prevent BQ duplicates."""
    if not date_ranges:
        return

    for range_start, range_end in date_ranges:
        query = f"""
            DELETE FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE}`
            WHERE domain = @domain 
            AND DATE(Date) >= DATE(@start_date) 
            AND DATE(Date) <= DATE(@end_date)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("domain", "STRING", domain),
                bigquery.ScalarQueryParameter("start_date", "STRING", range_start),
                bigquery.ScalarQueryParameter("end_date", "STRING", range_end),
            ]
        )
        try:
            bq_client.query(query, job_config=job_config).result() # Wait for delete to complete
            logging.info(f"[{domain}] 🧹 Cleaned overlapping BQ data from {range_start} to {range_end} (Zero Duplicates Mode).")
        except Exception as e:
            logging.error(f"[{domain}] ❌ Failed to clean overlapping BQ data: {e}")


def get_gsc_sites(service, p_id, search_term):
    try:
        site_list = service.sites().list().execute()
        if 'siteEntry' in site_list:
            return [site['siteUrl'] for site in site_list['siteEntry']]
        return []
    except HttpError:
        return []


def push_to_bigquery(records, search_term):
    if not records:
        return

    logging.info(f"[{search_term}] 📤 Streaming chunk of {len(records)} items to BigQuery...")
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE}"
    chunk_size = 10000

    for i in range(0, len(records), chunk_size):
        batch = records[i:i + chunk_size]
        try:
            errors = bq_client.insert_rows_json(table_id, batch)
            if errors:
                logging.error(f"[{search_term}] ❌ BigQuery Streaming Errors: {errors[:3]}")
        except Exception as e:
            logging.error(f"[{search_term}] ❌ BigQuery Exception: {e}")


def process_and_stream_site_data(service, site_url, start_date, end_date, search_term, p_id):
    date_counts = defaultdict(int)
    if search_term not in site_url.lower():
        return date_counts

    start_row = 0
    row_limit = 25000
    
    # Used to verify strict uniqueness during the fetch process
    unique_ids_seen = set()

    while True:
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['date', 'query', 'page'],
            'rowLimit': row_limit,
            'startRow': start_row
        }

        try:
            response = service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
            if 'rows' not in response:
                break

            rows = response['rows']
            chunk_records = []

            for row in rows:
                gsc_date = row['keys'][0]
                query = row['keys'][1]
                page = row['keys'][2]

                hash_id = hashlib.md5(f"{gsc_date}_{query}_{page}_{site_url}".encode('utf-8')).hexdigest()

                # Deduplicate immediately in Python
                if hash_id in unique_ids_seen:
                    continue
                unique_ids_seen.add(hash_id)

                chunk_records.append({
                    "_id": hash_id,
                    "Date": f"{gsc_date} 00:00:00 UTC",
                    "Query": str(query) if query else None,
                    "Page": str(page) if page else None,
                    "Clicks": int(row.get('clicks', 0)),
                    "Impressions": int(row.get('impressions', 0)),
                    "CTR": float(row.get('ctr', 0.0)),
                    "Position": round(float(row.get('position', 0.0)), 2),
                    "Project_ID": str(p_id),
                    "Site_URL": str(site_url),
                    "domain": str(search_term)
                })

                date_counts[gsc_date] += 1

            if chunk_records:
                push_to_bigquery(chunk_records, search_term)

            if len(rows) < row_limit:
                break

            start_row += row_limit
            time.sleep(0.4)

        except HttpError as error:
            if error.resp.status in [403, 401]:
                logging.debug(f"[{search_term}] Skipping {site_url}: {error.reason}")
            else:
                logging.error(f"[{search_term}] ❌ API Call Failed for {site_url}: {error.resp.status} - {error.reason}")
            break
        except Exception as e:
            logging.error(f"[{search_term}] ❌ Unexpected API error for {site_url}: {e}")
            break

    return date_counts


def process_domain(target, primary_client):
    search_term = target.get("domain")
    start_date = target.get("start_date")

    start_time = datetime.now()
    total_date_counts = defaultdict(int)

    date_ranges = get_missing_date_ranges(search_term, start_date)

    if date_ranges:
        logging.info(f"[{search_term}] 🔄 Found {len(date_ranges)} missing date range(s) to process.")
        
        # --- PREVENT DUPLICATES BEFORE STREAMING ---
        delete_overlapping_data(search_term, date_ranges)
        
        service_accounts = fetch_service_accounts(target, primary_client)

        if not service_accounts:
            logging.warning(f"[{search_term}] ⚠️ No valid service accounts found in ActiveDomains jsonContents or DB.")
        else:
            for account in service_accounts:
                p_id = account.get('project_id', 'Unknown')
                try:
                    credentials = service_account.Credentials.from_service_account_info(
                        account['credentials'],
                        scopes=['https://www.googleapis.com/auth/webmasters.readonly']
                    )
                    gsc_service = build('searchconsole', 'v1', credentials=credentials)
                    sites = get_gsc_sites(gsc_service, p_id, search_term)

                    if sites:
                        for site in sites:
                            if search_term in site.lower():
                                for range_start, range_end in date_ranges:
                                    logging.info(f"[{search_term}] -> Fetching: {range_start} to {range_end}")

                                    site_date_counts = process_and_stream_site_data(
                                        gsc_service, site, range_start, range_end, search_term, p_id
                                    )

                                    for d, c in site_date_counts.items():
                                        total_date_counts[d] += c

                except Exception as e:
                    error_msg = str(e).lower()
                    if "invalid_grant" in error_msg or "invalid jwt signature" in error_msg:
                        logging.debug(f"[{search_term}] Skipped invalid/revoked Service Account for project '{p_id}'")
                    else:
                        logging.error(f"[{search_term}] ❌ Processing error on project '{p_id}': {e}")

    # Log Execution
    end_time = datetime.now()
    time_taken = (end_time - start_time).total_seconds()

    total_added = sum(total_date_counts.values())
    dates_array = [{"date": k, "count": v} for k, v in total_date_counts.items()]

    run_history_doc = {
        "domain": search_term,
        "starttime": start_time,
        "endtime": end_time,
        "timetaken": time_taken,
        "added": total_added,
        "dates": dates_array
    }

    try:
        db = primary_client.get_database(RUN_HISTORY_DB)
        db.get_collection(RUN_HISTORY_COLL).insert_one(run_history_doc)
        logging.info(f"[{search_term}] ✅ RunHistory logged ({total_added} strictly unique records added).")
    except Exception as e:
        logging.error(f"[{search_term}] ❌ Failed to insert RunHistory document: {e}")
        
    return run_history_doc


# ====================================================================
# MAIN
# ====================================================================
def main():
    script_start = time.time()
    logging.info("🚀 Booting Integrated Direct Search Console to BigQuery Pipeline...")

    if not MONGO_CREDENTIALS_URI:
        logging.error("❌ Setup aborted: MONGO_URI missing from environment setup declarations.")
        return

    try:
        primary_client = MongoClient(MONGO_CREDENTIALS_URI, serverSelectionTimeoutMS=5000)
        primary_client.admin.command('ping')
    except ConnectionFailure as e:
        logging.error(f"❌ Failed establishing connection to MongoDB: {e}")
        return

    targets = get_active_domains()
    if not targets:
        logging.error("❌ Process aborted: No active domains found in BigQuery.")
        return

    logging.info(f"🔥 Distributing {len(targets)} active domains into parallel workers (Max: {MAX_WORKERS})...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_domain, target, primary_client): target
            for target in targets
        }

        for future in as_completed(futures):
            target_info = futures[future]
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                logging.error(f"❌ Domain thread worker crashed on target: {target_info.get('domain')} | Error: {e}")

    primary_client.close()
    
    # Process results for Email summary
    results.sort(key=lambda x: x.get("added", 0), reverse=True)
    total_runtime = round(time.time() - script_start, 2)
    total_rows_added = sum(r.get("added", 0) for r in results)
    
    domain_cards_html = ""
    for r in results:
        added = r.get("added", 0)
        domain = r.get("domain", "Unknown")
        runtime = round(r.get("timetaken", 0), 2)
        
        if added > 0:
            indicator_color = "#34C759"  # Apple Green
            date_strings = [f"{d['date']} ({d['count']} rows)" for d in r.get("dates", [])]
            dates_formatted = "<br>".join(date_strings)
            info_block = f"""
            <div style="margin-top:16px; padding-top:16px; border-top:1px solid rgba(0,0,0,0.04);">
                <span style="color:#86868B; font-size:12px; display:block; margin-bottom:4px; text-transform:uppercase; letter-spacing:0.02em; font-weight:600;">Unique Dates & Rows Uploaded</span>
                <div style="margin-top:8px; padding:12px; background:rgba(245, 245, 247, 0.8); border-radius:8px; border:1px solid rgba(0,0,0,0.04); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:11px; color:#1D1D1F; overflow-x:auto;">
                    {dates_formatted}
                </div>
            </div>
            """
        else:
            indicator_color = "#86868B"  # Gray
            info_block = ""
            
        domain_cards_html += f"""
        <div style="background:#FFFFFF; border-radius:20px; padding:24px; margin-bottom:20px; box-shadow:0 4px 24px rgba(0, 0, 0, 0.04); border:1px solid rgba(0,0,0,0.02);">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                    <td align="left" valign="middle">
                        <h3 style="margin:0 0 6px 0; font-size:18px; font-weight:600; color:#1D1D1F; letter-spacing:-0.02em;">{domain}</h3>
                    </td>
                    <td align="right" valign="middle">
                        <div style="font-size:26px; font-weight:700; color:{indicator_color}; letter-spacing:-0.04em; line-height:1;">{added}</div>
                        <div style="font-size:12px; color:#86868B; font-weight:500; margin-top:6px;">Unique Rows Added</div>
                    </td>
                </tr>
            </table>
            {info_block}
            <div style="margin-top:20px; font-size:11px; color:#A1A1A6; text-transform:uppercase; letter-spacing:0.04em; font-weight:500;">
                Domain Sync Runtime: {runtime}s
            </div>
        </div>
        """

    # Build Final Email HTML
    email_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin:0; padding:0; background-color:#F5F5F7; font-family:-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; -webkit-font-smoothing:antialiased;"> 

    <div style="max-width:680px; margin:0 auto; padding:40px 20px;">

        <div style="text-align:center; margin-bottom:32px;">
            <div style="display:inline-block; background:rgba(52, 199, 89, 0.1); color:#34C759; padding:6px 16px; border-radius:20px; font-weight:600; font-size:13px; margin-bottom:16px; letter-spacing:-0.01em;">
                Sync Complete
            </div>
            <h1 style="margin:0 0 12px 0; font-size:32px; font-weight:700; color:#1D1D1F; letter-spacing:-0.04em;">GSC to BigQuery Summary</h1>
            <p style="margin:0; font-size:16px; line-height:1.5; color:#515154;">Below is the summary of Search Console data synced to BigQuery during this execution run. BQ Overlaps were pre-deleted to prevent duplicates.</p>
        </div>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
            <tr>
                <td width="31%" valign="top">
                    <div style="background:#FFFFFF; padding:20px; border-radius:20px; box-shadow:0 4px 20px rgba(0,0,0,0.03); text-align:center;">
                        <div style="font-size:28px; font-weight:700; color:#1D1D1F; letter-spacing:-0.04em;">{total_runtime}s</div>
                        <div style="font-size:13px; color:#86868B; font-weight:500; margin-top:4px;">Total Runtime</div>
                    </div>
                </td>
                <td width="3.5%"></td>
                <td width="31%" valign="top">
                    <div style="background:#FFFFFF; padding:20px; border-radius:20px; box-shadow:0 4px 20px rgba(0,0,0,0.03); text-align:center;">
                        <div style="font-size:28px; font-weight:700; color:#1D1D1F; letter-spacing:-0.04em;">{len(results)}</div>
                        <div style="font-size:13px; color:#86868B; font-weight:500; margin-top:4px;">Domains Checked</div>
                    </div>
                </td>
                <td width="3.5%"></td>
                <td width="31%" valign="top">
                    <div style="background:rgba(52, 199, 89, 0.05); border:1px solid rgba(52, 199, 89, 0.1); padding:20px; border-radius:20px; text-align:center;">
                        <div style="font-size:28px; font-weight:700; color:#34C759; letter-spacing:-0.04em;">{total_rows_added}</div>
                        <div style="font-size:13px; color:#34C759; font-weight:600; margin-top:4px;">Total Unique Rows Added</div>
                    </div>
                </td>
            </tr>
        </table>

        {domain_cards_html}

        <div style="margin-top:48px; text-align:center;">
            <p style="margin:0; font-size:12px; color:#86868B; font-weight:500;">Automated GSC Data Pipeline</p>
            <p style="margin:6px 0 0 0; font-size:12px; color:#A1A1A6;">This is a System Generated Mail</p>
        </div>

    </div>

    </body>
    </html>
    """
    
    # Send Summary
    send_email(email_html)
    logging.info("🎉 Sync Pipeline completed successfully.")


if __name__ == "__main__":
    main()
