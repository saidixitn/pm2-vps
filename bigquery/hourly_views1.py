import os
import re
import sys
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# ==============================================================================
# 1. SETUP LOGGING & ENVIRONMENT CONFIG (VPS OPTIMIZED)
# ==============================================================================
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

load_dotenv()
LOCAL_MONGO_URI = os.getenv("MONGO_URI")

if not LOCAL_MONGO_URI:
    logging.critical("MONGO_URI environment variable is missing.")
    sys.exit(1)

PROJECT_ID = "dixit-464912"
DATASET_NAME = "All_Domains_Clicks"
DATASET_ID = f"{PROJECT_ID}.{DATASET_NAME}"
HISTORY_TABLE_ID = f"{DATASET_ID}.run_history"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

MONGO_CLIENTS_CACHE = {}

def get_mongo_client(uri):
    if uri not in MONGO_CLIENTS_CACHE:
        MONGO_CLIENTS_CACHE[uri] = MongoClient(uri, serverSelectionTimeoutMS=8000, maxPoolSize=50)
        MONGO_CLIENTS_CACHE[uri].admin.command("ping")
    return MONGO_CLIENTS_CACHE[uri]

try:
    local_client = get_mongo_client(LOCAL_MONGO_URI)
    
    creds_doc = local_client["google_creds"]["creds"].find_one({"name": "google_credentials"})
    if not creds_doc or "content" not in creds_doc:
        logging.critical("Google credentials payload not found in google_creds.creds")
        sys.exit(1)
        
    bq_credentials = service_account.Credentials.from_service_account_info(
        creds_doc["content"], scopes=SCOPES
    )
    bq_client = bigquery.Client(credentials=bq_credentials, project=creds_doc["content"]["project_id"])
    
except Exception as e:
    logging.critical(f"Initialization failed: {e}")
    sys.exit(1)


def format_iso_ms(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0000'


# ==============================================================================
# 2. UTILITY SANITIZATION & HISTORICAL RECORD LOGGING
# ==============================================================================
def setup_dataset_and_history():
    try:
        bq_client.get_dataset(bq_client.dataset(DATASET_NAME))
    except NotFound:
        bq_client.create_dataset(bigquery.Dataset(bq_client.dataset(DATASET_NAME)))

    try:
        bq_client.get_table(HISTORY_TABLE_ID)
    except NotFound:
        schema = [
            bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Hourly_Clicks", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("hour", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("start_time", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("end_time", "STRING", mode="REQUIRED")
            ])
        ]
        table = bigquery.Table(HISTORY_TABLE_ID, schema=schema)
        bq_client.create_table(table)


def sanitize_table_name(name):
    clean = str(name).replace(".com", "").replace(".net", "").replace(".org", "")
    clean = clean.replace("/", "_").replace(".", "_").replace(" ", "_").replace("-", "_").lower().strip()
    return re.sub(r'[^a-zA-Z0-9_]', '', clean)


def upsert_hourly_history(history_col, domain_val, date_str, hour_str, inserted_count, exec_start, exec_end):
    """Safely pushes or increments the hour object to prevent array duplicates."""
    existing_hour = history_col.find_one(
        {"domain": domain_val, "date": date_str, "Hourly_Clicks.hour": hour_str}
    )

    if existing_hour:
        # Only modify the array if new clicks arrived during this re-run
        if inserted_count > 0:
            history_col.update_one(
                {"domain": domain_val, "date": date_str, "Hourly_Clicks.hour": hour_str},
                {
                    "$inc": {"Hourly_Clicks.$.clicks": inserted_count},
                    "$set": {"Hourly_Clicks.$.end_time": format_iso_ms(exec_end)}
                }
            )
    else:
        hour_obj = {
            "hour": hour_str,
            "clicks": inserted_count,
            "start_time": format_iso_ms(exec_start),
            "end_time": format_iso_ms(exec_end)
        }
        history_col.update_one(
            {"domain": domain_val, "date": date_str},
            {"$push": {"Hourly_Clicks": hour_obj}},
            upsert=True
        )


# ==============================================================================
# 3. DIRECT INSERT WORKER (NO BQ READS)
# ==============================================================================
def insert_records_to_bq(clean_table_name, df_incoming):
    target_table_id = f"{DATASET_ID}.{clean_table_name}"

    if df_incoming.empty:
        return target_table_id, 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )

    inserted_count = len(df_incoming)
    bq_client.load_table_from_dataframe(df_incoming, target_table_id, job_config=job_config).result()
    return target_table_id, inserted_count


# ==============================================================================
# 4. DOMAIN-SPECIFIC EXTRACTION & PROCESSING THREAD
# ==============================================================================
def process_domain_worker(config_doc, windows_to_run, creds_map):
    domain_name = config_doc.get("domain", config_doc.get("Domain"))
    domain_type = str(config_doc.get("domainType", config_doc.get("Domain_Type", ""))).strip().lower()
    employer_id = config_doc.get("EmployerId", config_doc.get("employerId"))
    
    bq_table_override = config_doc.get("BigQuery_Table")
    clean_table_id = bq_table_override if bq_table_override else sanitize_table_name(domain_name)

    if "dc_subdomain" in domain_type:
        db_name = "directclients_prod"
    elif "dc_proxy" in domain_type or "proxy" in domain_type:
        db_name = "prod_jobiak_ai"
    else:
        domain_clean = domain_name.split('/')[0]
        db_base_name = domain_clean.replace(".com", "").replace(".net", "").replace(".org", "").strip()
        db_name = f"{db_base_name}_prod"

    record = creds_map.get(db_name)
    if not record and "dc_subdomain" not in domain_type and "proxy" not in domain_type and "." in db_base_name:
        root_part = db_base_name.split(".")[-1]
        record = creds_map.get(f"{root_part}_prod")

    if not record:
        logging.error(f"Credentials missing for {db_name}. Skipping {domain_name}.")
        return clean_table_id

    prod_client = get_mongo_client(record["mongo_uri"])
    db = prod_client[db_name]
    col = db["userAnalytics"]
    history_col = local_client["surveys_prod"]["bigquery_run_history"]

    for min_dt in windows_to_run:
        max_dt = min_dt + timedelta(hours=1) - timedelta(milliseconds=1)
        hour_str = f"{min_dt.hour:02d}"
        date_str = min_dt.strftime('%Y-%m-%d')
        exec_start = datetime.now(timezone.utc)

        match_stage = {
            "isBot": False,
            "browserType": {"$ne": "Unknown"},
            "deviceType": {"$ne": "Unknown"},
            "isFromGoogle": True,
            "createdDt": {"$gte": min_dt, "$lte": max_dt}
        }

        if employer_id:
            match_stage["employerId"] = employer_id

        if "proxy" in domain_type:
            pipeline = [
                {"$match": match_stage},
                {"$lookup": {
                    "from": "job",
                    "localField": "childJobId",
                    "foreignField": "jobId",
                    "as": "job_info"
                }},
                {"$unwind": {"path": "$job_info", "preserveNullAndEmptyArrays": True}},
                {"$project": {
                    "_id": 0,
                    "ipAddress": {"$ifNull": ["$ipAddress", ""]},
                    "userAgent": {"$ifNull": ["$userAgent", ""]},
                    "employerId": {"$ifNull": ["$employerId", ""]},
                    "uniqueId": {"$ifNull": ["$uniqueId", ""]},
                    "childJobId": {"$ifNull": ["$childJobId", ""]},
                    "action": {"$ifNull": ["$action", ""]},
                    "browserType": {"$ifNull": ["$browserType", ""]},
                    "deviceType": {"$ifNull": ["$deviceType", ""]},
                    "createdDt": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:%S.%L+0000", "date": "$createdDt"}},
                    "company": {"$trim": {"input": {"$ifNull": ["$companyName", {"$ifNull": ["$company", "Unknown"]}]}}},
                    "jobtitle": {"$trim": {"input": {"$ifNull": ["$job_info.title", "Unknown"]}}},
                    "location": {"$ifNull": ["$job_info.location", ""]},
                    "submittedUrl": {"$ifNull": ["$job_info.submittedUrl", ""]},
                    "sourceEmployerId": {"$ifNull": ["$extraInfo.sourceEmployerId", ""]}
                }}
            ]
        else:
            pipeline = [
                {"$match": match_stage},
                {"$project": {
                    "_id": 0,
                    "ipAddress": {"$ifNull": ["$ipAddress", ""]},
                    "userAgent": {"$ifNull": ["$userAgent", ""]},
                    "employerId": {"$ifNull": ["$employerId", ""]},
                    "action": {"$ifNull": ["$action", ""]},
                    "submittedUrl": {"$ifNull": ["$submittedUrl", ""]},
                    "createdDt": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:%S.%L+0000", "date": "$createdDt"}},
                    "browserType": {"$ifNull": ["$browserType", ""]},
                    "deviceType": {"$ifNull": ["$deviceType", ""]},
                    "company": {"$trim": {"input": {"$ifNull": ["$company", {"$ifNull": ["$companyName", "Unknown"]}]}}},
                    "jobtitle": {"$trim": {"input": {"$ifNull": ["$jobtitle", "Unknown"]}}},
                    "location": {"$ifNull": ["$location", ""]},
                    "url": 1,
                }},
                {"$addFields": {
                    "targetUrl": {"$ifNull": ["$url", {"$ifNull": ["$submittedUrl", ""]}]}
                }},
                {"$addFields": {
                    "calculatedEndUrl": {
                        "$ifNull": [{"$arrayElemAt": [{"$split": ["$targetUrl", "/"]}, 2]}, ""]
                    }
                }},
                {"$addFields": {
                    "sourceEmployerId": {"$ifNull": ["$extraInfo.sourceEmployerId", "$calculatedEndUrl"]}
                }},
                {"$project": {"url": 0, "targetUrl": 0, "calculatedEndUrl": 0}}
            ]

        docs = list(col.aggregate(pipeline, allowDiskUse=True))
        inserted_count = 0

        if docs:
            df = pd.DataFrame(docs)
            
            if "location" in df.columns:
                df["location"] = df["location"].apply(
                    lambda loc: re.sub(r',\s*', ', ', loc).strip() if isinstance(loc, str) else loc
                )

            df = df.replace({'nan': None, '<NA>': None, '': None, 'None': None})
            df.drop_duplicates(inplace=True)
            
            # THE FIX: Call the updated, faster function with just two arguments
            _, inserted_count = insert_records_to_bq(clean_table_id, df)

        exec_end = datetime.now(timezone.utc)
        logging.info(f"[{domain_name}] Hour {hour_str} ({date_str}) | Inserted: {inserted_count}")

        # Unique Upsert Logic
        upsert_hourly_history(history_col, domain_name, date_str, hour_str, inserted_count, exec_start, exec_end)

    return clean_table_id


# ==============================================================================
# 5. ORCHESTRATION & ARGUMENT PARSER
# ==============================================================================
def run_hourly_pipeline():
    setup_dataset_and_history()

    active_configs = list(local_client["surveys_prod"]["domains"].find({"isActive": True}))
    if not active_configs:
        logging.info("No active domains found. Exiting.")
        return

    creds_list = list(local_client["mongo_creds"]["creds"].find({}))
    creds_map = {doc["domain"]: doc for doc in creds_list}

    table_meta_map = {}
    for config in active_configs:
        d_name = config.get("domain", config.get("Domain", ""))
        d_type = str(config.get("domainType", config.get("Domain_Type", ""))).strip().lower()
        
        bq_table_override = config.get("BigQuery_Table")
        c_table = bq_table_override if bq_table_override else sanitize_table_name(d_name)
        
        table_meta_map[f"{DATASET_ID}.{c_table}"] = {
            "sourceDomain": d_name,
            "DomainType": d_type,
            "SourceTable": c_table
        }

    now_utc = datetime.now(timezone.utc)
    last_hour = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    
    # ---------------------------------------------------------
    # DATE PARSING LOGIC
    # ---------------------------------------------------------
    windows_to_run = []
    args = [arg for arg in sys.argv[1:] if arg.strip() != '-']

    try:
        if len(args) >= 2:
            start_date = datetime.strptime(args[0], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(args[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_time = end_date + timedelta(hours=23)
            logging.info(f"Targeting date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            curr = start_date
            while curr <= min(end_time, last_hour):
                windows_to_run.append(curr)
                curr += timedelta(hours=1)
                
        elif len(args) == 1:
            target_date = datetime.strptime(args[0], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_time = target_date + timedelta(hours=23)
            logging.info(f"Targeting single day: {target_date.strftime('%Y-%m-%d')}")
            
            curr = target_date
            while curr <= min(end_time, last_hour):
                windows_to_run.append(curr)
                curr += timedelta(hours=1)
                
        else:
            logging.info(f"Default run: Targeting previous hour only ({last_hour.strftime('%Y-%m-%d %H:00')})")
            windows_to_run.append(last_hour)
            
    except ValueError as e:
        logging.error(f"Invalid date format provided. Please use YYYY-MM-DD. Error: {e}")
        sys.exit(1)

    if not windows_to_run:
        logging.info("No valid time windows to process. Exiting.")
        return

    # ---------------------------------------------------------
    # PARALLEL EXECUTION (10 WORKERS)
    # ---------------------------------------------------------
    global_table_ledger = set()
    domain_tasks = [(config, windows_to_run) for config in active_configs]

    logging.info(f"Processing {len(domain_tasks)} domains with 10 concurrent workers.")
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="Worker") as executor:
        futures = [executor.submit(process_domain_worker, task[0], task[1], creds_map) for task in domain_tasks]
        for future in as_completed(futures):
            t_id = future.result()
            if t_id:
                global_table_ledger.add(f"{DATASET_ID}.{t_id}")

    # ---------------------------------------------------------
    # CONSOLIDATE MASTER VIEWS
    # ---------------------------------------------------------
    if global_table_ledger:
        logging.info("Rebuilding master_views schema consolidation...")
        try:
            all_dataset_tables = bq_client.list_tables(DATASET_ID)
            verified_tables = []
            all_fields_map = {}

            for table_item in all_dataset_tables:
                if table_item.table_id not in ["master_views", "run_history"]:
                    t_id = f"{DATASET_ID}.{table_item.table_id}"
                    verified_tables.append(t_id)
                    t_obj = bq_client.get_table(t_id)
                    for field in t_obj.schema:
                        if field.name not in ["sourceDomain", "SourceTable", "DomainType"]:
                            all_fields_map[field.name] = field.field_type

            if verified_tables and all_fields_map:
                union_segments = []
                for table_id in verified_tables:
                    try:
                        t = bq_client.get_table(table_id)
                        current_fields = {field.name for field in t.schema}

                        select_fields = []
                        for f_name, f_type in all_fields_map.items():
                            if f_name in current_fields:
                                select_fields.append(f"`{f_name}`")
                            else:
                                select_fields.append(f"CAST(NULL AS {f_type}) AS `{f_name}`")

                        meta = table_meta_map.get(table_id, {"sourceDomain": "Unknown", "DomainType": "Unknown", "SourceTable": table_id.split('.')[-1]})
                        select_fields.append(f"'{meta['sourceDomain']}' AS `sourceDomain`")
                        select_fields.append(f"'{meta['SourceTable']}' AS `SourceTable`")
                        select_fields.append(f"'{meta['DomainType']}' AS `DomainType`")

                        field_selection_str = ", ".join(select_fields)
                        union_segments.append(f"SELECT {field_selection_str} FROM `{table_id}`")
                    except Exception as e:
                        logging.error(f"Failed to process master view block for {table_id}: {e}")

                if union_segments:
                    master_table_id = f"{DATASET_ID}.master_views"
                    master_query = f"""
                        CREATE OR REPLACE TABLE `{master_table_id}` AS 
                        WITH global_consolidation AS (
                            {" UNION ALL ".join(union_segments)}
                        )
                        SELECT DISTINCT * FROM global_consolidation;
                    """
                    bq_client.query(master_query).result()
                    logging.info("master_views updated successfully.")
        except Exception as e:
            logging.error(f"Consolidation step failed: {e}")

    # ---------------------------------------------------------
    # SYNC HISTORY TO BIGQUERY
    # ---------------------------------------------------------
    try:
        history_col = local_client["surveys_prod"]["bigquery_run_history"]
        history_data = list(history_col.find({}, {"_id": 0}))
        if history_data:
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            bq_client.load_table_from_json(history_data, HISTORY_TABLE_ID, job_config=job_config).result()
    except Exception as e:
        logging.error(f"Failed to push run history to BigQuery: {e}")

    logging.info("Pipeline execution complete.")


if __name__ == "__main__":
    run_hourly_pipeline()
