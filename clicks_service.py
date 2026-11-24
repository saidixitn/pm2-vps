import os
import numpy as np
import pandas as pd
import gspread
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from dotenv import load_dotenv

# =========================
# ENV & CONFIG & LOGGING
# =========================
load_dotenv()
logger = logging.getLogger("JobGetAPI")

# =========================
# UTILITIES & HELPERS
# =========================
def clean_domain(d: str) -> str:
    return (d or "").replace("https://", "").replace("http://", "").strip()

def to_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0

def get_or_create_worksheet(sheet, name, rows=200, cols=30):
    try:
        return sheet.worksheet(name)
    except WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=rows, cols=cols)

def make_unique_columns(columns):
    seen, fixed = {}, []
    for c in columns:
        if c not in seen:
            seen[c] = 1
            fixed.append(c)
        else:
            seen[c] += 1
            fixed.append(f"{c}_{seen[c]}")
    return fixed

def coerce_cell(v):
    """Return a real number if the value is numeric, otherwise a string.
    This makes Google Sheets store numbers as numbers (not text)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        if isinstance(v, float):
            if np.isnan(v):
                return ""
            if v.is_integer():
                return int(v)
        return v
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        if np.isnan(f):
            return ""
        return int(f) if f.is_integer() else f
    s = str(v).strip()
    if s == "":
        return ""
    # Try to parse numeric-looking strings into real numbers
    try:
        if s.lstrip("+-").isdigit():
            return int(s)
        f = float(s)
        return int(f) if f.is_integer() else f
    except ValueError:
        return s

def df_to_values(df):
    """Convert a DataFrame to a list-of-lists where numeric cells are
    real numbers so Sheets treats them as numbers (with RAW input)."""
    header = [str(c) for c in df.columns.tolist()]
    body = [[coerce_cell(v) for v in row] for row in df.values.tolist()]
    return [header] + body

# =========================
# SAFE SHEET UPDATE
# =========================
def smart_update(ws, df, key_col="Date"):
    try:
        old = pd.DataFrame(ws.get_all_records())
    except Exception:
        old = pd.DataFrame()

    df = df.replace([np.nan, None], "")
    df = df.fillna("")
    df.columns = make_unique_columns(df.columns)

    if old.empty:
        ws.clear()
        ws.update(df_to_values(df), value_input_option="RAW")
        return

    all_cols = list(dict.fromkeys(list(old.columns) + list(df.columns)))
    old = old.reindex(columns=all_cols, fill_value="")
    df = df.reindex(columns=all_cols, fill_value="")

    keys = set(df[key_col].astype(str)) if key_col in df.columns else set()
    if key_col in old.columns:
        old = old[~old[key_col].astype(str).isin(keys)]

    merged = pd.concat([old, df], ignore_index=True).fillna("")
    ws.clear()
    ws.update(df_to_values(merged), value_input_option="RAW")

# =========================
# MONGO CONNECTION
# =========================
def connect_mongo(domain_record, db_name):
    mongo_uri = domain_record.get("mongo_uri")
    if not mongo_uri:
        raise ValueError(f"No mongo_uri found for {db_name}")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[db_name], db_name

# =========================
# FETCHERS: GOOGLE SHEETS
# =========================
def fetch_postings_programmatic(domain_name, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"employerId": {"$exists": True}},
        {"job_status": {"$ne": "3.0"}},
        {"gpost": 5.0},
        {"gpost_date": {"$gte": min_dt, "$lt": max_dt}},
    ]
    if employer_id: match_conditions.append({"employerId": employer_id})
    if exclude_list: match_conditions.append({"employerId": {"$nin": exclude_list}})

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$group": {
            "_id": {
                "employerId": "$employerId",
                "gpost_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$gpost_date"}},
                "company": {"$ifNull": ["$company", "Unknown"]}
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.gpost_date", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "PostingsCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db[collection_name].aggregate(pipeline, allowDiskUse=True))

def fetch_expires_programmatic(domain_name, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"employerId": {"$exists": True}},
        {"gpost_expire_date": {"$gte": min_dt, "$lt": max_dt}},
    ]
    if employer_id: match_conditions.append({"employerId": employer_id})
    if exclude_list: match_conditions.append({"employerId": {"$nin": exclude_list}})

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$group": {
            "_id": {
                "employerId": "$employerId",
                "expire_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$gpost_expire_date"}},
                "company": {"$ifNull": ["$company", "Unknown"]}
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.expire_date", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "ExpiresCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db[collection_name].aggregate(pipeline, allowDiskUse=True))

def fetch_views_programmatic(domain_name, db, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"isBot": False},
        {"browserType": {"$ne": "Unknown"}},
        {"deviceType": {"$ne": "Unknown"}},
        {"isFromGoogle": True},
        {"createdDt": {"$gt": min_dt, "$lt": max_dt}},
    ]
    if employer_id: match_conditions.append({"employerId": employer_id})
    if exclude_list: match_conditions.append({"employerId": {"$nin": exclude_list}})

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$project": {
            "company": {"$ifNull": ["$company", "Unknown"]},
            "viewDate": {"$substr": ["$createdDt", 0, 10]},
            "employerId": 1,
        }},
        {"$group": {
            "_id": {
                "company": "$company", "viewDate": "$viewDate", "employerId": "$employerId",
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.viewDate", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "ViewsCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db["userAnalytics"].aggregate(pipeline, allowDiskUse=True))

def fetch_unique_ips(domain_name, db, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"isBot": False},
        {"browserType": {"$ne": "Unknown"}},
        {"deviceType": {"$ne": "Unknown"}},
        {"isFromGoogle": True},
        {"createdDt": {"$gt": min_dt, "$lt": max_dt}},
    ]
    if employer_id: match_conditions.append({"employerId": employer_id})
    if exclude_list: match_conditions.append({"employerId": {"$nin": exclude_list}})

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$project": {
            "sourceEmployerId": {"$ifNull": ["$extraInfo.sourceEmployerId", ""]},
            "employerId": 1,
            "company": {"$ifNull": ["$company", "Unknown"]},
            "date": {"$substr": ["$createdDt", 0, 10]},
            "ipAddress": 1
        }},
        {"$group": {
            "_id": {
                "sourceEmployerId": "$sourceEmployerId", "employerId": "$employerId",
                "company": "$company", "date": "$date"
            },
            "ips": {"$addToSet": "$ipAddress"}
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.date", "sourceEmployerId": "$_id.sourceEmployerId",
            "EmployerId": "$_id.employerId", "Company": "$_id.company",
            "UniqueIpCount": {"$size": "$ips"},
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db["userAnalytics"].aggregate(pipeline, allowDiskUse=True))

# =========================
# WORKER
# =========================
def process_domain(row, min_dt, max_dt, domains_col):
    domain = row.get("Domain", "").strip()
    db_name = row.get("Database", "").strip()
    domain_type = row.get("Domain Type", "").strip()
    employer_id = str(row.get("EmployerId", "")).strip()
    collection_name = row.get("Collection", "").strip()

    exclude_raw = row.get("Exclude_Employers", "")
    exclude_list = [e.strip() for e in exclude_raw.split(",") if e.strip()]

    if not domain or not db_name:
        return {"success": False, "reason": "Missing domain or database"}

    record = domains_col.find_one({"domain": db_name})
    if not record:
        logger.warning(f"⚠️ Skipped (No Mongo record): {db_name}")
        return {"success": False, "reason": "No Mongo record"}

    try:
        db, _ = connect_mongo(record, db_name)

        # Sheets Pipeline
        postings = fetch_postings_programmatic(domain, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type)
        views = fetch_views_programmatic(domain, db, employer_id, exclude_list, min_dt, max_dt, domain_type)
        unique = fetch_unique_ips(domain, db, employer_id, exclude_list, min_dt, max_dt, domain_type)
        expires = fetch_expires_programmatic(domain, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type)

        logger.info(f"✅ {domain} done. (Views: {len(views)})")
        return {
            "success": True,
            "domain": domain,
            "sheet_data": (postings, views, unique, expires),
        }
    except Exception as e:
        logger.error(f"❌ {domain} failed during fetch: {e}")
        return {"success": False, "reason": str(e)}

# =========================
# MAIN PIPELINE FUNCTION
# =========================
def run_clicks_pipeline(date_str: str):
    try:
        logger.info(f"🚀 Initializing unified report builder for {date_str}...")

        min_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        max_dt = min_dt + timedelta(days=1)

        LOCAL_MONGO_URI = os.getenv("NEW_MONGO_URI")
        CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS", "clicks_credentials.json")
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

        # Load local Mongo
        try:
            local_client = MongoClient(LOCAL_MONGO_URI)
            domains_col = local_client["mongo_creds"]["creds"]
            stats_col = local_client["daily_domain_stats"]["stats"]
        except Exception as e:
            logger.error(f"❌ Failed to connect to local Mongo DB: {e}")
            return

        # Clean old stats
        stats_col.delete_many({"Date": date_str})

        # Auth Google Sheets
        try:
            creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
            gc = gspread.authorize(creds)
            sheet = gc.open("Domain Stats")
            domains_ws = sheet.worksheet("Domains")
            domains_rows = domains_ws.get_all_records()

            postings_ws = get_or_create_worksheet(sheet, "Postings")
            views_ws = get_or_create_worksheet(sheet, "Views")
            unique_ws = get_or_create_worksheet(sheet, "UniqueClicks")
            expires_ws = get_or_create_worksheet(sheet, "Expires")
        except Exception as e:
            logger.error(f"❌ Failed to authorize or load Google Sheets: {e}")
            return

        # Prepare Domain Data
        for r in domains_rows:
            r["Domain"] = (r.get("Domain") or "").strip()
            r["Database"] = (r.get("Database") or "").strip()
            r["Domain Type"] = (r.get("Domain Type") or "").strip()
            r["EmployerId"] = (str(r.get("EmployerId") or "")).strip()

        # Collectors
        all_sheet_postings, all_sheet_views, all_sheet_unique, all_sheet_expires = [], [], [], []

        # Thread Pool Data Fetching
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(process_domain, row, min_dt, max_dt, domains_col): row for row in domains_rows}
            for fut in as_completed(futures):
                res = fut.result()
                if not res["success"]:
                    continue

                p, v, u, e = res["sheet_data"]
                all_sheet_postings.extend(p)
                all_sheet_views.extend(v)
                all_sheet_unique.extend(u)
                all_sheet_expires.extend(e)

        # Write back to Local Mongo (backup)
        combined_mongo_inserts = []
        now = datetime.now(timezone.utc)
        for item in all_sheet_postings + all_sheet_views + all_sheet_unique:
            item["InsertedAt"] = now
            combined_mongo_inserts.append(item)

        if combined_mongo_inserts:
            try:
                stats_col.insert_many(combined_mongo_inserts)
                logger.info("✅ Stats safely backed up to MongoDB.")
            except Exception as e:
                logger.error(f"❌ Failed to insert into Mongo stats_col: {e}")

        # Write back to Google Sheets (numbers stored as numbers)
        try:
            if all_sheet_postings: smart_update(postings_ws, pd.DataFrame(all_sheet_postings).fillna(""))
            if all_sheet_views: smart_update(views_ws, pd.DataFrame(all_sheet_views).fillna(""))
            if all_sheet_unique: smart_update(unique_ws, pd.DataFrame(all_sheet_unique).fillna(""))
            if all_sheet_expires: smart_update(expires_ws, pd.DataFrame(all_sheet_expires).fillna(""))
            logger.info("✅ Google Sheets Smart Update Complete.")
        except Exception as e:
            logger.error(f"❌ Failed to update Google Sheets: {e}")

        logger.info(f"🎉 ALL DONE. Pipeline executed fully for {date_str}.")

    except Exception as e:
        logger.error(f"❌ Critical error in clicks pipeline: {str(e)}", exc_info=True)