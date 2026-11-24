import os
import sys
import time
import requests
import pandas as pd
from pymongo import MongoClient
from google.oauth2.service_account import Credentials
import gspread
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import gettempdir
import logging
from gspread.exceptions import WorksheetNotFound
from typing import Dict, Any, Optional

load_dotenv()

# Safe import for config module with fallback
try:
    import config
except ImportError:
    class ConfigFallback:
        GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")
        GOOGLE_SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        REMOTE_MONGO_URI = os.getenv("MONGO_URI", os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017"))
        SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
    config = ConfigFallback()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)

CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS", getattr(config, "GOOGLE_CREDS_FILE", "credentials.json"))
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", os.getenv("MONGO_URI", getattr(config, "REMOTE_MONGO_URI", "mongodb://localhost:27017")))

def get_sheets_client():
    if not os.path.exists(CREDENTIALS_FILE):
        raise RuntimeError(f"Credentials file not found: {CREDENTIALS_FILE}")
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=config.GOOGLE_SCOPES)
    return gspread.authorize(creds)


def get_or_create_ws(sheet, name):
    try:
        return sheet.worksheet(name)
    except WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=200, cols=30)


def connect_mongo(domain: str):
    local_client = MongoClient(LOCAL_MONGO_URI, socketTimeoutMS=20000, connectTimeoutMS=10000)
    creds_col = local_client["mongo_creds"]["creds"]
    record = creds_col.find_one({"domain": domain})
    if not record:
        raise ValueError(f"No mongo_uri for domain {domain}")
    client = MongoClient(record["mongo_uri"], serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[domain]


def run_target_p4_query(db, employer_ids):
    match_stage = {
        "employerId": {"$in": employer_ids} if employer_ids else {"$exists": True},
        "job_status": {"$ne": 3},
        "gpost": 1
    }
    pipeline = [
        {"$match": match_stage},
        {"$group": {
            "_id": {
                "jobtitle": "$jobtitle",
                "employerId": "$employerId",
                "company": "$company",
                "isRemote": "$isRemote",
                "atlExpand": "$extraInfo.atlExpand",
            },
            "Jobs": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0,
            "Jobs": 1,
            "jobtitle": "$_id.jobtitle",
            "company": "$_id.company",
            "isRemote": "$_id.isRemote",
            "ATL Expand Status": "$_id.atlExpand",
            "employerId": "$_id.employerId",
            "lookupKey": {
                "$concat": [
                    "$_id.jobtitle", "=", "$_id.company", "=", "$_id.employerId"
                ]
            }
        }}
    ]
    return list(db["Target_P4_Opt"].aggregate(pipeline, allowDiskUse=True))


def run_description_query(db, employer_ids):
    match_stage = {"employerId": {"$in": employer_ids} if employer_ids else {"$exists": True}}
    pipeline = [
        {"$match": match_stage},
        {"$project": {
            "_id": 0,
            "jobtitle": 1,
            "company": 1,
            "employerId": 1,
            "lookupKey": {
                "$concat": ["$jobtitle", "=", "$company", "=", "$employerId"]
            }
        }}
    ]
    return list(db["alternative_desc_change"].aggregate(pipeline, allowDiskUse=True))


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "Jobs" in df.columns:
        df["Jobs"] = pd.to_numeric(df["Jobs"], errors="coerce").fillna(0).astype("int32")

    if "ATL Expand Status" in df.columns:
        df["ATL Expand Status"] = pd.to_numeric(df["ATL Expand Status"], errors="coerce")
        df["ATL Expand Status"] = df["ATL Expand Status"].apply(lambda x: 1 if x == 1 else "")
        df["ATL Expand Status"] = df["ATL Expand Status"].astype(object)

    if "isRemote" in df.columns:
        df["isRemote"] = (
            df["isRemote"]
            .astype(str)
            .str.lower()
            .map({"true": "TRUE", "false": "FALSE"})
            .fillna("FALSE")
        )

    if "Description Status" in df.columns:
        df["Description Status"] = df["Description Status"].replace(
            {"Desc Missing": "Desc Not Found"}
        )

    return df


def process_domain(domain_row, retries=3):
    domain_name = str(domain_row["Domain"]).strip()
    employer_field = str(domain_row.get("employerId", "") or "").strip()
    employer_ids = [x.strip() for x in employer_field.split(",") if x.strip()] if employer_field else []

    for attempt in range(1, retries + 1):
        try:
            db = connect_mongo(domain_name)
            postings = run_target_p4_query(db, employer_ids)
            descs = run_description_query(db, employer_ids)
            desc_lookup = {d["lookupKey"] for d in descs}

            for p in postings:
                p["Domain"] = domain_name
                p["Description Status"] = (
                    "Desc Found" if p["lookupKey"] in desc_lookup else "Desc Not Found"
                )
                p["concatenated Jobtitle"] = f'"{p["jobtitle"]}",'

            expected_columns = [
                "Domain", "jobtitle", "company", "isRemote", "Jobs",
                "ATL Expand Status", "Description Status", "concatenated Jobtitle", "lookupKey", "employerId"
            ]

            df_post = pd.DataFrame(postings)
            if df_post.empty:
                df_post = pd.DataFrame(columns=expected_columns)

            df_post = clean_dataframe(df_post)

            df_desc = pd.DataFrame(descs)
            if not df_desc.empty:
                df_desc["Domain"] = domain_name

            tmp_post = os.path.join(gettempdir(), f"targetp4_{domain_name}.csv")
            tmp_desc = os.path.join(gettempdir(), f"desc_{domain_name}.csv")
            df_post.to_csv(tmp_post, index=False)
            df_desc.to_csv(tmp_desc, index=False)
            logging.info(f"✅ {domain_name}: {len(df_post)} targetP4, {len(df_desc)} desc rows.")
            return tmp_post, tmp_desc

        except Exception as e:
            logging.error(f"❌ {domain_name} failed (attempt {attempt}/{retries}): {e}")
            time.sleep(2)
    return None, None


def run_topost_pipeline(spreadsheet_name: str = "Postings") -> Dict[str, Any]:
    start_time = time.time()
    gc = get_sheets_client()

    try:
        sheet = gc.open(spreadsheet_name)
    except gspread.SpreadsheetNotFound:
        sp_id = getattr(config, "SPREADSHEET_ID", "")
        if sp_id:
            sheet = gc.open_by_key(sp_id)
        else:
            raise

    domains_ws = sheet.worksheet("Domains")
    target_ws = get_or_create_ws(sheet, "targetp4")
    desc_ws = get_or_create_ws(sheet, "Desc Not Found")
    stats_ws = get_or_create_ws(sheet, "Stats")

    domains_df = pd.DataFrame(domains_ws.get_all_records())
    if domains_df.empty:
        logging.warning("⚠️ No domains found in sheet.")
        return {
            "status": "warning",
            "message": "No domains found in Domains sheet",
            "elapsed_seconds": round(time.time() - start_time, 2)
        }

    workers = len(domains_df)
    logging.info(f"🚀 Spawning {workers} workers (one per domain)...")

    temp_posts, temp_descs = [], []
    failed = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_domain, row): row for _, row in domains_df.iterrows()}
        for fut in as_completed(futures):
            post_path, desc_path = fut.result()
            if post_path:
                temp_posts.append(post_path)
            if desc_path:
                temp_descs.append(desc_path)
            if not post_path:
                failed.append(futures[fut]["Domain"])

    valid_posts = []
    for f in temp_posts:
        if os.path.exists(f) and os.path.getsize(f) > 0:
            try:
                df = pd.read_csv(f)
                if not df.empty and not df.isna().all().all():
                    valid_posts.append(df)
            except pd.errors.EmptyDataError:
                logging.warning(f"⚠️ Skipping empty file: {f}")

    if valid_posts:
        final_posts = pd.concat(valid_posts, ignore_index=True)
    else:
        final_posts = pd.DataFrame()

    final_posts = clean_dataframe(final_posts)

    # API Call: Update isRemote to False ONLY when TRUE
    api_updated_count = 0
    if not final_posts.empty and "isRemote" in final_posts.columns:
        remote_true_mask = final_posts["isRemote"] == "TRUE"

        if remote_true_mask.any():
            remote_targets = final_posts[remote_true_mask][["Domain", "employerId"]].drop_duplicates()
            logging.info(
                f"🔄 Found {len(remote_targets)} domain/employer combinations with isRemote=TRUE. Updating via API...")

            for _, row in remote_targets.iterrows():
                db_domain = row["Domain"]
                emp_id = row["employerId"]

                api_domain = db_domain.replace("_prod", ".com")
                api_url = "http://104.251.219.45:5111/update_is_remote"
                params = {
                    "domain": api_domain,
                    "employerId": emp_id,
                    "isRemote": "false"
                }

                try:
                    response = requests.get(api_url, params=params, timeout=10)
                    response.raise_for_status()
                    api_updated_count += 1
                    logging.info(f"🌐 Success: API update for Domain: {api_domain}, EmployerId: {emp_id}")
                except Exception as e:
                    logging.error(f"❌ API call failed for {api_domain} - {emp_id}: {e}")

            final_posts.loc[remote_true_mask, "isRemote"] = "FALSE"
            logging.info("✅ DataFrame updated to reflect isRemote=FALSE before sheet upload.")

    # Filter DataFrames
    desc_found_df = final_posts[final_posts["Description Status"] == "Desc Found"].copy() if not final_posts.empty else pd.DataFrame()
    desc_not_found_df = final_posts[final_posts["Description Status"] == "Desc Not Found"].copy() if not final_posts.empty else pd.DataFrame()

    # Sort
    if not desc_found_df.empty:
        desc_found_df.sort_values(by=["Domain", "company", "Jobs"], ascending=[True, True, False], inplace=True)
    if not desc_not_found_df.empty:
        desc_not_found_df.sort_values(by=["Domain", "company", "Jobs"], ascending=[True, True, False], inplace=True)

    # Upload Sheets
    if not desc_found_df.empty:
        logging.info(f"📤 Uploading {len(desc_found_df)} rows to targetp4 (Desc Found) sheet...")
        target_ws.clear()
        target_ws.update([desc_found_df.columns.tolist()] + desc_found_df.values.tolist(),
                         value_input_option="USER_ENTERED")
    else:
        logging.warning("⚠️ No Desc Found data to upload.")

    if not desc_not_found_df.empty:
        logging.info(f"📤 Uploading {len(desc_not_found_df)} rows to Desc Not Found sheet...")
        desc_ws.clear()
        desc_ws.update([desc_not_found_df.columns.tolist()] + desc_not_found_df.values.tolist(),
                       value_input_option="USER_ENTERED")
    else:
        logging.warning("⚠️ No Desc Not Found data to upload.")

    # Stats Table
    if not final_posts.empty:
        stats = (
            final_posts
            .groupby(["Domain", "company", "isRemote"], as_index=False)
            .agg({"Jobs": "sum"})
        )

        pivot_stats = stats.pivot_table(
            index=["Domain", "company"],
            columns="isRemote",
            values="Jobs",
            aggfunc="sum",
            fill_value=0,
            margins=True,
            margins_name="Grand Total"
        ).reset_index()

        pivot_stats.columns = [str(c) for c in pivot_stats.columns]

        logging.info("📊 Uploading stats summary to Stats sheet...")
        stats_ws.clear()
        stats_ws.update([pivot_stats.columns.tolist()] + pivot_stats.values.tolist(), value_input_option="USER_ENTERED")

    # Cleanup temp files
    for f in temp_posts + temp_descs:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass

    elapsed = round(time.time() - start_time, 2)
    logging.info(
        f"🏁 Done in {elapsed}s — Uploaded {len(desc_found_df)} Desc Found, {len(desc_not_found_df)} Desc Not Found rows.")

    return {
        "status": "success",
        "desc_found_count": len(desc_found_df),
        "desc_not_found_count": len(desc_not_found_df),
        "api_updated_count": api_updated_count,
        "failed_domains": failed,
        "elapsed_seconds": elapsed,
    }


if __name__ == "__main__":
    run_topost_pipeline()
