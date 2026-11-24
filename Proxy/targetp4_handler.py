"""
TargetP4 / Description-audit pipeline.

Refactor of the standalone targetp4 script into a reusable handler so it can be
driven either by the FastAPI layer (POST /targetp4) or from the CLI.

Reuses the project's existing plumbing:
  - MongoHandler.get_client(domain)  -> per-domain client resolved from mongo_creds.creds
  - SheetsHandler                    -> gspread auth + spreadsheet access
  - config                           -> all tunables / env vars
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

import config
from mongo_handler import MongoHandler
from sheets_handler import SheetsHandler

logger = logging.getLogger("targetp4")

# Columns in the 'Domains' tab that may hold the public site URL
_URL_COLUMNS = ("SiteURL", "Domain URL", "URL", "site")

EXPECTED_COLUMNS = [
    "Domain", "jobtitle", "company", "isRemote", "Jobs",
    "ATL Expand Status", "Description Status", "concatenated Jobtitle",
    "lookupKey", "employerId",
]


def api_domain_from_db(db_domain: str, domain_row: Optional[Dict[str, Any]] = None) -> str:
    """Map a *_prod DB name to the public domain used by the isRemote API."""
    if domain_row is not None:
        for col in _URL_COLUMNS:
            value = domain_row.get(col)
            if value:
                return (
                    str(value)
                    .replace("https://", "")
                    .replace("http://", "")
                    .strip()
                    .strip("/")
                )

    try:
        mapping = json.loads(config.DOMAIN_API_MAP or "{}")
    except (ValueError, TypeError):
        mapping = {}

    if db_domain in mapping:
        return mapping[db_domain]

    return db_domain.replace("_prod", ".com")


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


class TargetP4Handler:
    def __init__(
        self,
        sheets: Optional[SheetsHandler] = None,
        mongo: Optional[MongoHandler] = None,
        max_workers: Optional[int] = None,
        retries: Optional[int] = None,
    ):
        self.sheets = sheets or SheetsHandler(
            spreadsheet_id=config.TARGETP4_SPREADSHEET_ID,
            spreadsheet_name=config.TARGETP4_SPREADSHEET_NAME,
        )
        self.mongo = mongo or MongoHandler()
        self.max_workers = max_workers or config.MAX_DOMAIN_WORKERS
        self.retries = retries or config.DOMAIN_RETRIES
        # MongoHandler caches clients in a plain dict; serialize creation across threads
        self._mongo_lock = threading.Lock()

    # ------------------------------------------------------------------
    # MONGO QUERIES
    # ------------------------------------------------------------------
    def _get_db(self, domain_name: str):
        with self._mongo_lock:
            client = self.mongo.get_client(domain_name)
        client.admin.command("ping")
        return client[domain_name]

    @staticmethod
    def run_target_p4_query(db, employer_ids: List[str]) -> List[dict]:
        match_stage = {
            "employerId": {"$in": employer_ids} if employer_ids else {"$exists": True},
            "job_status": {"$ne": 3},
            "gpost": 1,
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
                    "$concat": ["$_id.jobtitle", "=", "$_id.company", "=", "$_id.employerId"]
                },
            }},
        ]
        return list(db[config.TARGETP4_COLLECTION].aggregate(pipeline, allowDiskUse=True))

    @staticmethod
    def run_description_query(db, employer_ids: List[str]) -> List[dict]:
        match_stage = {"employerId": {"$in": employer_ids} if employer_ids else {"$exists": True}}
        pipeline = [
            {"$match": match_stage},
            {"$project": {
                "_id": 0,
                "jobtitle": 1,
                "company": 1,
                "employerId": 1,
                "lookupKey": {"$concat": ["$jobtitle", "=", "$company", "=", "$employerId"]},
            }},
        ]
        return list(db[config.DESC_CHANGE_COLLECTION].aggregate(pipeline, allowDiskUse=True))

    # ------------------------------------------------------------------
    # WORKER (one domain)
    # ------------------------------------------------------------------
    def process_domain(self, domain_row: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        domain_name = str(domain_row.get("Domain", "")).strip()
        employer_field = str(domain_row.get("employerId", "") or "").strip()
        employer_ids = [x.strip() for x in employer_field.split(",") if x.strip()] if employer_field else []

        for attempt in range(1, self.retries + 1):
            try:
                db = self._get_db(domain_name)
                postings = self.run_target_p4_query(db, employer_ids)
                descs = self.run_description_query(db, employer_ids)
                desc_lookup = {d["lookupKey"] for d in descs if d.get("lookupKey")}

                for p in postings:
                    p["Domain"] = domain_name
                    p["Description Status"] = (
                        "Desc Found" if p.get("lookupKey") in desc_lookup else "Desc Not Found"
                    )
                    p["concatenated Jobtitle"] = f'"{p.get("jobtitle", "")}",'

                df_post = pd.DataFrame(postings)
                if df_post.empty:
                    df_post = pd.DataFrame(columns=EXPECTED_COLUMNS)
                df_post = clean_dataframe(df_post)

                df_desc = pd.DataFrame(descs)
                if not df_desc.empty:
                    df_desc["Domain"] = domain_name

                logger.info(f"✅ {domain_name}: {len(df_post)} targetP4, {len(df_desc)} desc rows.")
                return df_post, df_desc

            except Exception as e:
                logger.error(f"❌ {domain_name} failed (attempt {attempt}/{self.retries}): {e}")
                if attempt < self.retries:
                    time.sleep(2)

        return None, None

    # ------------------------------------------------------------------
    # isRemote API
    # ------------------------------------------------------------------
    def update_is_remote(self, final_posts: pd.DataFrame, domain_api_map: Dict[str, str]) -> Dict[str, Any]:
        """Flip isRemote TRUE -> false through the external API, then mirror it in the DataFrame."""
        stats = {"attempted": 0, "succeeded": 0, "failed": []}

        if final_posts.empty or "isRemote" not in final_posts.columns:
            return stats

        remote_true_mask = final_posts["isRemote"] == "TRUE"
        if not remote_true_mask.any():
            return stats

        remote_targets = final_posts[remote_true_mask][["Domain", "employerId"]].drop_duplicates()
        stats["attempted"] = len(remote_targets)
        logger.info(
            f"🔄 Found {len(remote_targets)} domain/employer combinations with isRemote=TRUE. Updating via API..."
        )

        for _, row in remote_targets.iterrows():
            db_domain = row["Domain"]
            emp_id = row["employerId"]
            api_domain = domain_api_map.get(db_domain, api_domain_from_db(db_domain))
            params = {"domain": api_domain, "employerId": emp_id, "isRemote": "false"}

            try:
                response = requests.get(
                    config.IS_REMOTE_API_URL, params=params, timeout=config.IS_REMOTE_API_TIMEOUT
                )
                response.raise_for_status()
                stats["succeeded"] += 1
                logger.info(f"🌐 Success: API update for Domain: {api_domain}, EmployerId: {emp_id}")
            except Exception as e:
                stats["failed"].append(f"{api_domain}:{emp_id}")
                logger.error(f"❌ API call failed for {api_domain} - {emp_id}: {e}")

        # Overwrite so Google Sheets reflects the updated status
        final_posts.loc[remote_true_mask, "isRemote"] = "FALSE"
        logger.info("✅ DataFrame updated to reflect isRemote=FALSE before sheet upload.")
        return stats

    # ------------------------------------------------------------------
    # SHEET WRITES
    # ------------------------------------------------------------------
    def _upload(self, tab_name: str, df: pd.DataFrame) -> int:
        if df.empty:
            logger.warning(f"⚠️ No rows to upload to '{tab_name}'.")
            return 0
        ws = self.sheets.get_or_create_worksheet(tab_name)
        logger.info(f"📤 Uploading {len(df)} rows to '{tab_name}'...")
        ws.clear()
        values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
        try:
            ws.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
        except TypeError:
            ws.update(values, value_input_option="USER_ENTERED")
        return len(df)

    @staticmethod
    def build_stats(final_posts: pd.DataFrame) -> pd.DataFrame:
        if final_posts.empty or "isRemote" not in final_posts.columns:
            return pd.DataFrame()

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
            margins_name="Grand Total",
        ).reset_index()
        pivot_stats.columns = [str(c) for c in pivot_stats.columns]
        return pivot_stats

    # ------------------------------------------------------------------
    # PIPELINE ENTRY POINT
    # ------------------------------------------------------------------
    def run(
        self,
        domains: Optional[List[str]] = None,
        update_is_remote: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        start_time = time.time()
        run_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        sh = self.sheets.open_spreadsheet()
        domains_ws = self.sheets.find_worksheet_case_insensitive(sh, config.TARGETP4_DOMAINS_TAB)
        if not domains_ws:
            raise RuntimeError(f"Worksheet '{config.TARGETP4_DOMAINS_TAB}' not found in the spreadsheet")

        domains_df = pd.DataFrame(domains_ws.get_all_records())
        if domains_df.empty or "Domain" not in domains_df.columns:
            raise RuntimeError(f"No domains found in the '{config.TARGETP4_DOMAINS_TAB}' tab")

        # Optional filter from the request body
        if domains:
            wanted = {d.strip().lower() for d in domains}
            domains_df = domains_df[
                domains_df["Domain"].astype(str).str.strip().str.lower().isin(wanted)
            ]
            if domains_df.empty:
                raise RuntimeError(f"None of the requested domains exist in '{config.TARGETP4_DOMAINS_TAB}': {domains}")

        domain_api_map = {
            str(row["Domain"]).strip(): api_domain_from_db(str(row["Domain"]).strip(), row)
            for _, row in domains_df.iterrows()
        }

        workers = max(1, min(len(domains_df), self.max_workers))
        logger.info(f"Spawning {workers} workers for {len(domains_df)} domains...")

        post_frames: List[pd.DataFrame] = []
        desc_frames: List[pd.DataFrame] = []
        failed: List[str] = []

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(self.process_domain, row.to_dict()): row for _, row in domains_df.iterrows()}
            for fut in as_completed(futures):
                df_post, df_desc = fut.result()
                if df_post is None:
                    failed.append(str(futures[fut]["Domain"]).strip())
                    continue
                if not df_post.empty:
                    post_frames.append(df_post)
                if df_desc is not None and not df_desc.empty:
                    desc_frames.append(df_desc)

        final_posts = pd.concat(post_frames, ignore_index=True) if post_frames else pd.DataFrame(columns=EXPECTED_COLUMNS)
        final_posts = clean_dataframe(final_posts)

        remote_stats = {"attempted": 0, "succeeded": 0, "failed": []}
        if update_is_remote and not dry_run:
            remote_stats = self.update_is_remote(final_posts, domain_api_map)
        elif update_is_remote and dry_run:
            logger.info("🧪 Dry run - skipping isRemote API calls.")

        if "Description Status" in final_posts.columns and not final_posts.empty:
            desc_found_df = final_posts[final_posts["Description Status"] == "Desc Found"].copy()
            desc_not_found_df = final_posts[final_posts["Description Status"] == "Desc Not Found"].copy()
        else:
            desc_found_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
            desc_not_found_df = pd.DataFrame(columns=EXPECTED_COLUMNS)

        for df in (desc_found_df, desc_not_found_df):
            if not df.empty:
                df.sort_values(
                    by=["Domain", "company", "Jobs"],
                    ascending=[True, True, False],
                    inplace=True,
                )

        pivot_stats = self.build_stats(final_posts)

        summary = {
            "runAtUtc": run_at_utc,
            "domainsProcessed": len(domains_df) - len(failed),
            "domainsRequested": len(domains_df),
            "failedDomains": failed,
            "descFoundRows": len(desc_found_df),
            "descNotFoundRows": len(desc_not_found_df),
            "totalRows": len(final_posts),
            "descRows": sum(len(d) for d in desc_frames),
            "isRemoteUpdates": remote_stats,
            "dryRun": dry_run,
        }

        if dry_run:
            logger.info("🧪 Dry run active - skipping Google Sheets writes.")
            summary["elapsedSeconds"] = round(time.time() - start_time, 2)
            return summary

        self._upload(config.TARGETP4_TARGET_TAB, desc_found_df)
        self._upload(config.TARGETP4_DESC_TAB, desc_not_found_df)
        self._upload(config.TARGETP4_STATS_TAB, pivot_stats)

        summary["elapsedSeconds"] = round(time.time() - start_time, 2)

        self.mongo.log_run({
            "date": datetime.today().strftime("%Y-%m-%d"),
            "type": "targetp4_export",
            "domains": [str(d).strip() for d in domains_df["Domain"].tolist()],
            "exported": {
                "descFound": len(desc_found_df),
                "descNotFound": len(desc_not_found_df),
            },
            "failedDomains": failed,
            "isRemoteUpdates": remote_stats,
            "exportDone": True,
            "elapsedSeconds": summary["elapsedSeconds"],
        })

        logger.info(
            f"🏁 Done in {summary['elapsedSeconds']}s — "
            f"{len(desc_found_df)} Desc Found, {len(desc_not_found_df)} Desc Not Found rows."
        )
        if failed:
            logger.warning(f"⚠️ Failed domains: {', '.join(failed)}")

        return summary


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TargetP4 / Description audit pipeline")
    parser.add_argument("--domains", nargs="*", help="Optional subset of domains to process")
    parser.add_argument("--no-is-remote", action="store_true", help="Skip the isRemote API updates")
    parser.add_argument("--dry-run", action="store_true", help="Run queries without writing to Google Sheets")
    parser.add_argument("--max-workers", type=int, default=config.MAX_DOMAIN_WORKERS)
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)

    handler = TargetP4Handler(max_workers=args.max_workers)
    result = handler.run(
        domains=args.domains,
        update_is_remote=not args.no_is_remote,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
