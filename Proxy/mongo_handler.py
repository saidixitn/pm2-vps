import math
import time
import dateutil.parser
import concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Set
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure

import config


class MongoHandler:
    def __init__(self, main_mongo_uri: str = config.REMOTE_MONGO_URI):
        self.main_mongo_uri = main_mongo_uri
        self._local_client: Optional[MongoClient] = None
        self._db_clients: Dict[str, MongoClient] = {}

    def local_client(self) -> MongoClient:
        """Main cluster client for mongo_creds and domain_expires."""
        if self._local_client is None:
            if not self.main_mongo_uri:
                raise RuntimeError("REMOTE_MONGO_URI / LOCAL_MONGO_URI environment variable is not set")
            self._local_client = MongoClient(
                self.main_mongo_uri,
                socketTimeoutMS=60000,
                connectTimeoutMS=20000,
                serverSelectionTimeoutMS=30000,
                maxIdleTimeMS=45000,
            )
        return self._local_client

    def get_mongo_uri(self, domain_name: str) -> str:
        doc = self.local_client()[config.CREDS_DB]["creds"].find_one(
            {"domain": domain_name}, {"_id": 0, "mongo_uri": 1}
        )
        if doc and doc.get("mongo_uri"):
            print(f"🔐 Loaded mongo_uri for domain: {domain_name}")
            return doc["mongo_uri"]
        raise RuntimeError(f"No mongo_uri found in {config.CREDS_DB}.creds for domain: {domain_name}")

    def get_client(self, domain_name: str) -> MongoClient:
        if domain_name not in self._db_clients:
            uri = self.get_mongo_uri(domain_name)
            self._db_clients[domain_name] = MongoClient(
                uri,
                socketTimeoutMS=60000,
                connectTimeoutMS=20000,
                serverSelectionTimeoutMS=30000,
                maxIdleTimeMS=45000,
            )
        return self._db_clients[domain_name]

    def log_run(self, doc: dict) -> None:
        try:
            col = self.get_client(config.PROXY_DB)[config.PROXY_DB][config.LOG_COLLECTION]
            doc["createdAt"] = datetime.now(timezone.utc)
            col.insert_one(doc)
        except Exception as e:
            print(f"⚠ Could not write run log: {e}")

    # ================================================
    # POSTINGS WORKFLOW
    # ================================================
    def fetch_postings(
        self,
        employer_id: str,
        target_count: int,
        titles: List[str] = config.POSTINGS_TITLES_DEFAULT
    ) -> List[Tuple[str, str]]:
        """
        Full 3-step Postings workflow:
        1. Sample from ETL_AUD_Data (dynamically scaled per title).
        2. Filter out clientJobIds that already exist in prod_jobiak_ai.job.
        3. Verify scraping status in ETL_Scraping_Data for today's date (updatetime >= today UTC, job_status != 3).
        """
        print(f"\n🚀 [Postings] Starting fetch for EmployerId {employer_id} (Target: {target_count})")
        
        sample_titles_count = min(len(titles), 15)
        ids_per_title = max(10, math.ceil((target_count * 2) / sample_titles_count))

        postings_col = self.get_client(config.POSTINGS_DB)[config.POSTINGS_DB][config.POSTINGS_COLLECTION]

        pipeline = [
            {"$match": {
                "employerId": employer_id,
                "Type": config.POSTINGS_TYPE,
                "title": {"$in": titles},
            }},
            {"$sample": {"size": 50000}},
            {"$group": {"_id": "$title", "ids": {"$push": "$clientJobId"}}},
            {"$project": {
                "_id": 0,
                "title": "$_id",
                "ids": {"$slice": ["$ids", ids_per_title]},
            }},
            {"$sample": {"size": sample_titles_count}},
        ]

        groups = list(postings_col.aggregate(pipeline, allowDiskUse=True))
        print(f"📊 [Postings] Retrieved {len(groups)} title groups from ETL_AUD_Data")

        candidate_tuples: List[Tuple[str, str]] = []
        candidate_ids: List[str] = []
        seen = set()

        idx = 0
        while len(candidate_tuples) < (target_count * 2):
            added = False
            for g in groups:
                ids = g.get("ids") or []
                if idx < len(ids):
                    jid = ids[idx]
                    if jid and jid not in seen:
                        seen.add(jid)
                        candidate_tuples.append((g.get("title", ""), jid))
                        candidate_ids.append(jid)
                        added = True
            if not added:
                break
            idx += 1

        if not candidate_ids:
            print("⚠ [Postings] No candidate IDs extracted from ETL_AUD_Data")
            return []

        # Step 2: Check prod_jobiak_ai.job to remove existing clientJobIds
        job_col = self.get_client(config.JOB_DOMAIN_KEY)[config.JOB_DB][config.JOB_COLLECTION]
        existing_docs = job_col.find(
            {"employerId": employer_id, "clientJobId": {"$in": candidate_ids}},
            {"_id": 0, "clientJobId": 1}
        )
        existing_job_ids = {d["clientJobId"] for d in existing_docs if "clientJobId" in d}
        print(f"🔍 [Postings] Found {len(existing_job_ids)} existing job IDs in prod_jobiak_ai.job to exclude")

        new_candidates = [(t, jid) for t, jid in candidate_tuples if jid not in existing_job_ids]
        new_candidate_ids = [jid for _, jid in new_candidates]

        if not new_candidate_ids:
            print("⚠ [Postings] All candidate IDs already exist in prod_jobiak_ai.job")
            return []

        # Step 3: Verify in ETL_Scraping_Data (updatetime >= start of today UTC, job_status != 3)
        today_utc_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        etl_scraping_col = self.get_client(config.ETL_SCRAPING_DB)[config.ETL_SCRAPING_DB][config.ETL_SCRAPING_COLLECTION]

        scraping_docs = etl_scraping_col.find(
            {
                "clientJobId": {"$in": new_candidate_ids},
                "updatetime": {"$gte": today_utc_start},
                "job_status": {"$ne": 3}
            },
            {"_id": 0, "clientJobId": 1}
        )
        valid_scraping_ids = {d["clientJobId"] for d in scraping_docs if "clientJobId" in d}
        print(f"✅ [Postings] Verified {len(valid_scraping_ids)} scraped jobs updated today (job_status != 3)")

        final_postings = [(t, jid) for t, jid in new_candidates if jid in valid_scraping_ids]
        print(f"🎉 [Postings] Final selection: {len(final_postings[:target_count])} job postings")
        return final_postings[:target_count]

    # ================================================
    # UPDATES & DATEPOSTED WORKFLOW
    # ================================================
    def fetch_job_pool(
        self,
        employer_id: str,
        needed: int,
        exclude_ids: Optional[Set[str]] = None
    ) -> Tuple[List[str], Optional[str], List[dict]]:
        """
        Fetches job pool sorted by validThrough ascending using allowDiskUse=True.
        Supports excluding specific job IDs (e.g. for DatePosted excluding Updates/Expires).
        """
        print(f"\n🚀 [Job Pool] Querying pool for EmployerId {employer_id} (Needed: {needed})")
        col = self.get_client(config.JOB_DOMAIN_KEY)[config.JOB_DB][config.JOB_COLLECTION]

        base_match = {
            "employerId": employer_id,
            "status": "posted",
            "isActive": True,
            "datePosted": {"$exists": True},
            "title": {"$nin": config.EXCLUDED_TITLES},
        }

        if exclude_ids:
            base_match["jobId"] = {"$nin": list(exclude_ids)}

        summary_pipeline = [
            {"$match": base_match},
            {"$addFields": {"date": {
                "$dateToString": {"format": "%Y-%m-%d", "date": "$validThrough"}
            }}},
            {"$group": {"_id": {"date": "$date"}, "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "date": "$_id.date", "count": 1}},
            {"$sort": {"date": 1}},
        ]

        summary = list(col.aggregate(summary_pipeline, allowDiskUse=True))
        
        # Pick cutoff date
        total = 0
        cutoff = None
        for r in summary:
            total += r["count"]
            if total >= needed:
                cutoff = r["date"]
                break
        if not cutoff and summary:
            cutoff = summary[-1]["date"]

        if not cutoff:
            print("⚠ [Job Pool] No matching jobs found for query")
            return [], None, summary

        end_window = dateutil.parser.parse(cutoff + "T00:00:00Z") + timedelta(days=1)
        
        fetch_pipeline = [
            {"$match": {**base_match, "validThrough": {"$lt": end_window}}},
            {"$sort": {"validThrough": 1}},
            {"$limit": needed},
            {"$project": {"_id": 0, "jobId": 1}}
        ]

        cursor = col.aggregate(fetch_pipeline, allowDiskUse=True)
        job_ids = [d["jobId"] for d in cursor if d.get("jobId")]

        print(f"✅ [Job Pool] Retrieved {len(job_ids)} job IDs (Cutoff date: {cutoff})")
        return job_ids, cutoff, summary

    # ================================================
    # EXPIRES WORKFLOW (ALWAYS RUN WITH 1 WORKER PER TASK)
    # ================================================
    def build_expires_collection(
        self,
        employer_id: str = "68b8133b060e8f5df1a6c3a7",
        collection_name: str = "earnhaus_expires",
        days: int = 30,
        semrush_match_domain: str = "earnhaus.com"
    ) -> dict:
        """
        Always-on multi-threaded rebuild of domain_expires.<collection_name> collection.
        Uses 1 dedicated worker thread per fetch task (3 workers total):
        - Worker 1: Active jobs
        - Worker 2: User view analytics
        - Worker 3: Semrush whitelist URLs
        """
        print(f"\n========== STARTING EARNHAUS EXPIRY PROCESS (ALWAYS RUN) ==========")
        start_time = time.time()

        now = datetime.now(timezone.utc)
        start_date = now - timedelta(days=days)
        end_date = now

        prod_db = self.get_client(config.JOB_DOMAIN_KEY)[config.JOB_DB]

        # 1 Worker Dedicated Per Task (3 Workers Total)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            f_jobs = executor.submit(self._fetch_jobs_worker, prod_db, employer_id)
            f_views = executor.submit(self._fetch_views_worker, prod_db, employer_id, start_date, end_date)
            f_semrush = executor.submit(self._fetch_semrush_worker, semrush_match_domain)

            jobs_dict = f_jobs.result()
            viewed_ids = f_views.result()
            semrush_urls = f_semrush.result()

        all_job_ids = set(jobs_dict.keys())
        no_view_ids = all_job_ids - viewed_ids

        final_expiry_docs = []
        semrush_skipped = 0

        for jid in no_view_ids:
            job_doc = jobs_dict[jid]
            url = job_doc.get("submittedUrl")
            if url and url in semrush_urls:
                semrush_skipped += 1
                continue
            final_expiry_docs.append(job_doc)

        print(f"\nAnalysis Summary:")
        print(f"  • Total Active Jobs   : {len(all_job_ids):,}")
        print(f"  • Jobs with Views     : {len(viewed_ids):,}")
        print(f"  • Semrush Protected   : {semrush_skipped:,}")
        print(f"  ✓ Net Jobs to Expire  : {len(final_expiry_docs):,}\n")

        exp_db = self.local_client()[config.EXPIRES_DB]
        if final_expiry_docs:
            if collection_name in exp_db.list_collection_names():
                exp_db.drop_collection(collection_name)
            col = exp_db[collection_name]
            
            chunk_size = 1000
            total_docs = len(final_expiry_docs)
            print(f"💾 Saving data to local collection '{collection_name}' in batches of {chunk_size}...")

            for i in range(0, total_docs, chunk_size):
                batch = final_expiry_docs[i:i + chunk_size]
                inserted = False
                for attempt in range(1, 4):
                    try:
                        col.insert_many(batch, ordered=False)
                        inserted = True
                        break
                    except (AutoReconnect, ConnectionFailure) as err:
                        print(f"⚠ Mongo reconnect attempt {attempt} for batch {i}: {err}")
                        time.sleep(2)

                if not inserted:
                    print(f"❌ Failed to insert batch {i} to {i + len(batch)}")

            print(f"SUCCESS: Data saved to local collection '{collection_name}'.")
        else:
            print("No records matched expiry criteria.")

        elapsed = round(time.time() - start_time, 2)
        print(f"========== TOTAL RUN TIME: {elapsed:.2f}s ==========\n")
        return {
            "totalActiveJobs": len(all_job_ids),
            "jobsWithViews": len(viewed_ids),
            "semrushProtected": semrush_skipped,
            "candidates": len(final_expiry_docs),
            "buildSeconds": elapsed,
        }

    def _fetch_jobs_worker(self, prod_db, employer_id: str) -> Dict[str, dict]:
        """Worker 1: Dedicated worker for fetching active jobs."""
        print(f"  [Worker: Jobs] Querying {config.JOB_DB}.job...")
        query = {
            "employerId": employer_id,
            "status": "posted",
            "datePosted": {"$exists": True}
        }
        projection = {
            "_id": 0,
            "jobId": 1,
            "validThrough": 1,
            "submittedUrl": 1,
            "jobtitle": 1,
            "company": 1
        }
        cursor = prod_db["job"].find(query, projection)
        local_jobs = {doc["jobId"]: doc for doc in cursor if doc.get("jobId")}
        cursor.close()
        print(f"  [Worker: Jobs] Finished. Found {len(local_jobs):,} jobs.")
        return local_jobs

    def _fetch_views_worker(self, prod_db, employer_id: str, start_date: datetime, end_date: datetime) -> Set[str]:
        """Worker 2: Dedicated worker for fetching user view analytics."""
        print(f"  [Worker: Views] Querying {config.JOB_DB}.userAnalytics...")
        query = {
            "employerId": employer_id,
            "isBot": False,
            "browserType": {"$ne": "Unknown"},
            "deviceType": {"$ne": "Unknown"},
            "isFromGoogle": True,
            "createdDt": {
                "$gt": start_date,
                "$lt": end_date
            }
        }
        cursor = prod_db[config.USER_ANALYTICS_COLLECTION].find(query, {"jobId": 1})
        viewed_keys = {doc["jobId"] for doc in cursor if doc.get("jobId")}
        cursor.close()
        print(f"  [Worker: Views] Finished. Found {len(viewed_keys):,} job views.")
        return viewed_keys

    def _fetch_semrush_worker(self, match_domain: str = "earnhaus.com") -> Set[str]:
        """Worker 3: Dedicated worker for fetching Semrush whitelist URLs."""
        print(f"  [Worker: Semrush] Querying whitelist for {match_domain}...")
        uri = config.SEMRUSH_MONGO_URI or self.main_mongo_uri
        client = MongoClient(uri, socketTimeoutMS=60000, connectTimeoutMS=20000)
        try:
            query = {
                "view_exists": {"$exists": False},
                "domain": match_domain
            }
            cursor = client[config.SEMRUSH_DB_NAME][config.SEMRUSH_COLLECTION].find(query, {"url": 1})
            urls = {doc["url"] for doc in cursor if doc.get("url")}
            print(f"  [Worker: Semrush] Finished. Found {len(urls):,} whitelist URLs.")
            return urls
        finally:
            client.close()

    def fetch_expires(self, limit: int, exclude_ids: List[str], collection_name: str) -> List[str]:
        col = self.local_client()[config.EXPIRES_DB][collection_name]
        query = {"jobId": {"$nin": exclude_ids}} if exclude_ids else {}
        cursor = col.find(query, {"_id": 0, "jobId": 1}).limit(limit)
        ids = [d["jobId"] for d in cursor if d.get("jobId")]
        print(f"✅ [Expires] Fetched {len(ids)} IDs (excluded {len(exclude_ids)} update IDs)")
        return ids
