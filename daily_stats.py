from pymongo import MongoClient
from pymongo.errors import OperationFailure
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import threading
import traceback
import os
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# LOCAL CONNECTION
# =========================================================

LOCAL_MONGO_URI = os.getenv("MONGO_URI")

local_client = MongoClient(LOCAL_MONGO_URI)

# =========================================================
# DAILY STATS DB
# =========================================================

daily_stats_db = local_client["daily_stats"]

input_col = daily_stats_db["input"]
output_col = daily_stats_db["output"]

# =========================================================
# CREDS DB
# =========================================================

creds_db = local_client["mongo_creds"]
creds_col = creds_db["creds"]

# =========================================================
# CACHE
# =========================================================

DB_CACHE = {}
CLIENT_CACHE = {}
CACHE_LOCK = threading.Lock()

MAX_WORKERS = 10

# DB that holds ETL_AUD_Data (used for proxy Feed Expires)
DATA_CLEANSING_DB = "data_cleansing"
ETL_AUD_COLLECTION = "ETL_AUD_Data"

# DB that holds scraped feed jobs (used for proxy To Post).
# Note: DB name is "Scrapping" (double p), collection is "Scraping" (single p).
ETL_SCRAPING_DB = "ETL_Scrapping_Data"
ETL_SCRAPING_COLLECTION = "ETL_Scraping_Data"

# =========================================================
# DATES
# =========================================================

UTC = timezone.utc

today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = today + timedelta(days=1)
yesterday = today - timedelta(days=1)
five_days_future = today + timedelta(days=5)

# The script runs several times a day (cron) and writes TWO output docs:
#
#   1) Date = TODAY      -> only the live fields (To_Post, FeedExpiredJobs).
#                           They are snapshots taken at run time.
#
#   2) Date = YESTERDAY  -> every other metric, computed for the window
#                           [yesterday, today). The live fields are NOT
#                           touched on this doc: they were written by the
#                           runs that happened yesterday.
#
WINDOW_START = yesterday
WINDOW_END = today

TODAY_DATE = today
YESTERDAY_DATE = yesterday

# Fields that belong to the TODAY doc only
LIVE_FIELDS = {"To_Post", "FeedExpiredJobs"}

# =========================================================
# GET DATABASE (by name)
# =========================================================

def get_database_by_name(db_name):
    """
    Return a cached DB handle for db_name, connecting via the
    mongo_uri stored in mongo_creds.creds if not yet connected.
    Thread-safe.
    """
    try:
        if db_name in DB_CACHE:
            return DB_CACHE[db_name]

        with CACHE_LOCK:
            # Re-check inside the lock (another thread may have connected)
            if db_name in DB_CACHE:
                return DB_CACHE[db_name]

            creds = creds_col.find_one(
                {"domain": db_name},
                {"_id": 0, "mongo_uri": 1}
            )

            if not creds or "mongo_uri" not in creds:
                print(f"Mongo URI not found for {db_name}")
                return None

            client = MongoClient(creds["mongo_uri"], maxPoolSize=100)
            db = client[db_name]

            CLIENT_CACHE[db_name] = client
            DB_CACHE[db_name] = db

            print(f"CONNECTED : {db_name}")
            return db

    except Exception:
        print(traceback.format_exc())
        return None

# =========================================================
# GET DATABASE (by domain / domain type)
# =========================================================

def get_database(domain, domain_type):

    if domain_type == "Proxy":
        db_name = "prod_jobiak_ai"

    elif domain_type == "Direct Apply":
        db_name = "directclients_prod"

    elif domain_type == "Programmatic":
        base_domain = domain.replace("www.", "")
        base_domain = base_domain.split(".")[0]
        db_name = f"{base_domain}_prod"

    else:
        return None

    return get_database_by_name(db_name)

# =========================================================
# COLLECTIONS
# =========================================================

def get_collections(db, domain_type):

    if domain_type == "Proxy":
        return {
            "jobs": db["job"],
            "analytics": db["userAnalytics"],
            "proxy_logs": db["jobsGoogleSubmittedLog"]
        }

    return {
        "jobs": db["Target_P4_Opt"],
        "analytics": db["userAnalytics"],
        "queue": db["aj_jp_jobqueue"],
        "logs": db["aj_jp_google_logs"]
    }

# =========================================================
# HELPERS
# =========================================================

def normalize_employer_ids(employer_id):
    """
    Normalize employerId input to a list.

    Supports:
        "emp-123"
        ["emp-123", "emp-456"]

    The returned list is used consistently throughout the script.
    """
    if employer_id is None:
        return []

    if isinstance(employer_id, str):
        employer_ids = [employer_id]
    elif isinstance(employer_id, (list, tuple, set)):
        employer_ids = list(employer_id)
    else:
        employer_ids = [employer_id]

    return list(dict.fromkeys(
        str(value).strip()
        for value in employer_ids
        if value is not None and str(value).strip()
    ))


def build_employer_filter(domain_type, employer_ids):

    if domain_type == "Programmatic":
        return {}

    return {"employerId": {"$in": employer_ids}}


# Fields that may hold the company name on a job document, tried in order
COMPANY_NAME_FIELDS = ["company", "companyName", "hiringOrganization.name", "employerName"]


def resolve_company_name(input_company, cols, employer_ids):
    """
    Company name for the OUTPUT doc.
    1) use the Company from the input doc if it is filled in
    2) otherwise look at one job belonging to any supplied employer ID
    3) otherwise return "" (the field is then left out of the output)
    """
    name = (input_company or "").strip()
    if name:
        return name

    if not employer_ids:
        return ""

    try:
        projection = {"_id": 0, **{f: 1 for f in COMPANY_NAME_FIELDS}}
        doc = cols["jobs"].find_one(
            {"employerId": {"$in": employer_ids}},
            projection
        )
        if not doc:
            return ""

        for field in COMPANY_NAME_FIELDS:
            value = doc
            for part in field.split("."):
                value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    break
            if isinstance(value, str) and value.strip():
                return value.strip()
    except Exception:
        print(traceback.format_exc())

    return ""


def company_name_variants(company_name):
    """
    Same matching the old script used for ETL_AUD_Data:
    exact / lower / upper / title / trailing-space variants.
    """
    name = (company_name or "").strip()
    if not name:
        return []
    return list({name, name.lower(), name.upper(), name.title(), f"{name} "})


def get_proxy_feed_expires(company_name, employer_ids):
    """
    Proxy 'Daily Feed Expires' - restored from the old script:
    count of ETL_AUD_Data docs with Type == 0 for this company.

    Matches on company name (as the old script did). If no company
    name is available on the input doc, falls back to all supplied employer IDs.
    """
    db = get_database_by_name(DATA_CLEANSING_DB)
    if db is None:
        return 0

    variants = company_name_variants(company_name)

    if variants:
        match_filter = {"company": {"$in": variants}, "Type": 0}
    elif employer_ids:
        match_filter = {"employerId": {"$in": employer_ids}, "Type": 0}
    else:
        return 0

    return db[ETL_AUD_COLLECTION].count_documents(match_filter)


def collect_client_job_ids(collection, match_filter):
    """
    Stream clientJobId values from a collection into a set.
    Uses a cursor (not distinct) so it works for very large
    result sets without hitting the 16MB distinct limit.
    Ids are normalised to stripped strings so int/str mismatches
    between the two databases don't break the comparison.
    """
    ids = set()
    cursor = collection.find(
        match_filter,
        {"_id": 0, "clientJobId": 1},
        batch_size=5000
    )
    for doc in cursor:
        cid = doc.get("clientJobId")
        if cid is None:
            continue
        cid = str(cid).strip()
        if cid:
            ids.add(cid)
    return ids


def get_proxy_to_post(jobs, employer_ids):
    """
    Proxy 'To Post':

      A = clientJobIds in ETL_Scrapping_Data.ETL_Scraping_Data
          where employerId is any supplied employer ID and job_status != 3
          (jobs currently in the feed)

      B = clientJobIds in prod_jobiak_ai.job
          where employerId is any supplied employer ID and status in [posted, removed]
          (jobs we have already handled)

      To Post = | A - B |
    """
    if not employer_ids:
        return 0

    # ETL_Scrapping_Data needs its own credentials: the prod_jobiak_ai
    # user is not authorised on it. The URI is read from mongo_creds.creds
    # where domain == "ETL_Scrapping_Data".
    etl_db = get_database_by_name(ETL_SCRAPING_DB)
    if etl_db is None:
        print(f"TO_POST SKIPPED ({employer_ids}) : no creds for {ETL_SCRAPING_DB}")
        return None

    try:
        feed_ids = collect_client_job_ids(etl_db[ETL_SCRAPING_COLLECTION], {
            "employerId": {"$in": employer_ids},
            "job_status": {"$ne": 3}
        })
    except OperationFailure as e:
        # Don't fail the whole domain just because To_Post can't be read
        print(f"TO_POST SKIPPED ({employer_ids}) : {e.details.get('errmsg', e) if e.details else e}")
        return None

    if not feed_ids:
        return 0

    handled_ids = collect_client_job_ids(jobs, {
        "employerId": {"$in": employer_ids},
        "status": {"$in": ["posted", "removed"]}
    })

    return len(feed_ids - handled_ids)

# =========================================================
# COMPANY STATS
# =========================================================

def get_company_stats(collection, match_filter, company_field="company", employer_field="employerId"):

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": {
                    "company": {"$ifNull": [f"${company_field}", "Unknown"]},
                    "employerId": {"$ifNull": [f"${employer_field}", ""]}
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "company": "$_id.company",
                "employerId": "$_id.employerId",
                "count": 1
            }
        },
        {"$sort": {"count": -1}}
    ]

    return list(collection.aggregate(pipeline, allowDiskUse=True))

# =========================================================
# CACHE UPDATED COMPANIES
# =========================================================

def build_cache_updated_companies(source_companies, google_companies):

    cache_updated_companies = []

    source_map = {(i["company"], i["employerId"]): i["count"] for i in source_companies}
    google_map = {(i["company"], i["employerId"]): i["count"] for i in google_companies}

    for key in set(source_map) | set(google_map):
        diff = source_map.get(key, 0) - google_map.get(key, 0)
        if diff > 0:
            cache_updated_companies.append({
                "company": key[0],
                "employerId": key[1],
                "count": diff
            })

    cache_updated_companies.sort(key=lambda x: x["count"], reverse=True)
    return cache_updated_companies

# =========================================================
# REGULAR STATS
# =========================================================

def get_regular_stats(cols, employer_ids, domain_type):

    jobs = cols["jobs"]
    analytics = cols["analytics"]
    logs = cols["logs"]

    base_filter = build_employer_filter(domain_type, employer_ids)

    # Per-company breakdown arrays are only produced for Programmatic.
    include_companies = (domain_type == "Programmatic")

    def company_stats(collection, match_filter):
        return get_company_stats(collection, match_filter) if include_companies else []

    window = {"$gte": WINDOW_START, "$lt": WINDOW_END}

    # ---------------- TO POST (live, TODAY doc) ----------------
    to_post = jobs.count_documents({
        **base_filter,
        "gpost": 1,
        "job_status": {"$ne": 3}
    })

    # ---------------- POSTED JOBS ----------------
    posted_filter = {
        **base_filter,
        "action": "post",
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": window
    }
    posted_jobs = jobs.count_documents(posted_filter)
    posted_companies = company_stats(jobs, posted_filter)

    # ---------------- UPDATED JOBS ----------------
    updated_filter = {
        **base_filter,
        "action": "update",
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": window
    }
    updated_jobs = jobs.count_documents(updated_filter)
    updated_companies = company_stats(jobs, updated_filter)

    # ---------------- REPOSTS ----------------
    repost_filter = {
        **base_filter,
        "action": "repost",
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": window
    }
    reposts = jobs.count_documents(repost_filter)
    repost_companies = company_stats(jobs, repost_filter)

    # ---------------- GOOGLE URL UPDATED ----------------
    google_updated_filter = {
        **base_filter,
        "callType": "URL_UPDATED",
        "createdAt": window
    }
    google_updated_jobs = logs.count_documents(google_updated_filter)
    google_updated_companies = company_stats(logs, google_updated_filter)

    # ---------------- CACHE UPDATED ----------------
    cache_source_jobs = posted_jobs + updated_jobs + reposts
    cache_updated_jobs = max(cache_source_jobs - google_updated_jobs, 0)

    cache_updated_companies = []
    if include_companies:
        combined_source = {}
        for item in posted_companies + updated_companies + repost_companies:
            key = (item["company"], item["employerId"])
            if key not in combined_source:
                combined_source[key] = {"company": item["company"], "employerId": item["employerId"], "count": 0}
            combined_source[key]["count"] += item["count"]

        cache_updated_companies = build_cache_updated_companies(
            list(combined_source.values()),
            google_updated_companies
        )

    # ---------------- POSTED IDS / POSTED BOT ----------------
    posted_ids = jobs.distinct("jobPubJobId", {
        **base_filter,
        "gpost": 5,
        "gpost_date": window
    })

    posted_bot = len(analytics.distinct("jobPubJobId", {
        "jobPubJobId": {"$in": posted_ids},
        "isBot": True,
        "action": {"$in": ["post", "repost", "update"]},
        "createdDt": window
    }))

    # ---------------- EXPIRED JOBS ----------------
    expired_filter = {
        **base_filter,
        "callType": "URL_DELETED",
        "createdAt": window
    }
    expired_jobs = logs.count_documents(expired_filter)
    expired_companies = company_stats(logs, expired_filter)

    expired_ids = jobs.distinct("jobPubJobId", {
        **base_filter,
        "gpost": 6,
        "gpost_expire_date": window
    })

    # ---------------- EXPIRES BOT ----------------
    expires_bot = len(analytics.distinct("jobPubJobId", {
        "jobPubJobId": {"$in": expired_ids},
        "isBot": True,
        "action": "expire",
        "createdDt": window
    }))

    # ---------------- FEED EXPIRED JOBS (live, TODAY doc) ----------------
    feed_expires = jobs.count_documents({
        **base_filter,
        "job_status": 3
    })

    # ---------------- DB CLICKS ----------------
    clicks_pipeline = [
        {
            "$match": {
                **base_filter,
                "isBot": False,
                "browserType": {"$ne": "Unknown"},
                "deviceType": {"$ne": "Unknown"},
                "isFromGoogle": True,
                "createdDt": window
            }
        },
        {"$group": {"_id": {"ip": "$ipAddress", "job": "$jobPubJobId"}}},
        {"$count": "count"}
    ]
    clicks_result = list(analytics.aggregate(clicks_pipeline, allowDiskUse=True))
    db_clicks = clicks_result[0]["count"] if clicks_result else 0

    # ---------------- BOT VISITS ----------------
    bot_visits = analytics.count_documents({
        **base_filter,
        "isBot": True,
        "createdDt": window
    })

    # ---------------- QUEUES / FAILS ----------------
    posting_queue = jobs.count_documents({**base_filter, "gpost": 3, "job_status": {"$ne": 3}})
    expires_queue = jobs.count_documents({**base_filter, "gpost": 8})
    post_fail = jobs.count_documents({**base_filter, "gpost": 4, "gpostExeOn": {"$exists": True}})

    expires_fail = logs.count_documents({
        **base_filter,
        "callType": "URL_DELETED",
        "status_update": 4,
        "createdAt": window
    })

    # ---------------- TO EXPIRE IN 5 DAYS ----------------
    to_expire_5days = jobs.count_documents({
        **base_filter,
        "pub_expired": {"$lt": five_days_future},
        "gpost": 5,
        "job_status": {"$ne": 3}
    })

    # ---------------- TOTALS ----------------
    total_posted_jobs = jobs.count_documents({**base_filter, "gpost": 5, "job_status": {"$ne": 3}})
    total_expired_jobs = jobs.count_documents({**base_filter, "gpost": 6})

    result = {
        "To_Post": to_post,
        "Posted_Jobs": posted_jobs,
        "Updated_Jobs": updated_jobs,
        "Cache_Updated_Jobs": cache_updated_jobs,
        "Expired_Jobs": expired_jobs,
        "Posted_Bot": posted_bot,
        "Expires_Bot": expires_bot,
        "FeedExpiredJobs": feed_expires,
        "DB_Clicks": db_clicks,
        "Bot_Visits": bot_visits,
        "Posting_Queue": posting_queue,
        "Expires_Queue": expires_queue,
        "Post_Fail": post_fail,
        "Expires_Fail": expires_fail,
        "To_Expire_In_5Days": to_expire_5days,
        "Reposts": reposts,
        "Total_Posted_Jobs": total_posted_jobs,
        "Total_Expired_Jobs": total_expired_jobs
    }

    # Company breakdowns: Programmatic only
    if include_companies:
        result.update({
            "posted_companies": posted_companies,
            "updated_companies": updated_companies,
            "cache_updated_companies": cache_updated_companies,
            "expired_companies": expired_companies
        })

    return result

# =========================================================
# PROXY STATS
# =========================================================

def get_proxy_stats(cols, employer_ids, company_name):

    jobs = cols["jobs"]
    analytics = cols["analytics"]

    window = {"$gte": WINDOW_START, "$lt": WINDOW_END}

    # ---------------- TO POST (live, TODAY doc) ----------------
    # Feed clientJobIds (ETL_Scraping_Data, job_status != 3)
    # minus clientJobIds already posted/removed in prod_jobiak_ai.job
    to_post = get_proxy_to_post(jobs, employer_ids)

    # ---------------- CLASSIFICATION PIPELINE ----------------
    pipeline = [
        {
            "$match": {
                "employerId": {"$in": employer_ids},
                "status": "posted",
                "isActive": True,
                "datePosted": window
            }
        },
        {
            "$lookup": {
                "from": "jobsGoogleSubmittedLog",
                "let": {"jobId": "$jobId"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$jobId", "$$jobId"]},
                                    {"$eq": ["$callType", "URL_UPDATED"]}
                                ]
                            }
                        }
                    },
                    {"$project": {"_id": 1, "createdAt": 1}}
                ],
                "as": "allLogs"
            }
        },
        {
            "$addFields": {
                "todayLogs": {
                    "$filter": {
                        "input": "$allLogs",
                        "as": "log",
                        "cond": {
                            "$and": [
                                {"$gte": ["$$log.createdAt", WINDOW_START]},
                                {"$lt": ["$$log.createdAt", WINDOW_END]}
                            ]
                        }
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "job",
                "let": {"clientJobId": "$clientJobId"},
                "pipeline": [
                    {
                        "$match": {
                            "status": "removed",
                            "$expr": {"$eq": ["$clientJobId", "$$clientJobId"]}
                        }
                    },
                    {"$limit": 1}
                ],
                "as": "removedHistory"
            }
        },
        {
            "$addFields": {
                "totalLogCount": {"$size": "$allLogs"},
                "todayLogCount": {"$size": "$todayLogs"},
                "hasRemovedHistory": {"$gt": [{"$size": "$removedHistory"}, 0]}
            }
        },
        {
            "$addFields": {
                "classification": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$and": [
                                        {"$gt": ["$totalLogCount", 1]},
                                        {"$gt": ["$todayLogCount", 0]}
                                    ]
                                },
                                "then": "update"
                            },
                            {
                                "case": {
                                    "$and": [
                                        {"$eq": ["$totalLogCount", 1]},
                                        "$hasRemovedHistory",
                                        {"$gt": ["$todayLogCount", 0]}
                                    ]
                                },
                                "then": "repost"
                            },
                            {
                                "case": {
                                    "$and": [
                                        {"$eq": ["$totalLogCount", 1]},
                                        {"$eq": ["$hasRemovedHistory", False]},
                                        {"$gt": ["$todayLogCount", 0]}
                                    ]
                                },
                                "then": "posting"
                            }
                        ],
                        "default": "cache_update"
                    }
                }
            }
        }
    ]

    classified_jobs = list(jobs.aggregate(pipeline, allowDiskUse=True))

    postings = reposts = updates = cache_updates = 0

    for job in classified_jobs:
        classification = job.get("classification")

        if classification == "posting":
            postings += 1
        elif classification == "update":
            updates += 1
        elif classification == "repost":
            reposts += 1
        else:
            cache_updates += 1

    # ---------------- EXPIRED JOBS ----------------
    expired_filter = {
        "employerId": {"$in": employer_ids},
        "status": "removed",
        "isActive": False,
        "updatedAt": window
    }
    expired_jobs = jobs.count_documents(expired_filter)
    expired_ids = jobs.distinct("jobId", expired_filter)

    # ---------------- EXPIRES BOT ----------------
    expires_bot = len(analytics.distinct("childJobId", {
        "childJobId": {"$in": expired_ids},
        "isBot": True,
        "country": "United States",
        "createdDt": window
    }))

    # ---------------- POSTED IDS / POSTED BOT ----------------
    posted_ids = jobs.distinct("jobId", {
        "employerId": {"$in": employer_ids},
        "status": "posted",
        "isActive": True,
        "datePosted": window
    })

    posted_bot = len(analytics.distinct("childJobId", {
        "childJobId": {"$in": posted_ids},
        "isBot": True,
        "country": "United States",
        "createdDt": window
    }))

    # ---------------- FEED EXPIRED JOBS (live, TODAY doc) ----------------
    # Restored from old script: ETL_AUD_Data (data_cleansing) with Type == 0
    feed_expires = get_proxy_feed_expires(company_name, employer_ids)

    # ---------------- DB CLICKS ----------------
    clicks_pipeline = [
        {
            "$match": {
                "employerId": {"$in": employer_ids},
                "isBot": False,
                "browserType": {"$ne": "Unknown"},
                "deviceType": {"$ne": "Unknown"},
                "isFromGoogle": True,
                "createdDt": window
            }
        },
        {"$group": {"_id": {"ip": "$ipAddress", "job": "$jobId"}}},
        {"$count": "count"}
    ]
    clicks_result = list(analytics.aggregate(clicks_pipeline, allowDiskUse=True))
    db_clicks = clicks_result[0]["count"] if clicks_result else 0

    # ---------------- BOT VISITS ----------------
    bot_visits = analytics.count_documents({
        "employerId": {"$in": employer_ids},
        "isBot": True,
        "createdDt": window
    })

    # ---------------- TOTALS ----------------
    total_posted_jobs = jobs.count_documents({
        "employerId": {"$in": employer_ids},
        "status": "posted",
        "isActive": True
    })

    total_expired_jobs = jobs.count_documents({
        "employerId": {"$in": employer_ids},
        "status": "removed"
    })

    # ---------------- TO EXPIRE IN 5 DAYS ----------------
    to_expire_5days = jobs.count_documents({
        "employerId": {"$in": employer_ids},
        "status": "posted",
        "isActive": True,
        "validThrough": {"$lt": five_days_future}
    })

    return {
        "To_Post": to_post,
        "Posted_Jobs": postings,
        "Updated_Jobs": updates,
        "Cache_Updated_Jobs": cache_updates,
        "Expired_Jobs": expired_jobs,
        "Posted_Bot": posted_bot,
        "Expires_Bot": expires_bot,
        "FeedExpiredJobs": feed_expires,
        "DB_Clicks": db_clicks,
        "Bot_Visits": bot_visits,
        "To_Expire_In_5Days": to_expire_5days,
        "Reposts": reposts,
        "Total_Posted_Jobs": total_posted_jobs,
        "Total_Expired_Jobs": total_expired_jobs
    }

# =========================================================
# SAVE OUTPUT (two docs: TODAY + YESTERDAY)
# =========================================================

def save_output(domain, domain_type, company_name, employer_ids, stats):

    generated_at = datetime.now(UTC)

    common = {
        "Domain": domain,
        "DomainType": domain_type,
        "employerId": employer_ids,
    }
    # Only write Company when we actually have a name
    if company_name:
        common["Company"] = company_name

    live_stats = {k: v for k, v in stats.items() if k in LIVE_FIELDS}
    daily_stats = {k: v for k, v in stats.items() if k not in LIVE_FIELDS}

    # -------- TODAY doc: only To_Post + FeedExpiredJobs --------
    # Every run of the day overwrites these two with the latest snapshot.
    output_col.update_one(
        {"Domain": domain, "employerId": employer_ids, "Date": TODAY_DATE},
        {
            "$set": {
                **common,
                "Date": TODAY_DATE,
                "LiveGeneratedAt": generated_at,
                **live_stats
            }
        },
        upsert=True
    )

    # -------- YESTERDAY doc: everything else --------
    # To_Post / FeedExpiredJobs are NOT written here, so the values
    # written by yesterday's runs are preserved. If yesterday's doc did
    # not exist yet, those two fields are simply absent (no null).
    output_col.update_one(
        {"Domain": domain, "employerId": employer_ids, "Date": YESTERDAY_DATE},
        {
            "$set": {
                **common,
                "Date": YESTERDAY_DATE,
                "WindowStart": WINDOW_START,
                "WindowEnd": WINDOW_END,
                "GeneratedAt": generated_at,
                **daily_stats
            }
        },
        upsert=True
    )

# =========================================================
# PROCESS DOMAIN
# =========================================================

def process_domain(doc):

    try:
        domain = doc.get("Domain")
        domain_type = doc.get("DomainType")
        employer_ids = normalize_employer_ids(doc.get("employerId", []))
        input_company = str(doc.get("Company", "") or "").strip()

        print(f"\nPROCESSING : {domain}")
        print(f"EMPLOYER IDs : {employer_ids}")

        # Avoid running unfiltered Proxy / Direct Apply queries if IDs are missing.
        if domain_type in ("Proxy", "Direct Apply") and not employer_ids:
            print(f"SKIPPED : No employerId found for {domain}")
            return False

        db = get_database(domain, domain_type)
        if db is None:
            print("DB NOT FOUND")
            return False

        cols = get_collections(db, domain_type)

        if domain_type == "Proxy":
            # Feed Expires keeps matching on the INPUT company (old-script
            # behaviour); with a blank input company it falls back to employerId.
            stats = get_proxy_stats(cols, employer_ids, input_company)
        else:
            stats = get_regular_stats(cols, employer_ids, domain_type)

        # Company name for the output doc only
        company_name = resolve_company_name(input_company, cols, employer_ids)

        save_output(domain, domain_type, company_name, employer_ids, stats)

        print(f"DONE : {domain}")
        return True

    except Exception:
        print(traceback.format_exc())
        return False

# =========================================================
# MAIN
# =========================================================

def main():

    docs = list(input_col.find({}))
    print(f"TOTAL DOMAINS  : {len(docs)}")
    print(f"TODAY DOC      : {TODAY_DATE.date()}  -> {', '.join(sorted(LIVE_FIELDS))} (live snapshot)")
    print(f"YESTERDAY DOC  : {YESTERDAY_DATE.date()}  -> all other metrics (window {WINDOW_START} -> {WINDOW_END})")

    success = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_domain, d) for d in docs]
        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                failed += 1

    print("\n====================================")
    print(f"SUCCESS : {success}")
    print(f"FAILED  : {failed}")
    print("====================================")

    for client in CLIENT_CACHE.values():
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
