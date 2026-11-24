import pymongo
from datetime import datetime, timedelta, timezone
import sys
import re
import concurrent.futures
import time
import os
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
LOCAL_MONGO_URI = os.getenv("MONGO_URI")
CREDS_DB = "mongo_creds"

DOMAIN = "directclients_prod"      # source (prod) database
TARGET_DB = DOMAIN
LOCAL_DB = "domain_expires"        # local db: config in, results out

CONFIG_COLLECTION = "config_domains"
JOBS_COLLECTION = "Target_P4_Opt"
SEMRUSH_COLLECTION = "semrush_url's"
SEMRUSH_URL_FIELD = "url"
SEMRUSH_DOMAIN_FIELD = "domain"

# Analytics sources.
#   date_filter=True  -> only views inside the DAYS window count
#   date_filter=False -> every view in the collection counts
# Both default to False, matching the queries as written. False is the
# conservative choice: more URLs count as "viewed", so fewer jobs get
# flagged expired.
ANALYTICS_SOURCES = [
    {"name": "userAnalytics", "date_filter": False},
    {"name": "userAnalytics_old", "date_filter": False},
]

# Applied only if the collection actually carries the field.
VIEW_FILTERS = {
    "isBot": False,
    "browserType": {"$ne": "Unknown"},
    "deviceType": {"$ne": "Unknown"},
    "isFromGoogle": True,
}
DATE_FIELD_CANDIDATES = ["createdDt", "viewDate"]

# Extra job-side filters. The query you supplied uses only employerId +
# gpost_date, so this is empty. Uncomment to restore the older behaviour.
EXTRA_JOB_FILTERS = {
    # "job_status": {"$ne": "3.0"},
    # "gpost": 5.0,
}

JOB_PROJECTION = {
    "_id": 0,
    "submittedUrl": 1,
    "gpost": 1,
    "gpost_date": 1,
    "jobtitle": 1,
    "location": 1,
    "employerId": 1,
}

NORMALIZE_URLS = True   # case / trailing-slash insensitive matching
DAYS = 30
CHUNK_SIZE = 10_000
MAX_WORKERS = 10


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def normalize_url(url):
    """Comparable form of a URL, or None if unusable."""
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u:
        return None
    if NORMALIZE_URLS:
        u = u.rstrip("/").lower()
    return u or None


def _squash(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def resolve_collection(db, wanted, contains=None):
    """
    Find the real collection name, tolerating quote/case/underscore drift
    (e.g. straight vs curly apostrophe in "semrush_url's").
    """
    try:
        names = db.list_collection_names()
    except Exception as e:
        print(f"    ! Could not list collections on {db.name}: {e}")
        return wanted

    if wanted in names:
        return wanted

    target = _squash(wanted)
    for n in names:
        if _squash(n) == target:
            print(f"    ! Name mismatch: using '{n}' instead of '{wanted}'")
            return n

    if contains:
        matches = [n for n in names if contains.lower() in n.lower()]
        if len(matches) == 1:
            print(f"    ! Fuzzy match: using '{matches[0]}' for '{wanted}'")
            return matches[0]
        if len(matches) > 1:
            print(f"    ! Ambiguous candidates: {matches} - set the exact name in CONFIG.")
            return None

    print(f"    ! '{wanted}' not found in {db.name}. Existing collections:")
    for n in sorted(names):
        print(f"        - {n}")
    return None


def field_exists(coll, field):
    try:
        return coll.count_documents({field: {"$exists": True}}, limit=1) > 0
    except Exception:
        return False


def default_collection_name(cfg):
    """apexfocusgroup.com -> apexfocusgroup_expires"""
    if cfg.get("expiryCollection"):
        return cfg["expiryCollection"]
    base = cfg.get("SemrushDomain") or cfg.get("domain") or cfg["employerId"]
    label = base.split(".")[0]
    return f"{re.sub(r'[^a-zA-Z0-9]+', '_', label).strip('_').lower()}_expires"


# --------------------------------------------------
# STEP 1: LOAD CONFIG
# --------------------------------------------------

def load_configs(export_db):
    """Read domain_expires.config_domains."""
    print(f"STEP 1: Loading {LOCAL_DB}.{CONFIG_COLLECTION}...")

    real_name = resolve_collection(export_db, CONFIG_COLLECTION)
    if not real_name:
        sys.exit("Error: config collection not found. Nothing to do.")

    configs = []
    for doc in export_db[real_name].find({}):
        emp = doc.get("employerId")
        if not emp:
            print(f"  ! Skipping config without employerId: {doc.get('_id')}")
            continue
        cfg = {
            "employerId": emp,
            "domain": doc.get("domain"),
            "SemrushDomain": doc.get("SemrushDomain"),
            "expiryCollection": doc.get("expiryCollection"),
        }
        cfg["output"] = default_collection_name(cfg)
        configs.append(cfg)

    if not configs:
        sys.exit("Error: config_domains is empty.")

    for c in configs:
        print(f"  • {c['employerId']:<28} semrush={c['SemrushDomain'] or '-':<22} -> {c['output']}")
    return configs


# --------------------------------------------------
# WORKERS
# --------------------------------------------------

def fetch_jobs(db, employer_ids, start_date, end_date):
    """Jobs posted in the window, keyed employerId -> {norm_url: doc}."""
    tag = "  [Worker: Jobs]"
    print(f"{tag} Querying {JOBS_COLLECTION}...")

    match = {
        "employerId": {"$in": employer_ids},
        "gpost_date": {"$gt": start_date, "$lt": end_date},
        "gpost" : 5
    }
    match.update(EXTRA_JOB_FILTERS)

    by_employer = {e: {} for e in employer_ids}
    cursor = db[JOBS_COLLECTION].find(match, JOB_PROJECTION)

    total = 0
    for doc in cursor:
        key = normalize_url(doc.get("submittedUrl"))
        if not key:
            continue
        emp = doc.get("employerId")
        if emp in by_employer:
            by_employer[emp][key] = doc
            total += 1
    cursor.close()

    print(f"{tag} Finished. {total:,} jobs across {len(employer_ids)} employers.")
    for emp, jobs in by_employer.items():
        print(f"{tag}   {emp}: {len(jobs):,} unique URLs")
    return by_employer


def fetch_views(db, source, employer_ids, start_date, end_date):
    """Viewed URLs from one analytics collection, keyed employerId -> set."""
    name = source["name"]
    apply_date = source.get("date_filter", False)
    tag = f"  [Worker: Views/{name}]"
    print(f"{tag} Querying (date_filter={apply_date})...")

    viewed = {e: set() for e in employer_ids}

    real_name = resolve_collection(db, name)
    if not real_name:
        print(f"{tag} Skipped - collection unavailable.")
        return viewed

    coll = db[real_name]

    # NOTE: equality, not $gt. A $gt string comparison would silently drop the
    # first employer and pull in unrelated ones.
    match = {"employerId": {"$in": employer_ids}}

    skipped = []
    for field, condition in VIEW_FILTERS.items():
        if field_exists(coll, field):
            match[field] = condition
        else:
            skipped.append(field)
    if skipped:
        print(f"{tag} Fields absent, filter relaxed: {', '.join(skipped)}")

    if apply_date:
        date_field = next((f for f in DATE_FIELD_CANDIDATES if field_exists(coll, f)), None)
        if not date_field:
            print(f"{tag} No usable date field - running without a date window.")
        else:
            sample = coll.find_one({date_field: {"$exists": True, "$ne": None}}, {date_field: 1})
            value = sample.get(date_field) if sample else None
            if isinstance(value, datetime):
                match[date_field] = {"$gt": start_date, "$lt": end_date}
            else:
                print(f"{tag} '{date_field}' stored as {type(value).__name__}; casting via $toDate.")
                match["$expr"] = {
                    "$and": [
                        {"$gt": [{"$toDate": f"${date_field}"}, start_date]},
                        {"$lt": [{"$toDate": f"${date_field}"}, end_date]},
                    ]
                }

    pipeline = [
        {"$match": match},
        {"$group": {"_id": {"emp": "$employerId", "url": "$submittedUrl"}}},
    ]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)

    for doc in cursor:
        gid = doc["_id"]
        key = normalize_url(gid.get("url"))
        emp = gid.get("emp")
        if key and emp in viewed:
            viewed[emp].add(key)
    cursor.close()

    total = sum(len(v) for v in viewed.values())
    if not total:
        docs = coll.estimated_document_count()
        print(f"{tag} 0 URLs matched. Collection holds ~{docs:,} docs.")
        probe = coll.find_one({}, {"_id": 0})
        if probe:
            print(f"{tag}   sample fields: {sorted(probe.keys())}")

    print(f"{tag} Finished. {total:,} unique viewed URLs.")
    for emp, urls in viewed.items():
        print(f"{tag}   {emp}: {len(urls):,}")
    return viewed


def fetch_semrush(export_db, semrush_domains):
    """Semrush URLs keyed SemrushDomain -> set. None means unavailable."""
    tag = "  [Worker: Semrush]"
    print(f"{tag} Querying {LOCAL_DB}.{SEMRUSH_COLLECTION}...")

    real_name = resolve_collection(export_db, SEMRUSH_COLLECTION, contains="semrush")
    if not real_name:
        print(f"{tag} ABORTING - exclusion list unavailable.")
        return None

    coll = export_db[real_name]
    by_domain = {d: set() for d in semrush_domains}

    cursor = coll.find(
        {SEMRUSH_DOMAIN_FIELD: {"$in": list(semrush_domains)}},
        {"_id": 0, SEMRUSH_URL_FIELD: 1, SEMRUSH_DOMAIN_FIELD: 1},
    )

    total = 0
    for doc in cursor:
        total += 1
        dom = doc.get(SEMRUSH_DOMAIN_FIELD)
        key = normalize_url(doc.get(SEMRUSH_URL_FIELD))
        if key and dom in by_domain:
            by_domain[dom].add(key)
    cursor.close()

    print(f"{tag} Finished. Read {total:,} docs.")
    for dom, urls in by_domain.items():
        flag = "" if urls else "   <-- nothing found for this domain"
        print(f"{tag}   {dom}: {len(urls):,} URLs{flag}")
    return by_domain


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    print("\n========== CONFIG-DRIVEN EXPIRY SYNC STARTING ==========\n")
    start_time = time.time()

    now = datetime.now(timezone.utc)
    START_DATE = now - timedelta(days=DAYS)
    END_DATE = now
    print(f"Window: {START_DATE:%Y-%m-%d} -> {END_DATE:%Y-%m-%d} ({DAYS} days)\n")

    # Connections
    local_client = pymongo.MongoClient(LOCAL_MONGO_URI)
    export_db = local_client[LOCAL_DB]

    creds = local_client[CREDS_DB]["creds"].find_one({"domain": DOMAIN}, {"mongo_uri": 1})
    if not creds:
        sys.exit(f"Error: Credentials for '{DOMAIN}' not found in {CREDS_DB}.creds")

    prod_db = pymongo.MongoClient(creds["mongo_uri"])[TARGET_DB]

    # 1. Config
    configs = load_configs(export_db)
    employer_ids = [c["employerId"] for c in configs]
    semrush_domains = {c["SemrushDomain"] for c in configs if c.get("SemrushDomain")}

    # 2. Parallel fetch
    print(f"\nSTEP 2: Fetching with {MAX_WORKERS} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        f_jobs = executor.submit(fetch_jobs, prod_db, employer_ids, START_DATE, END_DATE)
        f_views = {
            src["name"]: executor.submit(fetch_views, prod_db, src, employer_ids,
                                         START_DATE, END_DATE)
            for src in ANALYTICS_SOURCES
        }
        f_semrush = executor.submit(fetch_semrush, export_db, semrush_domains)

        jobs_by_emp = f_jobs.result()
        views_by_source = {n: f.result() for n, f in f_views.items()}
        semrush_by_domain = f_semrush.result()

    # Guard: never write a list that skipped the exclusion step
    if semrush_by_domain is None:
        sys.exit("\nABORTED: semrush exclusion list unreadable. Nothing written.\n")

    # 3. Per-domain filter + write
    print("\nSTEP 3: Filtering and writing...")
    grand_total = 0

    for cfg in configs:
        emp = cfg["employerId"]
        out = cfg["output"]
        sem_dom = cfg.get("SemrushDomain")

        jobs = jobs_by_emp.get(emp, {})
        all_urls = set(jobs.keys())

        viewed = set()
        per_source = {}
        for src_name, mapping in views_by_source.items():
            urls = mapping.get(emp, set())
            per_source[src_name] = urls
            viewed |= urls

        semrush = semrush_by_domain.get(sem_dom, set()) if sem_dom else set()
        remaining = all_urls - viewed - semrush

        print(f"\n  === {emp}  ({cfg.get('domain')}) ===")
        print(f"    Total job URLs          : {len(all_urls):,}")
        for src_name, urls in per_source.items():
            print(f"    Viewed in {src_name:<18}: {len(urls):,}")
        print(f"    Viewed (combined)       : {len(viewed):,}")
        print(f"    Semrush URLs ({sem_dom}) : {len(semrush):,}")
        print(f"    Removed by views        : {len(all_urls & viewed):,}")
        print(f"    Removed by semrush      : {len(all_urls & semrush):,}")
        print(f"    Removed by both         : {len(all_urls & viewed & semrush):,}")
        print(f"    Remaining (expired)     : {len(remaining):,}")

        docs = []
        for url in remaining:
            doc = dict(jobs[url])
            doc["domain"] = cfg.get("domain")
            doc["SemrushDomain"] = sem_dom
            doc["syncedAt"] = now
            docs.append(doc)

        export_db[out].drop()
        if not docs:
            print(f"    -> {out}: 0 records.")
            continue

        for i in range(0, len(docs), CHUNK_SIZE):
            export_db[out].insert_many(docs[i:i + CHUNK_SIZE])

        print(f"    -> {out}: written {len(docs):,} records.")
        grand_total += len(docs)

    elapsed = time.time() - start_time
    print(f"\n========== {grand_total:,} RECORDS IN {elapsed:.2f} SECONDS ==========\n")