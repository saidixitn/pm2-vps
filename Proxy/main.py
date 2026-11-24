import sys
import io
import time
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Tuple

# Safe print handling for CLI, web servers, and background tasks
def safe_print(*args, **kwargs):
    try:
        msg = " ".join(str(a) for a in args) + kwargs.get("end", "\n")
        if sys.stdout and not sys.stdout.closed:
            sys.stdout.write(msg)
            sys.stdout.flush()
        else:
            sys.__stdout__.write(msg)
            sys.__stdout__.flush()
    except Exception:
        pass

print = safe_print

import config
from sheets_handler import SheetsHandler
from mongo_handler import MongoHandler


def run_pipeline_for_domain(
    sheets: SheetsHandler,
    mongo: MongoHandler,
    employer_id: str,
    company: str,
    domain: str,
    counts: Dict[str, int],
    rebuild_expires: bool = True,
    dry_run: bool = False,
):
    print(f"\n==================================================")
    print(f"🚀 Processing domain: {company} ({employer_id})")
    print(f"📋 Requested Counts: {counts}")
    print(f"==================================================")

    today_str = datetime.today().strftime("%Y-%m-%d")
    run_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()

    update_ids: List[str] = []
    expire_ids: List[str] = []
    date_posted_ids: List[str] = []
    postings_tuples: List[Tuple[str, str]] = []

    cutoff_date = None
    expires_build_stats = None

    wants_updates = counts.get("updates", 0) > 0
    wants_expires = counts.get("expires", 0) > 0
    wants_date_posted = counts.get("datePosted", 0) > 0
    wants_postings = counts.get("postings", 0) > 0

    # ----------------------------------------------------
    # STEP 1: FETCH UPDATES
    # ----------------------------------------------------
    if wants_updates:
        n_updates = counts["updates"]
        job_pool, cutoff_date, _ = mongo.fetch_job_pool(employer_id, n_updates)
        update_ids = job_pool[:n_updates]
        print(f"✅ [Updates] Selected {len(update_ids)} job IDs")

    # ----------------------------------------------------
    # STEP 2: FETCH EXPIRES (EXCLUDING UPDATE_IDS)
    # ----------------------------------------------------
    if wants_expires:
        coll_name = f"{domain.replace('.', '_')}_expires"
        if rebuild_expires:
            expires_build_stats = mongo.build_expires_collection(employer_id, coll_name, days=30)

        expire_ids = mongo.fetch_expires(counts["expires"], update_ids, coll_name)

    # ----------------------------------------------------
    # STEP 3: FETCH DATEPOSTED (EXCLUDING UPDATE_IDS AND EXPIRE_IDS)
    # ----------------------------------------------------
    if wants_date_posted:
        excluded_set = set(update_ids) | set(expire_ids)
        needed_dp = counts["datePosted"]

        # Fetch jobs from pool excluding Updates and Expires
        date_posted_ids, dp_cutoff, _ = mongo.fetch_job_pool(employer_id, needed_dp, exclude_ids=excluded_set)
        if not cutoff_date:
            cutoff_date = dp_cutoff
        print(f"✅ [DatePosted] Selected {len(date_posted_ids)} job IDs (excluding Updates & Expires)")

    # ----------------------------------------------------
    # STEP 4: FETCH POSTINGS
    # ----------------------------------------------------
    if wants_postings:
        postings_tuples = mongo.fetch_postings(employer_id, counts["postings"])

    # ----------------------------------------------------
    # MUTUAL EXCLUSION CHECK (VERIFICATION)
    # ----------------------------------------------------
    up_set = set(update_ids)
    exp_set = set(expire_ids)
    dp_set = set(date_posted_ids)

    overlap_up_exp = up_set & exp_set
    overlap_up_dp = up_set & dp_set
    overlap_exp_dp = exp_set & dp_set

    if overlap_up_exp or overlap_up_dp or overlap_exp_dp:
        print(f"❌ CRITICAL ERROR: ID Overlap Detected!")
        print(f"   Updates vs Expires Overlap: {len(overlap_up_exp)}")
        print(f"   Updates vs DatePosted Overlap: {len(overlap_up_dp)}")
        print(f"   Expires vs DatePosted Overlap: {len(overlap_exp_dp)}")
        raise RuntimeError("Mutual exclusion violation detected!")
    else:
        print(f"🛡 [Mutual Exclusion Verified] Zero overlap between Updates, Expires, and DatePosted.")

    # ----------------------------------------------------
    # STEP 5: WRITE TO GOOGLE SHEETS
    # ----------------------------------------------------
    if dry_run:
        print("\n🧪 Dry Run active - Skipping Google Sheets write.")
        print(f"   Sample Updates: {update_ids[:3]}")
        print(f"   Sample Expires: {expire_ids[:3]}")
        print(f"   Sample DatePosted: {date_posted_ids[:3]}")
        print(f"   Sample Postings: {postings_tuples[:3]}")
        return

    print("\n💾 Writing extracted IDs directly into Google Sheets tabs...")

    if wants_postings:
        sheets.write_tab("postings", ["clientJobId"], [[jid] for t, jid in postings_tuples])
    if wants_updates:
        sheets.write_tab("updates", ["jobId"], [[jid] for jid in update_ids])
    if wants_expires:
        sheets.write_tab("expires", ["jobId"], [[jid] for jid in expire_ids])
    if wants_date_posted:
        sheets.write_tab("dateposted", ["jobId"], [[jid] for jid in date_posted_ids])

    # Append run logs to sheet
    log_rows = []
    if wants_postings:
        log_rows.append([run_at_utc, company, employer_id, "Postings", counts["postings"], len(postings_tuples), ""])
    if wants_updates:
        log_rows.append([run_at_utc, company, employer_id, "Updates", counts["updates"], len(update_ids), f"cutoff={cutoff_date}"])
    if wants_expires:
        log_rows.append([run_at_utc, company, employer_id, "Expires", counts["expires"], len(expire_ids), f"rebuilt={rebuild_expires}"])
    if wants_date_posted:
        log_rows.append([run_at_utc, company, employer_id, "DatePosted", counts["datePosted"], len(date_posted_ids), ""])

    sheets.write_run_log(log_rows)

    # ----------------------------------------------------
    # STEP 6: LOG TO MONGODB
    # ----------------------------------------------------
    mongo.log_run({
        "EmployerId": employer_id,
        "Company": company,
        "date": today_str,
        "type": "sheet_export",
        "requested": counts,
        "exported": {
            "postings": len(postings_tuples),
            "updates": len(update_ids),
            "expires": len(expire_ids),
            "datePosted": len(date_posted_ids),
        },
        "cutoff": cutoff_date,
        "expiresBuild": expires_build_stats,
        "exportDone": True,
        "elapsedSeconds": round(time.time() - t0, 2),
    })

    print(f"🎉 Domain {company} completed successfully in {round(time.time() - t0, 2)} seconds!")


def main():
    parser = argparse.ArgumentParser(description="Google for Jobs Automated Daily Pipeline")
    parser.add_argument("--sheet-id", default=config.SPREADSHEET_ID, help="Google Sheet ID")
    parser.add_argument("--config-tab", default="domain details", help="Configuration sheet tab name")
    parser.add_argument("--no-rebuild-expires", action="store_true", help="Skip rebuilding the expires collection")
    parser.add_argument("--dry-run", action="store_true", help="Perform dry run without writing to Google Sheets")
    args = parser.parse_args()

    print("⚡ Starting Google for Jobs Daily Automation Pipeline...")

    sheets = SheetsHandler(spreadsheet_id=args.sheet_id)
    mongo = MongoHandler()

    # Load dynamic config directly from Google Sheet 'domain details' tab
    config_data = sheets.read_config_sheet(sheet_name=args.config_tab)

    emp_id = config_data.get("EmployerId", "68b8133b060e8f5df1a6c3a7")
    company = config_data.get("Company", "Earn Haus")
    domain = config_data.get("Domain", "earnhaus")
    counts = config_data.get("counts", {"postings": 1000, "updates": 1000, "expires": 500, "datePosted": 5000})

    run_pipeline_for_domain(
        sheets=sheets,
        mongo=mongo,
        employer_id=emp_id,
        company=company,
        domain=domain,
        counts=counts,
        rebuild_expires=not args.no_rebuild_expires,
        dry_run=args.dry_run,
    )

    print("\n✅ All domains processed and Google Sheets updated successfully!")


if __name__ == "__main__":
    main()
