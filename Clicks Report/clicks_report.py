"""
Daily Clicks Report — Flask API + Dashboard (single file)
=========================================================

Run the server:
    python clicks_api.py                          # listens on 0.0.0.0:8000
    PORT=9000 python clicks_api.py                # custom port
    API_KEY=secret123 python clicks_api.py        # require ?key= / X-API-Key
    REPORT_TZ=UTC python clicks_api.py            # clock in labels (default IST)
    SHEET_STATS=false python clicks_api.py        # never write stats to the sheet

Endpoints (all accept the key via X-API-Key header or ?key=...).
Every `date` parameter accepts YYYY-MM-DD, `today` or `yesterday`
(blank = yesterday, UTC).

  GET  /                dashboard UI (open as /?key=YOURKEY when auth is on)
  GET  /health          liveness check
  GET  /teams           teams from the sheet: lead, members, domains
  GET  /summary         overview analytics for the dashboard, served from
                        the locally stored daily stats (instant — does NOT
                        touch the remote domain databases)
        date  YYYY-MM-DD|today|yesterday · team (blank/all = everyone)
  GET|POST /run         execute the report: store stats, write the Google
                        Sheet, send emails
        date     YYYY-MM-DD|today|yesterday   default: yesterday (UTC)
        teams    Dixit,Ravi    default: all teams (also accepts "all")
        email    true|false    default true  — false = no emails at all
        sheet    true|false    default true  — write stats to the sheet
        dry_run  true|false    default false — build only; no emails, no
                               stats, no sheet
        to       a@x.com,...   redirect ALL emails (testing)
        mode     sync|async    async returns job_id; poll /jobs/<id>
  GET|POST /stats       same as /run with email=false (stats + sheet only)
  GET  /jobs/<job_id>   status + LIVE PROGRESS + result of an async job
  GET  /jobs            recent jobs (report runs and preview refreshes)
  GET  /preview         preview PAGE: Today / Yesterday chips, team tabs,
                        Refresh button. Loads instantly from the stored
                        preview cache; if nothing is cached it starts a
                        refresh job and shows a skeleton + live progress
        date  YYYY-MM-DD|today|yesterday (default yesterday)
        team  (team name, blank = first team, team=all -> every team)
        raw   true -> build synchronously and return the bare email HTML
  GET  /preview/data    JSON: cached previews for date+team (html inline)
  GET|POST /preview/refresh   start an async job that re-runs the report
                        for date+team, re-renders and stores the previews
                        (never sends, never writes stats); returns job_id
  GET  /diagnose        per-line health for a date (read-only)

Cron examples:
    30 6 * * *  curl -s "http://localhost:8000/run?key=secret123"
    30 15 * * * curl -s "http://localhost:8000/run?date=today&key=secret123"
    30 6 * * *  curl -s "http://localhost:8000/stats?key=secret123"        # no emails
    30 6 * * *  curl -s "http://localhost:8000/run?teams=Dixit&mode=async&key=secret123"

WHAT'S IN THIS VERSION
----------------------
Per-employer lines
  - A report "line" is a domain PLUS the EmployerId(s) it is filtered by
    (Key = "domain@emp1,emp2"; a row with no EmployerId keeps Key = domain).
    Two sheet rows with the same domain but different EmployerIds are two
    separate lines with their own clicks, history, chart, comparison,
    failures and stored stats.
  - The same line listed under several teams is fetched ONCE and shared,
    so it can never be double-counted.
  - Stored stats, the as-of snapshot and /summary are keyed by Key.

Google Sheet stats
  - Every real run writes one row per (team, line) to the sheet:
      * tab "Master Stats" (MASTER_STATS_TAB env)
          Date | Team | Domain | Employer ID | Type | Clicks | Unique Clicks | Postings | Expires
      * one tab per team named after the lead (e.g. "Dixit")
          Date | Domain | Employer ID | Type | Clicks | Unique Clicks | Postings | Expires
  - Tabs are created and formatted (bold frozen header) on first use.
  - Newest date on top. Re-running a date replaces that date's rows
    (matched on Date + Team + Domain + Employer ID) — no duplicates.
  - Numbers are real numbers. Employer ID / Postings / Expires are blank
    when not applicable. Failed lines are left out (never a fake 0).
  - email=false (or /stats) runs everything except the emails.

Preview cache + refresh
  - Every rendered team email (charts inlined) is stored in
    daily_domain_stats.previews keyed by (Date, Team). /preview reads the
    cache; a cache miss or Refresh starts an async job with live progress.

Time-aware "yesterday" comparison
  - Running for TODAY: every comparison is like-for-like at the same clock
    time. Running for any older date: whole previous day.
  - REPORT_TZ (env, default Asia/Kolkata -> "IST") controls only the clock
    printed in labels; day buckets and the cutoff stay UTC.
"""

import io
import os
import sys
import json
import time
import uuid
import base64
import threading
import traceback
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import gspread
from gspread.utils import rowcol_to_a1
import smtplib
from pymongo import MongoClient
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify, Response

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

load_dotenv()

# ==========================================================
# CONFIG
# ==========================================================
LOCAL_MONGO_URI = os.getenv("MONGO_URI")
if not LOCAL_MONGO_URI:
    print("MONGO_URI not set in environment")
    sys.exit(1)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER", "saidixitn@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS")

API_KEY = os.getenv("API_KEY", "")          # empty = auth disabled
HTTP_PORT = int(os.getenv("PORT", "8000"))

CREDENTIALS_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = "1jZX3nU-YqZiIoSyWDfzxzUyNMAQmo5EcN6LdQKYeXwc"
DOMAINS_SHEET = "Domains"
TEAMS_SHEET = "Teams"
SHEET_URL = (f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
             "/edit?usp=sharing")

# Stats written back to the sheet
SHEET_STATS_ENABLED = os.getenv("SHEET_STATS", "true").strip().lower() != "false"
MASTER_STATS_TAB = os.getenv("MASTER_STATS_TAB", "Master Stats")

TREND_DAYS = 7    # trend chart window
FETCH_DAYS = 8    # trend (7 days) + same weekday last week (day -7)
SPARK_DAYS = 14   # dashboard chart window

# Tunables (env-overridable for bigger deployments)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
SHEET_CACHE_TTL = int(os.getenv("SHEET_CACHE_TTL", "60"))     # seconds
MONGO_MAX_TIME_MS = int(os.getenv("MONGO_MAX_TIME_MS", "120000"))
CHART_DPI = int(os.getenv("CHART_DPI", "170"))
CHART_SAMPLES = int(os.getenv("CHART_SAMPLES", "26"))         # per segment

# Clock shown in "as of HH:MM" labels. Day buckets + the intraday cutoff
# are always computed in UTC (matching createdDt); this only changes how
# the cutoff is PRINTED. Default is India Standard Time -> "15:04 IST".
REPORT_TZ = os.getenv("REPORT_TZ", "Asia/Kolkata")
try:
    from zoneinfo import ZoneInfo
    REPORT_TZINFO = ZoneInfo(REPORT_TZ)
except Exception:
    print(f"warn: unknown REPORT_TZ '{REPORT_TZ}', falling back to UTC")
    REPORT_TZINFO = timezone.utc

# Design tokens
GREEN = "#30d158"
GREEN_DARK = "#248a3d"
RED = "#ff453a"
RED_DARK = "#d70015"
INK = "#1d1d1f"
GREY = "#86868b"
GREY_LIGHT = "#aeaeb2"
GREY_FAINT = "#c7c7cc"
HAIRLINE = "#e8e8ed"
CARD_BG = "#f5f5f7"

# ==========================================================
# PURE HELPERS
# ==========================================================
def clean_domain(d: str) -> str:
    return (d or "").replace("https://", "").replace("http://", "").strip().strip("/")

def no_link(text: str) -> str:
    """Stop Gmail/Outlook auto-linking domain-like text (blue underline)."""
    return (text or "").replace(".", ".&#8203;").replace("/", "/&#8203;")

def to_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0

def normalize_type(t) -> str:
    return str(t or "").strip().lower()

def is_true(v) -> bool:
    """Only true/TRUE (any case) counts. Blank / FALSE / anything else = off."""
    return str(v or "").strip().lower() == "true"

def opt_bool(v, default: bool) -> bool:
    """Blank -> default; otherwise only 'true' is True."""
    s = str(v if v is not None else "").strip().lower()
    return default if s == "" else s == "true"

def pct_change(today: int, prev: int):
    if prev <= 0:
        return None
    return (today - prev) / prev * 100.0

def week_trend(values):
    """Chart direction: least-squares slope over the 7-day window."""
    n = len(values)
    if n < 2:
        return True
    mean_x = (n - 1) / 2
    mean_y = sum(values) / n
    slope = sum((i - mean_x) * (v - mean_y) for i, v in enumerate(values))
    return slope >= 0

def fmt_signed(n: int) -> str:
    return f"+{n:,}" if n >= 0 else f"&#8722;{abs(n):,}"

def parse_date(s: str) -> str:
    """Validate YYYY-MM-DD, raising ValueError with a clean message."""
    datetime.strptime(s, "%Y-%m-%d")
    return s

def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def yesterday_utc() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

def resolve_date(raw) -> str:
    """Accept YYYY-MM-DD, 'today' or 'yesterday' (UTC). Blank = yesterday.
    Raises ValueError for anything else."""
    s = str(raw or "").strip().lower()
    if s in ("", "yesterday"):
        return yesterday_utc()
    if s == "today":
        return today_utc()
    return parse_date(s)

DATE_ERROR = "invalid date '{}', expected YYYY-MM-DD, today or yesterday"

def local_clock(dt_utc) -> str:
    """'15:04' — the cutoff clock in REPORT_TZ (used in short pills)."""
    return dt_utc.astimezone(REPORT_TZINFO).strftime("%H:%M")

def local_clock_tz(dt_utc) -> str:
    """'15:04 IST' — clock + zone abbreviation (used in full labels)."""
    loc = dt_utc.astimezone(REPORT_TZINFO)
    return f"{loc.strftime('%H:%M')} {loc.tzname() or REPORT_TZ}"

def iso_utc(dt) -> str:
    """Mongo hands back naive UTC datetimes; normalise to an ISO 'Z' string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

import re as _re
from bson import ObjectId as _ObjectId

def parse_employer_ids(raw) -> list:
    """Sheet cell may hold one id or a comma list:
    'emp-fjl-apptness,emp-fjl-lensa' -> ['emp-fjl-apptness','emp-fjl-lensa']"""
    return [p.strip() for p in str(raw or "").split(",") if p.strip()]

def row_key(domain: str, employer_ids) -> str:
    """Identity of one report line: the domain PLUS the employer id(s) it is
    filtered by. Same domain + different EmployerId = a different line that
    must never share counts. No employer id -> the key is just the domain."""
    ids = (parse_employer_ids(employer_ids) if isinstance(employer_ids, str)
           else list(employer_ids or []))
    return f"{domain}@{','.join(sorted(ids))}" if ids else domain

def employer_match(employer_ids):
    """Build the employerId filter for 1..N sheet ids.
    - single non-hex id -> plain string match
    - anything else     -> $in over all forms; 24-hex ids are matched as
      BOTH string and ObjectId (some collections store ObjectId)."""
    if isinstance(employer_ids, str):
        employer_ids = parse_employer_ids(employer_ids)
    values = []
    for eid in employer_ids:
        values.append(eid)
        if _re.fullmatch(r"[0-9a-fA-F]{24}", eid):
            try:
                values.append(_ObjectId(eid))
            except Exception:
                pass
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return {"$in": values}

GROUP_FIELDS = {
    "company": ("By company", 0),
    "employerid": ("By employer ID", 1),
    "sourceemployerid": ("By source employer ID", 2),
}
DEFAULT_GROUPS = ["company"]

def parse_group_by(raw: str):
    fields = []
    for part in str(raw or "").split(","):
        key = part.strip().lower()
        if key in GROUP_FIELDS and key not in fields:
            fields.append(key)
    return fields or DEFAULT_GROUPS

# ==========================================================
# DATABASE ROUTING BY DOMAIN TYPE
# ==========================================================
TYPE_DB_MAP = {
    "proxy": "prod_jobiak_ai",
    "direct apply": "directclients_prod",
    "direct_apply": "directclients_prod",
    "direct-apply": "directclients_prod",
}

# Known good indexes per database. Anything not listed falls back to
# DEFAULT_HINT (typical for the per-domain programmatic *_prod DBs).
DB_HINTS = {
    "prod_jobiak_ai": "createdDt_1",
    "directclients_prod": "isBot_1_browser_1_country_1_createdDt_1_mapJobId_1",
}
DEFAULT_HINT = "createdDt_1"

def derive_prod_db(domain: str) -> str:
    """earnwithwfh.com -> earnwithwfh_prod"""
    host = clean_domain(domain).split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host.split(".")[0] + "_prod"

def db_for(row) -> str:
    t = normalize_type(row.get("Domain Type"))
    if t == "programmatic":
        return derive_prod_db(row.get("Domain", ""))
    return TYPE_DB_MAP.get(t) or row.get("Database", "")

# ==========================================================
# SHARED CONNECTIONS (thread-safe, live for the process)
# ==========================================================
_local_client = MongoClient(LOCAL_MONGO_URI)
domains_col = _local_client["mongo_creds"]["creds"]
stats_col = _local_client["daily_domain_stats"]["stats"]
# intraday snapshot: one doc per line per date with the "as of HH:MM"
# yesterday comparison, so /summary can show it for today without
# touching the remote databases
asof_col = _local_client["daily_domain_stats"]["asof"]
# rendered team emails (charts inlined) keyed by (Date, Team) — the
# preview page reads these instead of re-running the report
previews_col = _local_client["daily_domain_stats"]["previews"]

# Ensure the local stores stay fast as history grows. create_index is
# idempotent.
for _col in (stats_col, asof_col):
    try:
        _col.create_index([("Date", 1), ("Domain", 1)],
                          name="date_domain", background=True)
        _col.create_index([("Date", 1), ("Key", 1)],
                          name="date_key", background=True)
    except Exception as _e:
        print(f"warn: could not ensure index on {_col.name}: {_e}")
try:
    previews_col.create_index([("Date", 1), ("Team", 1)],
                              name="date_team", unique=True, background=True)
except Exception as _e:
    print(f"warn: could not ensure index on previews: {_e}")

_client_cache = {}
_client_lock = threading.Lock()
_creds_cache = {}

# hints known to be missing on a db -> skip straight to no-hint (avoids
# paying the failed-hint round trip on every run)
_bad_hints = set()
_bad_hints_lock = threading.Lock()

def get_db(db_name):
    """Resolve creds -> db handle.

    IMPORTANT: only successful lookups are cached. Misses are re-checked on
    every call so creds added later (or a transient blip) recover without a
    process restart.
    """
    with _client_lock:
        record = _creds_cache.get(db_name)
    if not record:
        record = domains_col.find_one({"domain": db_name})
        if record:
            with _client_lock:
                _creds_cache[db_name] = record
    if not record:
        return None
    uri = record["mongo_uri"]
    with _client_lock:
        c = _client_cache.get(uri)
        if c is None:
            c = MongoClient(uri, serverSelectionTimeoutMS=8000,
                            maxPoolSize=max(20, MAX_WORKERS * 2))
            _client_cache[uri] = c
    return c[db_name]

# matplotlib's pyplot is not thread-safe -> serialize chart rendering
_chart_lock = threading.Lock()

# ==========================================================
# GOOGLE SHEET — client, config (cached SHEET_CACHE_TTL seconds) and the
# stats writer (Master Stats tab + one tab per team)
# ==========================================================
_sheet_cache = {"ts": 0.0, "data": None}
_sheet_lock = threading.Lock()
_gc_cache = {"gc": None}
_sheet_write_lock = threading.Lock()   # one writer at a time per process

def gspread_client():
    with _sheet_lock:
        gc = _gc_cache["gc"]
    if gc is None:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        with _sheet_lock:
            _gc_cache["gc"] = gc
    return gc

def _load_sheet_config_uncached():
    sheet = gspread_client().open_by_key(SPREADSHEET_ID)

    domains_rows = sheet.worksheet(DOMAINS_SHEET).get_all_records()
    teams_rows = sheet.worksheet(TEAMS_SHEET).get_all_records()

    for r in domains_rows:
        r["Domain"] = (r.get("Domain") or "").strip()
        r["Database"] = (r.get("Database") or "").strip()
        r["Domain Type"] = (r.get("Domain Type") or "").strip()
        r["EmployerId"] = (str(r.get("EmployerId") or "")).strip()
        r["EmployerIds"] = parse_employer_ids(r.get("EmployerId"))
        r["Lead"] = (r.get("Lead") or "").strip()
        r["GroupBy"] = parse_group_by(r.get("Group By"))
        r["PostingsExpires"] = is_true(r.get("Postings & Expires"))
        r["Key"] = row_key(r["Domain"], r["EmployerIds"])

    teams = defaultdict(lambda: {"members": []})
    for t in teams_rows:
        lead = (t.get("Lead") or "").strip()
        name = (t.get("User") or "").strip()
        email = (t.get("email") or t.get("Email") or "").strip()
        if lead and email:
            teams[lead]["members"].append((name, email))

    team_domains = defaultdict(list)
    for r in domains_rows:
        if r["Domain"] and r["Lead"]:
            team_domains[r["Lead"]].append(r)

    # group-by setting per LINE (keyed by Key, not domain)
    domain_groups = {r["Key"]: r["GroupBy"] for r in domains_rows if r["Domain"]}
    return domains_rows, dict(teams), dict(team_domains), domain_groups

def load_sheet_config(force=False):
    now = time.time()
    with _sheet_lock:
        if (not force and _sheet_cache["data"] is not None
                and now - _sheet_cache["ts"] < SHEET_CACHE_TTL):
            return _sheet_cache["data"]
    data = _load_sheet_config_uncached()
    with _sheet_lock:
        _sheet_cache["data"] = data
        _sheet_cache["ts"] = time.time()
    return data

def resolve_lead(team, team_domains):
    """Case-insensitive team lookup -> canonical lead name, or None."""
    if not team:
        return None
    return {k.lower(): k for k in team_domains}.get(team.strip().lower())

# ---- stats written back to the sheet ----
MASTER_HEADER = ["Date", "Team", "Domain", "Employer ID", "Type",
                 "Clicks", "Unique Clicks", "Postings", "Expires"]
TEAM_HEADER = ["Date", "Domain", "Employer ID", "Type",
               "Clicks", "Unique Clicks", "Postings", "Expires"]
MASTER_ID_COLS = (0, 1, 2, 3)   # Date, Team, Domain, Employer ID
TEAM_ID_COLS = (0, 1, 2)        # Date, Domain, Employer ID

def team_tab_title(lead: str) -> str:
    """Tab is simply the lead's name. Google Sheets forbids [ ] * ? / \\ :
    and caps titles at 100 chars. A lead that happens to share a name with
    a config tab gets ' Stats' appended so nothing is clobbered."""
    cleaned = "".join("-" if c in '[]*?/\\:' else c for c in lead).strip() or "Team"
    reserved = {DOMAINS_SHEET.lower(), TEAMS_SHEET.lower(), MASTER_STATS_TAB.lower()}
    if cleaned.lower() in reserved:
        cleaned += " Stats"
    return cleaned[:100]

def _style_header(ws, ncols):
    """Bold, light-grey, frozen header. Cosmetic — never fatal."""
    try:
        ws.format(f"A1:{rowcol_to_a1(1, ncols)}", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.97},
        })
        ws.freeze(rows=1)
    except Exception as e:
        print(f"warn: could not style header on '{ws.title}': {e}")

def _open_or_create_ws(sheet, title, header):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=1000, cols=len(header))
        ws.update(values=[header],
                  range_name=f"A1:{rowcol_to_a1(1, len(header))}",
                  value_input_option="RAW")
        _style_header(ws, len(header))
        return ws

def upsert_stats_rows(ws, header, rows, id_cols):
    """Rows that share an identity (id_cols) with an existing row are
    updated in place; the rest are inserted at the TOP (row 2) so the
    newest date is always first. One read + one batch update + one
    insert per tab. RAW input keeps dates/ids as plain strings so the
    identity match round-trips; numbers stay numbers."""
    values = ws.get_all_values()
    first = [c.strip() for c in values[0][:len(header)]] if values else []
    if first != header:
        if values and any(c.strip() for c in values[0]):
            raise RuntimeError(
                f"tab '{ws.title}' has a different header "
                f"({' | '.join(c for c in values[0] if c)}). Delete or "
                f"rename that tab and run again.")
        ws.update(values=[header],
                  range_name=f"A1:{rowcol_to_a1(1, len(header))}",
                  value_input_option="RAW")
        _style_header(ws, len(header))
        values = [header]

    def ident(row):
        return tuple(str(row[i]).strip() if i < len(row) else "" for i in id_cols)

    existing = {}
    for rownum, row in enumerate(values[1:], start=2):
        existing.setdefault(ident(row), rownum)

    updates, new_rows = [], []
    for row in rows:
        rownum = existing.get(ident(row))
        if rownum:
            updates.append({
                "range": f"A{rownum}:{rowcol_to_a1(rownum, len(row))}",
                "values": [row],
            })
        else:
            new_rows.append(row)
    if updates:
        ws.batch_update(updates, value_input_option="RAW")
    if new_rows:
        ws.insert_rows(new_rows, row=2, value_input_option="RAW")
    return {"tab": ws.title, "updated": len(updates), "added": len(new_rows)}

# ==========================================================
# REPORT RUN — everything for one date, one team-set
# ==========================================================
class ReportRun:
    def __init__(self, report_date, team_filter=None, dry_run=False,
                 redirect_to=None, send_email=True, write_sheet=True):
        # dates
        parse_date(report_date)  # raises ValueError on bad input
        self.report_date = report_date
        self.min_dt = datetime.strptime(report_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        self.max_dt = self.min_dt + timedelta(days=1)
        self.window_min_dt = self.min_dt - timedelta(days=FETCH_DAYS - 1)
        all_dates = [(self.min_dt - timedelta(days=i)).strftime("%Y-%m-%d")
                     for i in range(FETCH_DAYS - 1, -1, -1)]
        self.window_dates = all_dates[-TREND_DAYS:]
        self.wow_date = all_dates[0]
        self.weekday = self.min_dt.strftime("%a")
        self.day_letters = [datetime.strptime(dt, "%Y-%m-%d").strftime("%a")[0]
                            for dt in self.window_dates]

        # "yesterday" is always relative to the REPORT date
        yday_dt = self.min_dt - timedelta(days=1)
        self.yday_date = yday_dt.strftime("%Y-%m-%d")
        self.yday_weekday = yday_dt.strftime("%a")

        # "Now" is only honest when the report date IS today (the intraday
        # 3 PM run). For yesterday's 10 AM run, show the weekday instead.
        self.now_utc = datetime.now(timezone.utc)
        self.is_today = (report_date == self.now_utc.strftime("%Y-%m-%d"))
        self.last_label = "Now" if self.is_today else self.weekday

        # Comparison rule:
        #   intraday (report date == today) -> compare at the SAME clock
        #   time: yesterday / last week counted only up to now's HH:MM:SS
        #   any older date                  -> compare with the full day
        self.compare_by_time = self.is_today
        if self.compare_by_time:
            self.cutoff_hms = self.now_utc.strftime("%H:%M:%S")   # UTC, like createdDt
            self.asof_clock = local_clock(self.now_utc)           # "15:04"
            self.asof_label = local_clock_tz(self.now_utc)        # "15:04 IST"
        else:
            self.cutoff_hms = None
            self.asof_clock = None
            self.asof_label = None

        # what this run is allowed to do
        self.dry_run = dry_run                       # dry = build only
        self.send_email = bool(send_email) and not dry_run
        self.write_sheet = bool(write_sheet) and not dry_run and SHEET_STATS_ENABLED
        self.redirect_to = redirect_to or []

        # live progress (read by the /jobs poller)
        self.progress = {"phase": "pending", "done": 0, "total": 0}
        self._prog_lock = threading.Lock()

        # sheet config (cached)
        (self.domains_rows, self.teams, self.team_domains,
         self.domain_groups) = load_sheet_config()

        # team filter (case-insensitive lead names)
        if team_filter:
            wanted = {t.strip().lower() for t in team_filter if t.strip()}
            available = {k.lower(): k for k in self.team_domains}
            unknown = [t for t in wanted if t not in available]
            if unknown:
                raise ValueError(
                    f"Unknown team(s): {', '.join(sorted(unknown))}. "
                    f"Available: {', '.join(sorted(self.team_domains))}")
            self.selected_leads = [available[t] for t in wanted]
        else:
            self.selected_leads = list(self.team_domains.keys())

        self._apply_selection()

        # filled by fetch/aggregate — ALL keyed by line Key
        self.all_rows = []
        self.postings_expires = {}
        self.domains = {}
        self.history = defaultdict(lambda: defaultdict(int))        # clicks (full day)
        self.history_ips = defaultdict(lambda: defaultdict(int))    # unique IPs (full day)
        self.history_asof = defaultdict(lambda: defaultdict(int))       # clicks up to cutoff
        self.history_ips_asof = defaultdict(lambda: defaultdict(int))   # unique IPs up to cutoff
        self.failures = {}   # key -> reason (never shown as fake 0 clicks)
        self.log = []

    def _apply_selection(self):
        selected = set(self.selected_leads)
        self.run_domain_rows = [r for r in self.domains_rows
                                if r["Domain"] and r["Lead"] in selected]
        # one fetch per unique line (domain + employer ids); a line listed
        # under several teams is queried once and shared, never double-counted
        self.run_lines = {}
        for r in self.run_domain_rows:
            ln = self.run_lines.get(r["Key"])
            if ln is None:
                ln = dict(r)
                ln["Leads"] = []
                ln["PostingsExpires"] = False
                self.run_lines[r["Key"]] = ln
            if r["Lead"] not in ln["Leads"]:
                ln["Leads"].append(r["Lead"])
            ln["PostingsExpires"] = ln["PostingsExpires"] or r["PostingsExpires"]

    def restrict_to_lead(self, lead):
        """Narrow the run to a single team BEFORE fetching — used by
        single-team /preview so it never queries every team's databases."""
        self.selected_leads = [lead]
        self._apply_selection()

    def _set_progress(self, phase=None, done=None, total=None, bump=False):
        with self._prog_lock:
            if phase is not None:
                self.progress["phase"] = phase
            if total is not None:
                self.progress["total"] = total
            if done is not None:
                self.progress["done"] = done
            if bump:
                self.progress["done"] += 1

    def compare_info(self):
        """Machine-readable description of the active comparison rule
        (returned by /run, /diagnose, stored in the job result)."""
        return {
            "mode": "as_of_time" if self.compare_by_time else "full_day",
            "yesterday_date": self.yday_date,
            "as_of": self.asof_label,
            "as_of_utc": (self.now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                          if self.compare_by_time else None),
            "timezone": REPORT_TZ if self.compare_by_time else None,
        }

    # ---------------- fetch ----------------
    def _aggregate(self, col, db_name, pipeline):
        """Run an aggregation with the best-known index hint. Falls back to
        an unhinted run (once) if the hint is missing, and remembers bad
        hints so future runs skip the failed attempt entirely."""
        hint = DB_HINTS.get(db_name, DEFAULT_HINT)
        key = (db_name, col.name, hint)
        with _bad_hints_lock:
            skip = key in _bad_hints
        if hint and not skip:
            try:
                return list(col.aggregate(pipeline, allowDiskUse=True,
                                          hint=hint,
                                          maxTimeMS=MONGO_MAX_TIME_MS))
            except Exception as e:
                with _bad_hints_lock:
                    _bad_hints.add(key)
                self.log.append(
                    f"hint '{hint}' unavailable on {db_name}.{col.name} "
                    f"({type(e).__name__}); running without hint")
        return list(col.aggregate(pipeline, allowDiskUse=True,
                                  maxTimeMS=MONGO_MAX_TIME_MS))

    def _fetch_views(self, key, domain, db, db_name, employer_ids, domain_type):
        col = db["userAnalytics"]
        match = {
            "isBot": False,
            "browserType": {"$ne": "Unknown"},
            "deviceType": {"$ne": "Unknown"},
            "isFromGoogle": True,
            "createdDt": {"$gt": self.window_min_dt, "$lt": self.max_dt},
        }
        emp_filter = employer_match(employer_ids)
        if emp_filter is not None:
            match["employerId"] = emp_filter

        project = {
            "Company": {"$ifNull": ["$company", {"$ifNull": ["$companyName", "Unknown"]}]},
            "date": {"$substr": ["$createdDt", 0, 10]},
            "employerId": {"$ifNull": ["$employerId", ""]},
            "url": 1,
            "submittedUrl": 1,
            "ipAddress": 1,
            "extraInfo": 1,
        }
        group = {
            "_id": {
                "Company": "$Company",
                "EmployerId": "$employerId",
                "SourceEmployerId": "$sourceEmployerId",
                "Date": "$date",
            },
            "ViewsCount": {"$sum": 1},
            "UniqueIpCount": {"$addToSet": "$ipAddress"},
        }
        final = {
            "_id": 0,
            "Date": "$_id.Date",
            "Company": "$_id.Company",
            "EmployerId": {"$ifNull": ["$_id.EmployerId", ""]},
            "sourceEmployerId": {"$ifNull": ["$_id.SourceEmployerId", ""]},
            "ViewsCount": 1,
            "UniqueIpCount": {"$size": "$UniqueIpCount"},
            "Domain": domain,
            "Domain Type": domain_type,
            # line identity — the sheet's domain + employer filter
            "Key": key,
            "SheetEmployerId": ",".join(employer_ids),
        }

        if self.compare_by_time:
            # Same pipeline, one extra flag: did this click happen before
            # today's cutoff clock (UTC)? Lets every historical day be
            # counted "as of the same time" with no second query.
            project["beforeCutoff"] = {
                "$cond": [
                    {"$eq": [{"$type": "$createdDt"}, "date"]},
                    {"$lt": [{"$dateToString": {"format": "%H:%M:%S",
                                                "date": "$createdDt"}},
                             self.cutoff_hms]},
                    False,
                ]
            }
            group["ViewsCountAsOf"] = {"$sum": {"$cond": ["$beforeCutoff", 1, 0]}}
            group["UniqueIpAsOf"] = {
                "$addToSet": {"$cond": ["$beforeCutoff", "$ipAddress", None]}
            }
            final["ViewsCountAsOf"] = 1
            final["UniqueIpCountAsOf"] = {
                "$size": {"$filter": {"input": "$UniqueIpAsOf", "as": "ip",
                                      "cond": {"$ne": ["$$ip", None]}}}
            }

        pipeline = [
            {"$match": match},
            {"$project": project},
            {"$addFields": {
                "targetUrl": {"$ifNull": ["$url", {"$ifNull": ["$submittedUrl", ""]}]}
            }},
            {"$addFields": {
                "calculatedEndUrl": {
                    "$ifNull": [
                        {"$arrayElemAt": [{"$split": ["$targetUrl", "/"]}, 2]},
                        ""
                    ]
                }
            }},
            {"$addFields": {
                "sourceEmployerId": {
                    "$ifNull": ["$extraInfo.sourceEmployerId", "$calculatedEndUrl"]
                }
            }},
            {"$group": group},
            {"$project": final},
        ]
        return self._aggregate(col, db_name, pipeline)

    def _fetch_postings_expires(self, db, db_name, domain_type, employer_ids):
        t = normalize_type(domain_type)
        if t == "proxy":
            col = db["jobsGoogleSubmittedLog"]
            match = {"createdAt": {"$gt": self.min_dt, "$lt": self.max_dt}}
        else:  # direct apply / programmatic
            col = db["aj_jp_google_logs"]
            match = {
                "createdAt": {"$gt": self.min_dt, "$lt": self.max_dt},
                "status_update": 6,
            }
        emp_filter = employer_match(employer_ids)
        if emp_filter is not None:
            match["employerId"] = emp_filter

        pipeline = [
            {"$match": match},
            {"$group": {"_id": "$callType", "count": {"$sum": 1}}},
        ]
        postings = expires = 0
        # no hint here — different collections, unknown index shapes;
        # the date-range match is small (one day)
        for doc in col.aggregate(pipeline, allowDiskUse=True,
                                 maxTimeMS=MONGO_MAX_TIME_MS):
            if doc["_id"] == "URL_UPDATED":
                postings = to_int(doc["count"])
            elif doc["_id"] == "URL_DELETED":
                expires = to_int(doc["count"])
        return {"postings": postings, "expires": expires}

    def _process_domain(self, line):
        """Returns (key, rows, pe, error). error is None on success —
        a failed fetch is REPORTED, never silently rendered as 0 clicks."""
        key, domain = line["Key"], line["Domain"]
        domain_type = line["Domain Type"]
        employer_ids = line["EmployerIds"]
        if not domain:
            return (key, [], None, "empty domain row")

        db_name = db_for(line)
        if not db_name:
            return (key, [], None,
                    f"unknown Domain Type '{domain_type}' (no Database column)")

        db = get_db(db_name)
        if db is None:
            return (key, [], None, f"no mongo creds for db '{db_name}'")

        try:
            rows = self._fetch_views(key, domain, db, db_name, employer_ids,
                                     domain_type)
        except Exception as e:
            return (key, [], None, f"fetch failed: {type(e).__name__}: {e}")

        pe = None
        if line["PostingsExpires"]:
            try:
                pe = self._fetch_postings_expires(db, db_name, domain_type,
                                                  employer_ids)
            except Exception as e:
                self.log.append(f"postings/expires failed for {key}: {e}")

        return (key, rows, pe, None)

    # ---------------- aggregate ----------------
    def fetch_and_aggregate(self):
        lines = list(self.run_lines.values())
        self._set_progress(phase="fetching", done=0, total=len(lines))
        workers = min(MAX_WORKERS, max(1, len(lines)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(self._process_domain, ln) for ln in lines]
            for f in as_completed(futures):
                key, rows, pe, err = f.result()
                self._set_progress(bump=True)
                if err:
                    self.failures[key] = err
                    continue
                self.all_rows.extend(rows)
                if pe is not None:
                    self.postings_expires[key] = pe

        self._set_progress(phase="aggregating")
        today_rows = [r for r in self.all_rows if r.get("Date") == self.report_date]
        ok_keys = [k for k in self.run_lines if k not in self.failures]
        ok_domains = [self.run_lines[k]["Domain"] for k in ok_keys]

        # store today's rows (real runs only; previews/dry-runs never touch
        # stats). Replace ONLY the lines that fetched OK; also sweep rows
        # written before Key existed so they can't double-count in /summary.
        if not self.dry_run:
            stats_col.delete_many({
                "Date": self.report_date,
                "$or": [{"Key": {"$in": ok_keys}},
                        {"Key": {"$exists": False},
                         "Domain": {"$in": ok_domains}}]})
            for r in today_rows:
                r["InsertedAt"] = datetime.now(timezone.utc)
            if today_rows:
                stats_col.insert_many(today_rows, ordered=False)

        # per-line aggregation — seed ONLY lines that fetched OK, so a
        # failed line can never render as a legitimate-looking 0-click card
        self.domains = defaultdict(lambda: {
            "type": "",
            "domain": "",
            "employer_id": "",
            "clicks": 0,
            "ips": 0,
            "yday_clicks": 0,   # yesterday under the active comparison rule
            "yday_ips": 0,
            "rows": defaultdict(lambda: {"clicks": 0, "ips": 0})
        })
        for key in ok_keys:
            ln = self.run_lines[key]
            d = self.domains[key]
            d["type"] = ln["Domain Type"]
            d["domain"] = ln["Domain"]
            d["employer_id"] = ",".join(ln["EmployerIds"])

        for r in today_rows:
            key = r.get("Key", "")
            if not key:
                continue
            d = self.domains[key]
            clicks = to_int(r.get("ViewsCount", 0))
            ips = to_int(r.get("UniqueIpCount", 0))
            d["clicks"] += clicks
            d["ips"] += ips
            k = (
                r.get("Company", "Unknown"),
                r.get("EmployerId", "") or "",
                r.get("sourceEmployerId", "") or "",
            )
            d["rows"][k]["clicks"] += clicks
            d["rows"][k]["ips"] += ips

        for r in self.all_rows:
            key = r.get("Key", "")
            dt = r.get("Date", "")
            self.history[key][dt] += to_int(r.get("ViewsCount", 0))
            self.history_ips[key][dt] += to_int(r.get("UniqueIpCount", 0))
            if self.compare_by_time:
                self.history_asof[key][dt] += to_int(r.get("ViewsCountAsOf", 0))
                self.history_ips_asof[key][dt] += to_int(r.get("UniqueIpCountAsOf", 0))

        # yesterday figures under the active rule (same clock time when the
        # run is for today, whole day otherwise)
        for key, d in list(self.domains.items()):
            d["yday_clicks"], d["yday_ips"] = self.prev_day_for(key)

        # intraday real runs snapshot the as-of comparison for /summary
        if not self.dry_run and self.compare_by_time:
            self._store_asof_snapshot(ok_keys)

    def _store_asof_snapshot(self, ok_keys):
        if not ok_keys:
            return
        now = datetime.now(timezone.utc)
        docs = []
        for key in ok_keys:
            ln, d = self.run_lines[key], self.domains[key]
            docs.append({
                "Date": self.report_date,
                "Key": key,
                "Domain": ln["Domain"],
                "EmployerId": ",".join(ln["EmployerIds"]),
                "Teams": ln["Leads"],
                "Team": ln["Leads"][0],
                "Cutoff": self.asof_label,
                "CutoffUtc": self.now_utc,
                "YesterdayDate": self.yday_date,
                "Clicks": d["clicks"],
                "UniqueIps": d["ips"],
                "YdayClicks": d["yday_clicks"],
                "YdayUniqueIps": d["yday_ips"],
                "InsertedAt": now,
            })
        try:
            asof_col.delete_many({
                "Date": self.report_date,
                "$or": [{"Key": {"$in": ok_keys}},
                        {"Key": {"$exists": False},
                         "Domain": {"$in": [self.run_lines[k]["Domain"]
                                            for k in ok_keys]}}]})
            asof_col.insert_many(docs, ordered=False)
        except Exception as e:
            self.log.append(f"as-of snapshot write failed: {e}")

    # ---- series: chart always shows FULL days; cmp_* follow the rule ----
    def series_for(self, key):
        return [self.history.get(key, {}).get(dt, 0) for dt in self.window_dates]

    def ip_series_for(self, key):
        return [self.history_ips.get(key, {}).get(dt, 0)
                for dt in self.window_dates]

    def _cmp_hist(self):
        return self.history_asof if self.compare_by_time else self.history

    def _cmp_hist_ips(self):
        return self.history_ips_asof if self.compare_by_time else self.history_ips

    def cmp_series_for(self, key):
        h = self._cmp_hist()
        return [h.get(key, {}).get(dt, 0) for dt in self.window_dates]

    def prev_day_for(self, key):
        """(clicks, unique) for the day before the report date — counted
        up to the cutoff clock intraday, whole day otherwise."""
        return (self._cmp_hist().get(key, {}).get(self.yday_date, 0),
                self._cmp_hist_ips().get(key, {}).get(self.yday_date, 0))

    def wow_for(self, key):
        h = self._cmp_hist()
        today = h.get(key, {}).get(self.report_date, 0)
        prev = h.get(key, {}).get(self.wow_date, 0)
        return pct_change(today, prev)

    # ---------------- chart ----------------
    @staticmethod
    def _catmull_rom(values, samples_per_seg=None):
        samples_per_seg = samples_per_seg or CHART_SAMPLES
        n = len(values)
        if n < 2:
            return list(range(n)), list(values)
        pts = [values[0]] + list(values) + [values[-1]]
        xs, ys = [], []
        for i in range(1, n):
            p0, p1, p2, p3 = pts[i - 1], pts[i], pts[i + 1], pts[i + 2]
            for s in range(samples_per_seg):
                t = s / samples_per_seg
                t2, t3 = t * t, t * t * t
                y = 0.5 * ((2 * p1) + (-p0 + p2) * t
                           + (2 * p0 - 5 * p1 + 4 * p2 - p3) * t2
                           + (-p0 + 3 * p1 - 3 * p2 + p3) * t3)
                xs.append((i - 1) + t)
                ys.append(y)
        xs.append(n - 1)
        ys.append(values[-1])
        return xs, ys

    def make_chart_png(self, values, ip_values, rising: bool) -> bytes:
        """Solid line + soft fill = total clicks. Dotted line = unique IPs."""
        color = GREEN if rising else RED
        xs, ys = self._catmull_rom(values)
        has_ips = bool(ip_values) and any(ip_values)

        all_vals = list(values) + (list(ip_values) if has_ips else [])
        lo, hi = min(all_vals), max(all_vals)
        pad = max((hi - lo) * 0.18, 1)

        with _chart_lock:
            fig, ax = plt.subplots(figsize=(5.2, 1.5), dpi=CHART_DPI)
            fig.patch.set_alpha(0)
            ax.set_facecolor("none")

            # clicks (solid, filled)
            ax.plot(xs, ys, color=color, linewidth=3,
                    solid_capstyle="round", zorder=4)
            ax.fill_between(xs, ys, lo - pad * 1.2,
                            color=color, alpha=0.14, linewidth=0, zorder=2)
            ax.scatter([len(values) - 1], [values[-1]], s=70, color=color,
                       edgecolors="white", linewidths=2.2, zorder=6)

            # unique IPs (dotted, same accent, softer)
            if has_ips:
                xi, yi = self._catmull_rom(ip_values)
                ax.plot(xi, yi, color=color, linewidth=2,
                        linestyle=(0, (1, 2.4)), dash_capstyle="round",
                        alpha=0.55, zorder=3)
                ax.scatter([len(ip_values) - 1], [ip_values[-1]], s=32,
                           facecolors="white", edgecolors=color,
                           linewidths=1.8, alpha=0.9, zorder=5)

            ax.set_xlim(-0.05, len(values) - 0.8)
            ax.set_ylim(lo - pad, hi + pad)
            ax.axis("off")
            plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

            buf = io.BytesIO()
            fig.savefig(buf, format="png", transparent=True)
            plt.close(fig)
        return buf.getvalue()

    # ---------------- HTML pieces ----------------
    def chart_legend_html(self, rising):
        accent = GREEN_DARK if rising else RED_DARK
        return f"""
            <div class="legend" style="font-size:11px; color:{GREY_LIGHT}; padding-top:10px; letter-spacing:0.01em;">
                <span style="color:{accent}; font-weight:700;">&#9472;&#9472;</span>&nbsp;Total clicks
                &nbsp;&nbsp;&nbsp;&nbsp;
                <span style="color:{accent}; font-weight:700; letter-spacing:2px;">&#8226;&#8226;&#8226;</span>&nbsp;Unique clicks
            </div>"""

    def day_strip_html(self, values, ip_values, rising):
        accent = GREEN_DARK if rising else RED_DARK
        cells = ""
        n = len(values)
        for i, v in enumerate(values):
            ip = ip_values[i] if i < len(ip_values) else 0
            if i < n - 1:
                cells += f"""
                <td align="center" width="14%" style="width:14%; padding:0 2px; text-align:center;"><div class="day-lbl" style="font-size:11px; color:{GREY_FAINT};">{self.day_letters[i]}</div><div class="day-val" style="font-size:12px; color:{GREY_LIGHT}; padding-top:4px; letter-spacing:-0.01em;">{v:,}</div><div class="day-ip" style="font-size:10px; color:{GREY_FAINT}; padding-top:2px; letter-spacing:-0.01em;">{ip:,}</div></td>"""
            else:
                cells += f"""
                <td align="center" width="16%" style="width:16%; padding:0 2px; text-align:center;"><div class="day-lbl" style="font-size:11px; color:{accent}; font-weight:600;">{self.last_label}</div><div class="day-val" style="font-size:12px; color:{INK}; font-weight:600; padding-top:4px; letter-spacing:-0.01em;">{v:,}</div><div class="day-ip" style="font-size:10px; color:{GREY}; padding-top:2px; letter-spacing:-0.01em;">{ip:,}</div></td>"""
        return f"""
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; table-layout:fixed; margin-top:12px; margin-bottom:32px;">
                <tr>{cells}
                </tr>
            </table>"""

    def section_html(self, title, mapping):
        """mapping: label -> {'clicks': int, 'ips': int}. Renders Clicks and
        Unique IPs side by side with mini column headers."""
        if not mapping:
            return ""
        rows = ""
        ordered = sorted(mapping.items(), key=lambda kv: kv[1]["clicks"],
                         reverse=True)
        for label, s in ordered:
            rows += f"""
                <tr>
                    <td class="row-lbl" style="font-size:14px; color:#6e6e73; padding:11px 0; border-top:1px solid {HAIRLINE}; word-break:break-word;">{no_link(str(label))}</td>
                    <td class="row-val" align="right" width="76" style="width:76px; text-align:right; font-size:14px; color:{INK}; padding:11px 0; border-top:1px solid {HAIRLINE}; white-space:nowrap;">{s['clicks']:,}</td>
                    <td class="row-ip" align="right" width="76" style="width:76px; text-align:right; font-size:13px; color:{GREY}; padding:11px 0 11px 14px; border-top:1px solid {HAIRLINE}; white-space:nowrap;">{s['ips']:,}</td>
                </tr>"""
        return f"""
            <div class="sec-lbl" style="font-size:12px; font-weight:600; color:{GREY_LIGHT}; letter-spacing:0.06em; text-transform:uppercase; padding:26px 0 10px 0;">{title}</div>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;">
                <tr>
                    <td></td>
                    <td class="col-hdr" align="right" width="76" style="width:76px; text-align:right; font-size:10px; font-weight:600; color:{GREY_FAINT}; letter-spacing:0.06em; text-transform:uppercase; padding-bottom:6px; white-space:nowrap;">Clicks</td>
                    <td class="col-hdr" align="right" width="76" style="width:76px; text-align:right; font-size:10px; font-weight:600; color:{GREY_FAINT}; letter-spacing:0.06em; text-transform:uppercase; padding:0 0 6px 14px; white-space:nowrap;">Unique</td>
                </tr>{rows}
            </table>"""

    def postings_expires_html(self, pe):
        if pe is None:
            return ""
        return f"""
            <div class="sec-lbl" style="font-size:12px; font-weight:600; color:{GREY_LIGHT}; letter-spacing:0.06em; text-transform:uppercase; padding:26px 0 10px 0;">Postings &amp; Expires</div>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;">
                <tr>
                    <td class="row-lbl" style="font-size:14px; color:#6e6e73; padding:11px 0; border-top:1px solid {HAIRLINE};">Postings</td>
                    <td class="row-val" align="right" style="text-align:right; font-size:14px; color:{INK}; padding:11px 0; border-top:1px solid {HAIRLINE};">{pe['postings']:,}</td>
                </tr>
                <tr>
                    <td class="row-lbl" style="font-size:14px; color:#6e6e73; padding:11px 0; border-top:1px solid {HAIRLINE};">Expires</td>
                    <td class="row-val" align="right" style="text-align:right; font-size:14px; color:{INK}; padding:11px 0; border-top:1px solid {HAIRLINE};">{pe['expires']:,}</td>
                </tr>
            </table>"""

    def receipt_rows_html(self, key, d):
        group_fields = self.domain_groups.get(key, DEFAULT_GROUPS)

        sections = ""
        for field in group_fields:
            title, idx = GROUP_FIELDS[field]
            mapping = defaultdict(lambda: {"clicks": 0, "ips": 0})
            for k, s in d["rows"].items():
                label = k[idx]
                if field == "company" and (not label or label == "Unknown"):
                    continue
                if field != "company" and not label:
                    continue
                mapping[label]["clicks"] += s["clicks"]
                mapping[label]["ips"] += s["ips"]
            sections += self.section_html(title, mapping)

        if self.compare_by_time:
            yday_row_lbl = f"Yesterday as of {self.asof_label}"
        else:
            yday_row_lbl = f"Yesterday ({self.yday_weekday}, full day)"

        summary = f"""
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; margin-top:26px;">
                <tr>
                    <td class="row-lbl" style="font-size:14px; font-weight:600; color:{INK}; padding:12px 0; border-top:1px solid {HAIRLINE};">Total clicks</td>
                    <td class="row-val" align="right" style="text-align:right; font-size:14px; font-weight:600; color:{INK}; padding:12px 0; border-top:1px solid {HAIRLINE};">{d['clicks']:,}</td>
                </tr>
                <tr>
                    <td class="row-lbl" style="font-size:14px; font-weight:600; color:{INK}; padding:12px 0; border-top:1px solid {HAIRLINE};">Unique IPs</td>
                    <td class="row-val" align="right" style="text-align:right; font-size:14px; font-weight:600; color:{INK}; padding:12px 0; border-top:1px solid {HAIRLINE};">{d['ips']:,}</td>
                </tr>
                <tr>
                    <td class="row-lbl" style="font-size:14px; color:#6e6e73; padding:12px 0 0 0; border-top:1px solid {HAIRLINE};">{yday_row_lbl}</td>
                    <td class="row-val" align="right" style="text-align:right; font-size:14px; color:#6e6e73; padding:12px 0 0 0; border-top:1px solid {HAIRLINE}; white-space:nowrap;">{d['yday_clicks']:,} <span style="color:{GREY_LIGHT};">&middot; {d['yday_ips']:,} unique</span></td>
                </tr>
            </table>"""

        return sections + summary

    def numbers_row_html(self, d):
        """Three metrics in ONE consistent shape — number, caption line,
        qualifier line — so they align on any width. Three columns on wide
        screens; on phones each metric becomes its own full-width row
        (.num-cell is switched to display:block by the mobile CSS)."""
        when = "today" if self.is_today else f"on {self.weekday}"

        yday = d["yday_clicks"]
        ydelta = d["clicks"] - yday
        ypct = pct_change(d["clicks"], yday)
        if ydelta > 0:
            ycolor, yarrow = GREEN_DARK, "&#8593;"
        elif ydelta < 0:
            ycolor, yarrow = RED_DARK, "&#8595;"
        else:
            ycolor, yarrow = GREY, "&#8226;"
        ydelta_txt = f"{yarrow}&#8202;{fmt_signed(ydelta)}"
        if ypct is not None:
            ydelta_txt += f" &middot; {abs(ypct):.1f}%"
        if self.compare_by_time:
            yqual = f"as of {self.asof_label}"
        else:
            yqual = f"{self.yday_weekday}, full day"

        def cell(num, cap, qual, extra="", first=False, last=False):
            border = "" if first else f"border-left:1px solid {HAIRLINE};"
            pad = ("padding-right:12px;" if first else
                   "padding-left:14px;" if last else "padding:0 12px 0 14px;")
            cls = "num-cell num-first" if first else "num-cell"
            return f"""
                    <td class="{cls}" width="33%" valign="top" style="width:33%; {pad} {border}">
                        <div class="big-num" style="font-size:40px; font-weight:300; color:{INK}; letter-spacing:-0.04em; line-height:1;">{num:,}</div>
                        <div class="num-cap" style="font-size:13px; color:{GREY}; padding-top:10px; line-height:1.3;">{cap}</div>
                        <div class="num-qual" style="font-size:12px; color:{GREY_LIGHT}; padding-top:2px; line-height:1.3;">{qual}</div>{extra}
                    </td>"""

        extra = (f'<div class="yday-delta" style="font-size:12px; font-weight:600; '
                 f'color:{ycolor}; padding-top:6px; white-space:nowrap;">{ydelta_txt}</div>')
        return f"""
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="num-row" style="width:100%; table-layout:fixed; margin-bottom:26px;">
                <tr>{cell(d['clicks'], 'total clicks', when, first=True)}{cell(d['ips'], 'unique clicks', when)}{cell(yday, 'yesterday&#8217;s clicks', yqual, extra=extra, last=True)}
                </tr>
            </table>"""

    def domain_card_html(self, key, d, values, ip_values, chart_cid,
                         rising, wow_pct):
        if wow_pct is None:
            pill = "&#8212;"
            pill_color = GREY
        else:
            arrow = "&#8593;" if wow_pct >= 0 else "&#8595;"
            pill = f"{arrow}&#8202;{abs(wow_pct):.1f}% vs last {self.weekday}"
            if self.compare_by_time:
                pill += f" at {self.asof_clock}"
            pill_color = GREEN_DARK if wow_pct >= 0 else RED_DARK

        dtype = d.get("type") or ""
        emp = d.get("employer_id") or ""
        sub = dtype + (f" &middot; {no_link(emp)}" if emp else "")
        dom_label = no_link(clean_domain(d.get("domain") or key))
        chart_img = (
            f'<img src="cid:{chart_cid}" alt="7-day trend" width="520" '
            f'style="width:100%; height:auto; display:block;" />'
        ) if chart_cid else ""
        legend = self.chart_legend_html(rising) if chart_cid else ""
        pe = self.postings_expires.get(key)

        return f"""
        <tr><td class="card" style="background-color:{CARD_BG}; border-radius:26px; padding:32px 26px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; margin-bottom:28px;">
                <tr>
                    <td valign="middle" style="word-break:break-word;">
                        <div class="dom-name" style="font-size:15px; font-weight:600; color:{INK}; letter-spacing:-0.01em;">{dom_label}</div>
                        <div style="font-size:13px; color:{GREY}; padding-top:3px;">{sub}</div>
                    </td>
                    <td valign="middle" align="right" style="text-align:right; padding-left:10px;">
                        <table role="presentation" cellpadding="0" cellspacing="0" border="0" align="right"><tr>
                            <td class="pill" style="background-color:#ffffff; border-radius:100px; padding:6px 12px; font-size:12px; font-weight:600; color:{pill_color}; white-space:nowrap;">{pill}</td>
                        </tr></table>
                    </td>
                </tr>
            </table>
            {self.numbers_row_html(d)}
            <div style="width:100%; line-height:0;">{chart_img}</div>
            {legend}
            {self.day_strip_html(values, ip_values, rising)}
            {self.postings_expires_html(pe)}
            {self.receipt_rows_html(key, d)}
        </td></tr>
        <tr><td style="height:20px; line-height:20px; font-size:0;">&nbsp;</td></tr>"""

    def failed_card_html(self, domain, employer_id, reason):
        emp_line = (f'<div style="font-size:13px; color:{GREY}; padding-top:3px;">{no_link(employer_id)}</div>'
                    if employer_id else "")
        return f"""
        <tr><td class="card" style="background-color:{CARD_BG}; border-radius:26px; padding:26px;">
            <div class="dom-name" style="font-size:15px; font-weight:600; color:{INK}; letter-spacing:-0.01em; word-break:break-word;">{no_link(clean_domain(domain))}</div>
            {emp_line}
            <div style="font-size:13px; color:{RED_DARK}; padding-top:8px;">&#9888;&#65039; Data unavailable for this run</div>
            <div style="font-size:12px; color:{GREY}; padding-top:4px; word-break:break-word;">{reason}</div>
        </td></tr>
        <tr><td style="height:20px; line-height:20px; font-size:0;">&nbsp;</td></tr>"""

    # ---------------- full email ----------------
    def subject_for(self, lead):
        subject = f"Daily Domain Click Report - Team {lead} - {self.report_date}"
        if self.compare_by_time:
            subject += f" - as of {self.asof_label}"
        return subject

    def team_lines(self, team_domain_rows):
        """Unique line keys for a team, in sheet order."""
        seen, out = set(), []
        for r in team_domain_rows:
            if r["Key"] not in seen:
                seen.add(r["Key"])
                out.append(r["Key"])
        return out

    def build_team_email(self, lead, team_domain_rows, chart_offset=0):
        team_doms, team_failed = [], []
        for k in self.team_lines(team_domain_rows):
            if k in self.domains:
                team_doms.append((k, self.domains[k]))
            elif k in self.failures:
                team_failed.append((k, self.failures[k]))
        ordered = sorted(team_doms, key=lambda x: x[1]["clicks"], reverse=True)
        if not ordered and not team_failed:
            return None, []

        total_today = sum(d["clicks"] for _, d in ordered)
        total_ips = sum(d["ips"] for _, d in ordered)
        total_yday = sum(d["yday_clicks"] for _, d in ordered)

        # headline + delta follow the comparison rule: intraday every
        # previous day is counted only up to the same clock time
        cmp_totals = [sum(self.cmp_series_for(k)[i] for k, _ in ordered)
                      for i in range(TREND_DAYS)] if ordered else [0] * TREND_DAYS
        prev_totals = cmp_totals[:-1]
        delta = total_today - total_yday
        days_with_data = sum(1 for v in prev_totals if v > 0)

        if days_with_data == 0:
            headline, headline_color, arrow = "Here's how today went", GREY, "&#8226;"
        elif total_today > max(prev_totals) and total_today > 0:
            headline, headline_color, arrow = "Your best day this week", GREEN_DARK, "&#8593;"
        elif delta > 0:
            headline, headline_color, arrow = "Trending up today", GREEN_DARK, "&#8593;"
        elif delta < 0:
            headline, headline_color, arrow = "A slower day", RED_DARK, "&#8595;"
        else:
            headline, headline_color, arrow = "Holding steady", GREY, "&#8226;"

        # intraday run reads "so far"; the morning run doesn't overclaim
        if self.is_today and days_with_data > 0:
            if headline == "Your best day this week":
                headline = "Your best day this week, so far"
            elif headline == "A slower day":
                headline = "A slower day, so far"

        images = []
        cards = ""
        for i, (key, d) in enumerate(ordered):
            values = self.series_for(key)
            ip_values = self.ip_series_for(key)
            rising = week_trend(values)
            wow = self.wow_for(key)

            cid = None
            if any(values) or any(ip_values):
                cid = f"chart{chart_offset + i}"
                images.append((cid, self.make_chart_png(values, ip_values, rising)))

            cards += self.domain_card_html(key, d, values, ip_values, cid,
                                           rising, wow)

        for key, reason in team_failed:
            ln = self.run_lines[key]
            cards += self.failed_card_html(ln["Domain"],
                                           ",".join(ln["EmployerIds"]), reason)

        insight_block = f"""
        <tr><td class="darkcard" style="background-color:{INK}; border-radius:26px; padding:36px 30px 32px 30px;">
            <div style="font-size:13px; font-weight:600; color:{GREY}; padding-bottom:14px;">Grow this report</div>
            <div class="insight" style="font-size:21px; line-height:1.4; font-weight:500; color:#ffffff; letter-spacing:-0.018em;">
                Want more domains in tomorrow's report? Add them to the spreadsheet and they'll show up automatically.
            </div>
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:28px;"><tr>
                <td style="background-color:#ffffff; border-radius:100px;">
                    <a href="{SHEET_URL}" style="display:inline-block; padding:12px 26px; font-size:15px; font-weight:600; color:{INK}; text-decoration:none;">Add domains</a>
                </td>
            </tr></table>
        </td></tr>"""

        try:
            nice_date = self.min_dt.strftime("%A, %B %-d")
        except ValueError:
            nice_date = self.min_dt.strftime("%A, %B %d")

        if self.is_today:
            date_line = f"Here's how your domains are doing so far today, {nice_date}."
            hero_sub = f"clicks so far today &middot; as of {self.asof_label}"
            vs_label = f"vs Yesterday at {self.asof_clock}"
            preheader_cmp = f" vs {total_yday:,} yesterday at {self.asof_clock}."
        else:
            date_line = f"Here's how your domains performed on {nice_date}."
            hero_sub = "clicks across your domains"
            vs_label = "vs Yesterday"
            preheader_cmp = f" vs {total_yday:,} the day before."

        html = f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="x-apple-disable-message-reformatting">
    <title>Daily clicks report</title>
    <style>
        /* Mobile: tighten every layer so nothing wraps or overflows */
        @media only screen and (max-width: 480px) {{
            .wrap    {{ padding: 0 10px !important; }}
            .card    {{ padding: 24px 18px !important; border-radius: 22px !important; }}
            .darkcard{{ padding: 28px 22px !important; border-radius: 20px !important; }}
            .hero-num{{ font-size: 64px !important; }}
            .hero-sub{{ font-size: 15px !important; }}
            .head-line{{ font-size: 14px !important; }}
            /* the three metrics stack into full-width rows */
            .num-row {{ margin-bottom: 18px !important; }}
            .num-cell{{ display: block !important; width: 100% !important; padding: 14px 0 0 0 !important; margin-top: 14px !important; border-left: 0 !important; border-top: 1px solid #e8e8ed !important; }}
            .num-first{{ padding-top: 0 !important; margin-top: 0 !important; border-top: 0 !important; }}
            .big-num {{ font-size: 34px !important; }}
            .num-cap {{ font-size: 12px !important; padding-top: 6px !important; }}
            .num-qual{{ font-size: 11px !important; }}
            .yday-delta{{ font-size: 11px !important; }}
            .dom-name{{ font-size: 14px !important; }}
            .pill    {{ font-size: 10px !important; padding: 5px 9px !important; }}
            .stat-num{{ font-size: 17px !important; }}
            .stat-lbl{{ font-size: 10px !important; }}
            .day-lbl {{ font-size: 10px !important; }}
            .day-val {{ font-size: 10px !important; letter-spacing: -0.02em; }}
            .day-ip  {{ font-size: 9px !important; letter-spacing: -0.02em; }}
            .legend  {{ font-size: 10px !important; }}
            .sec-lbl {{ font-size: 10px !important; padding-top: 20px !important; }}
            .col-hdr {{ font-size: 9px !important; width: 60px !important; }}
            .row-lbl {{ font-size: 13px !important; padding-top: 9px !important; padding-bottom: 9px !important; }}
            .row-val {{ font-size: 13px !important; padding-top: 9px !important; padding-bottom: 9px !important; width: 60px !important; }}
            .row-ip  {{ font-size: 12px !important; padding-top: 9px !important; padding-bottom: 9px !important; width: 60px !important; }}
            .insight {{ font-size: 18px !important; line-height: 1.45 !important; }}
        }}
    </style>
</head>
<body style="margin:0; padding:0; background-color:#ffffff; font-family:-apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Helvetica, Arial, sans-serif; -webkit-font-smoothing:antialiased;">
    <div style="display:none; max-height:0; overflow:hidden; mso-hide:all;">
        {total_today:,} clicks &middot; {total_ips:,} unique{preheader_cmp} {headline}.
    </div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; background-color:#ffffff;">
    <tr><td class="wrap" align="center" style="padding:0 20px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; max-width:580px;">

        <tr><td align="center" style="padding:48px 0 0 0;">
            <div style="font-size:26px; font-weight:600; color:{INK}; letter-spacing:-0.02em;">Hey there &#128075;</div>
            <div class="head-line" style="font-size:15px; color:{GREY}; padding-top:10px;">{date_line}</div>
        </td></tr>

        <tr><td align="center" style="padding:56px 0 0 0;">
            <div style="font-size:15px; font-weight:600; color:{headline_color}; padding-bottom:18px;">{arrow}&#8202; {headline}</div>
            <div class="hero-num" style="font-size:88px; font-weight:300; color:{INK}; letter-spacing:-0.05em; line-height:0.95;">{total_today:,}</div>
            <div class="hero-sub" style="font-size:16px; color:{GREY}; padding-top:16px;">{hero_sub}</div>
        </td></tr>

        <tr><td style="padding:56px 0 24px 0;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%; table-layout:fixed;">
                <tr>
                    <td width="33%" align="center" valign="top" style="width:33%; text-align:center; border-right:1px solid #f2f2f4; padding:4px 4px;">
                        <div class="stat-num" style="font-size:22px; font-weight:500; color:{INK}; letter-spacing:-0.02em;">{fmt_signed(delta)}</div>
                        <div class="stat-lbl" style="font-size:13px; color:{GREY}; padding-top:6px; line-height:1.3;">{vs_label}</div>
                    </td>
                    <td width="33%" align="center" valign="top" style="width:33%; text-align:center; border-right:1px solid #f2f2f4; padding:4px 4px;">
                        <div class="stat-num" style="font-size:22px; font-weight:500; color:{INK}; letter-spacing:-0.02em;">{total_ips:,}</div>
                        <div class="stat-lbl" style="font-size:13px; color:{GREY}; padding-top:6px; line-height:1.3;">Unique Clicks</div>
                    </td>
                    <td width="34%" align="center" valign="top" style="width:34%; text-align:center; padding:4px 4px;">
                        <div class="stat-num" style="font-size:22px; font-weight:500; color:{INK}; letter-spacing:-0.02em;">{len(ordered)}</div>
                        <div class="stat-lbl" style="font-size:13px; color:{GREY}; padding-top:6px; line-height:1.3;">Domain{'s' if len(ordered) != 1 else ''} Tracked</div>
                    </td>
                </tr>
            </table>
        </td></tr>

        <tr><td style="height:48px; line-height:48px; font-size:0;">&nbsp;</td></tr>

{cards}
{insight_block}

        <tr><td align="center" style="padding:48px 0 60px 0;">
            <div style="font-size:12px; color:{GREY_LIGHT}; line-height:1.8;">
                Sent to Team {lead} by the Daily Click Report &middot; Delivered every morning
            </div>
        </td></tr>

    </table>
    </td></tr>
    </table>
</body>
</html>"""
        return html, images

    # ---------------- preview cache ----------------
    @staticmethod
    def _inline_charts(html, images):
        for cid, png in images:
            b64 = base64.b64encode(png).decode()
            html = html.replace(f"cid:{cid}", f"data:image/png;base64,{b64}")
        return html

    def save_preview(self, lead, html_inlined):
        """Store the rendered email so /preview never has to re-run."""
        try:
            previews_col.replace_one(
                {"Date": self.report_date, "Team": lead},
                {"Date": self.report_date, "Team": lead,
                 "Html": html_inlined,
                 "Subject": self.subject_for(lead),
                 "AsOf": self.asof_label,
                 "CompareMode": "as_of_time" if self.compare_by_time else "full_day",
                 "IsToday": self.is_today,
                 "GeneratedAt": datetime.now(timezone.utc)},
                upsert=True)
        except Exception as e:
            self.log.append(f"preview cache write failed for {lead}: {e}")

    # ---------------- sheet stats ----------------
    def sheet_rows(self):
        """Master-tab rows: one per (team, line) that fetched OK, in
        MASTER_HEADER order. Failed lines are left out on purpose."""
        rows = []
        for lead in self.selected_leads:
            for key in self.team_lines(self.team_domains.get(lead, [])):
                ln = self.run_lines.get(key)
                if ln is None or key in self.failures:
                    continue
                d = self.domains[key]
                pe = self.postings_expires.get(key)
                rows.append([
                    self.report_date,
                    lead,
                    ln["Domain"],
                    ",".join(ln["EmployerIds"]),
                    ln["Domain Type"],
                    d["clicks"],
                    d["ips"],
                    pe["postings"] if pe else "",
                    pe["expires"] if pe else "",
                ])
        return rows

    def write_sheet_stats(self):
        """Upsert this run's rows into the master tab and one tab per
        selected team (tab = lead name). Returns a summary dict."""
        rows = self.sheet_rows()
        out = {"rows": len(rows), "master": None, "teams": {}}
        if not rows:
            return out
        with _sheet_write_lock:
            sheet = gspread_client().open_by_key(SPREADSHEET_ID)
            master = _open_or_create_ws(sheet, MASTER_STATS_TAB, MASTER_HEADER)
            out["master"] = upsert_stats_rows(master, MASTER_HEADER, rows,
                                              MASTER_ID_COLS)
            for lead in self.selected_leads:
                # same row minus the Team column
                trows = [[r[0]] + r[2:] for r in rows if r[1] == lead]
                if not trows:
                    continue
                ws = _open_or_create_ws(sheet, team_tab_title(lead), TEAM_HEADER)
                out["teams"][lead] = upsert_stats_rows(ws, TEAM_HEADER, trows,
                                                       TEAM_ID_COLS)
        return out

    # ---------------- send ----------------
    def send_all(self):
        """Fetch + aggregate, build every team email (always cached as a
        preview), send them if send_email, then write the sheet if
        write_sheet. Returns a summary dict."""
        started = time.time()
        self.fetch_and_aggregate()

        self._set_progress(phase="sending" if self.send_email else "building",
                           done=0, total=len(self.selected_leads))
        sent, skipped = [], []
        server = None
        if self.send_email:
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
            server.login(SMTP_USER, SMTP_PASS)

        try:
            chart_offset = 0
            for lead in self.selected_leads:
                tdomains = self.team_domains.get(lead, [])
                members = self.teams.get(lead, {}).get("members", [])
                if self.send_email and not members:
                    skipped.append({"team": lead, "reason": "no members in Teams sheet"})
                    self._set_progress(bump=True)
                    continue

                html, images = self.build_team_email(lead, tdomains, chart_offset)
                chart_offset += len(images)
                if html is None:
                    skipped.append({"team": lead, "reason": "no reportable domains"})
                    self._set_progress(bump=True)
                    continue

                self.save_preview(lead, self._inline_charts(html, images))

                recipients = []
                subject = self.subject_for(lead)
                if self.send_email:
                    recipients = (self.redirect_to
                                  or [email for _, email in members])
                    msg = MIMEMultipart("related")
                    msg["From"] = "Daily Clicks Report"
                    msg["To"] = ", ".join(recipients)
                    msg["Subject"] = subject

                    alt = MIMEMultipart("alternative")
                    alt.attach(MIMEText(html, "html"))
                    msg.attach(alt)

                    for cid, png in images:
                        img = MIMEImage(png, _subtype="png")
                        img.add_header("Content-ID", f"<{cid}>")
                        img.add_header("Content-Disposition", "inline",
                                       filename=f"{cid}.png")
                        msg.attach(img)

                    server.sendmail(SMTP_USER, recipients, msg.as_string())

                team_keys = [k for k in self.team_lines(tdomains) if k in self.domains]
                total = sum(self.domains[k]["clicks"] for k in team_keys)
                total_yday = sum(self.domains[k]["yday_clicks"] for k in team_keys)
                sent.append({
                    "team": lead,
                    "emailed": self.send_email,
                    "recipients": recipients,
                    "subject": subject,
                    "total_clicks": total,
                    "yesterday_clicks": total_yday,
                    "delta_vs_yesterday": total - total_yday,
                    "domains": len(team_keys),
                })
                self._set_progress(bump=True)
        finally:
            if server is not None:
                server.quit()

        sheet_stats = None
        if self.write_sheet:
            self._set_progress(phase="sheet", done=0, total=1)
            try:
                sheet_stats = self.write_sheet_stats()
            except Exception as e:
                sheet_stats = {"error": f"{type(e).__name__}: {e}"}
                self.log.append(f"sheet stats write failed: {e}")
        self._set_progress(phase="finished", done=1, total=1)

        return {
            "date": self.report_date,
            "is_today": self.is_today,
            "compare": self.compare_info(),
            "dry_run": self.dry_run,
            "emailed": self.send_email,
            "teams_sent": sent,
            "teams_skipped": skipped,
            "domain_failures": self.failures,
            "domains_processed": len(self.run_lines),
            "rows_fetched": len(self.all_rows),
            "sheet_stats": sheet_stats,
            "warnings": self.log,
            "duration_sec": round(time.time() - started, 1),
        }

    # ---------------- previews ----------------
    def build_preview(self, lead):
        """Browser-viewable HTML for ONE team (charts as data URIs).
        Never sends, never writes stats, fetches only this team's DBs.
        Stores the result in the preview cache."""
        self.restrict_to_lead(lead)
        self.fetch_and_aggregate()
        self._set_progress(phase="rendering", done=0, total=1)
        tdomains = self.team_domains.get(lead, [])
        html, images = self.build_team_email(lead, tdomains)
        if html is None:
            self._set_progress(phase="finished", done=1)
            return None
        out = self._inline_charts(html, images)
        self.save_preview(lead, out)
        self._set_progress(phase="finished", done=1)
        return out

    def build_preview_all(self):
        """Previews for EVERY selected team from a single fetch pass.
        Returns [(lead, html), ...] — never sends, never writes stats.
        Stores each result in the preview cache."""
        self.fetch_and_aggregate()
        self._set_progress(phase="rendering", done=0,
                           total=len(self.selected_leads))
        out = []
        for lead in self.selected_leads:
            tdomains = self.team_domains.get(lead, [])
            html, images = self.build_team_email(lead, tdomains)
            self._set_progress(bump=True)
            if html is None:
                continue
            inlined = self._inline_charts(html, images)
            self.save_preview(lead, inlined)
            out.append((lead, inlined))
        self._set_progress(phase="finished")
        return out

# ==========================================================
# JOB REGISTRY (async report runs + preview refreshes, with live progress)
# ==========================================================
_jobs = {}
_jobs_lock = threading.Lock()

def _job_new(kind, params):
    job_id = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _jobs[job_id] = {"status": "queued", "kind": kind, "params": params}
    return job_id

def _job_update(job_id, **fields):
    with _jobs_lock:
        _jobs[job_id].update(fields)

def _run_report_job(job_id, kwargs):
    _job_update(job_id, status="running",
                started_at=datetime.now(timezone.utc).isoformat())
    try:
        run = ReportRun(**kwargs)
        _job_update(job_id, progress=run.progress)   # live reference
        result = run.send_all()
        _job_update(job_id, status="done", result=result)
    except Exception:
        _job_update(job_id, status="failed", error=traceback.format_exc())
    finally:
        _job_update(job_id, finished_at=datetime.now(timezone.utc).isoformat())

def _run_preview_job(job_id, report_date, team):
    """Re-run the report for date+team, re-render and cache the previews.
    Never sends, never writes stats."""
    _job_update(job_id, status="running",
                started_at=datetime.now(timezone.utc).isoformat())
    try:
        if team == "all":
            run = ReportRun(report_date, dry_run=True)
            _job_update(job_id, progress=run.progress)
            previews = run.build_preview_all()
            leads = [lead for lead, _ in previews]
        else:
            run = ReportRun(report_date, team_filter=[team], dry_run=True)
            _job_update(job_id, progress=run.progress)
            lead = run.selected_leads[0] if run.selected_leads else None
            html = run.build_preview(lead) if lead else None
            leads = [lead] if html else []
        _job_update(job_id, status="done", result={
            "date": report_date, "team": team, "teams": leads,
            "compare": run.compare_info(),
            "domain_failures": run.failures, "warnings": run.log,
        })
    except Exception:
        _job_update(job_id, status="failed", error=traceback.format_exc())
    finally:
        _job_update(job_id, finished_at=datetime.now(timezone.utc).isoformat())

# ==========================================================
# FLASK APP
# ==========================================================
app = Flask(__name__)

def check_key():
    if not API_KEY:
        return True
    supplied = request.headers.get("X-API-Key") or request.args.get("key", "")
    return supplied == API_KEY

def parse_params():
    """Merge query params + optional JSON body into one dict."""
    p = {}
    p.update(request.args.to_dict())
    if request.is_json:
        p.update(request.get_json(silent=True) or {})
    return p

@app.route("/health")
def health():
    return jsonify({"status": "ok",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "report_tz": REPORT_TZ,
                    "sheet_stats": SHEET_STATS_ENABLED,
                    "local_time": local_clock_tz(datetime.now(timezone.utc))})

@app.route("/teams")
def list_teams():
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    try:
        _, teams, team_domains, _ = load_sheet_config()
    except Exception as e:
        return jsonify({"error": f"failed to read sheet: {e}"}), 502
    out = []
    for lead in sorted(team_domains):
        members = teams.get(lead, {}).get("members", [])
        out.append({
            "team": lead,
            "members": [{"name": n, "email": e} for n, e in members],
            "domains": [r["Domain"] for r in team_domains[lead]],
            "lines": [{"domain": r["Domain"], "employer_id": r["EmployerId"],
                       "key": r["Key"]} for r in team_domains[lead]],
        })
    return jsonify({"teams": out})

@app.route("/summary")
def summary():
    """Dashboard overview served from the LOCAL stats store — instant, and
    never touches the remote domain databases. Data exists for any date a
    real (non-dry) run has completed. For TODAY, the latest intraday run's
    "yesterday as of HH:MM" snapshot is returned as `asof`."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()
    team = (p.get("team") or "").strip()
    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400

    try:
        _, _, team_domains, _ = load_sheet_config()
    except Exception as e:
        return jsonify({"error": f"failed to read sheet: {e}"}), 502

    if team and team.lower() != "all":
        lead = resolve_lead(team, team_domains)
        if lead is None:
            return jsonify({"error": f"unknown team '{team}'"}), 400
        wanted_leads = [lead]
    else:
        wanted_leads = sorted(team_domains)

    # one line per (domain, employer ids); a line may belong to several teams
    key_teams, key_info = {}, {}
    for lead in wanted_leads:
        for r in team_domains[lead]:
            key_teams.setdefault(r["Key"], [])
            if lead not in key_teams[r["Key"]]:
                key_teams[r["Key"]].append(lead)
            key_info[r["Key"]] = r
    keys = list(key_teams)
    # rows stored before Key existed only carry Domain; they can only be
    # matched to lines without an employer filter (key == domain)
    legacy_domains = [k for k in keys if k == key_info[k]["Domain"]]
    line_filter = {"$or": [{"Key": {"$in": keys}},
                           {"Key": {"$exists": False},
                            "Domain": {"$in": legacy_domains}}]}

    end_dt = datetime.strptime(report_date, "%Y-%m-%d")
    dates = [(end_dt - timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(SPARK_DAYS - 1, -1, -1)]

    try:
        docs = list(stats_col.aggregate([
            {"$match": {"Date": {"$in": dates}, **line_filter}},
            {"$group": {"_id": {"Date": "$Date",
                                "Key": {"$ifNull": ["$Key", "$Domain"]}},
                        "clicks": {"$sum": "$ViewsCount"},
                        "ips": {"$sum": "$UniqueIpCount"}}},
        ], maxTimeMS=MONGO_MAX_TIME_MS))
    except Exception:
        return jsonify({"error": traceback.format_exc()}), 500

    daily = {dt: {"clicks": 0, "ips": 0} for dt in dates}
    per_team = {lead: {"clicks": 0, "ips": 0, "domains": 0,
                       "domains_tracked": len(team_domains[lead])}
                for lead in wanted_leads}
    per_line_today = {}
    for doc in docs:
        dt, key = doc["_id"]["Date"], doc["_id"]["Key"]
        leads = key_teams.get(key)
        if not leads:
            continue
        clicks, ips = to_int(doc["clicks"]), to_int(doc["ips"])
        if dt in daily:
            daily[dt]["clicks"] += clicks
            daily[dt]["ips"] += ips
        if dt == report_date:
            for lead in leads:
                per_team[lead]["clicks"] += clicks
                per_team[lead]["ips"] += ips
                per_team[lead]["domains"] += 1
            r = key_info[key]
            per_line_today[key] = {"domain": r["Domain"],
                                   "employer_id": r["EmployerId"],
                                   "clicks": clicks, "ips": ips,
                                   "team": ", ".join(leads)}

    prev_date = (end_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    today_t = daily.get(report_date, {"clicks": 0, "ips": 0})
    prev_t = daily.get(prev_date, {"clicks": 0, "ips": 0})

    # "yesterday as of HH:MM" — only meaningful while the date IS today
    # (once the day is over, stored stats are full-day and so is `prev`)
    asof = None
    if report_date == today_utc():
        try:
            adocs = list(asof_col.find(
                {"Date": report_date, **line_filter},
                {"_id": 0}, max_time_ms=MONGO_MAX_TIME_MS))
        except Exception:
            adocs = []
        if adocs:
            latest = max(adocs, key=lambda x: str(x.get("InsertedAt") or ""))
            y_clicks = y_ips = n = 0
            for x in adocs:
                k = x.get("Key") or x.get("Domain")
                leads = key_teams.get(k)
                if not leads:
                    continue
                n += 1
                yc = to_int(x.get("YdayClicks"))
                y_clicks += yc
                y_ips += to_int(x.get("YdayUniqueIps"))
                for lead in leads:
                    per_team[lead]["yday_asof"] = (per_team[lead].get("yday_asof", 0)
                                                   + yc)
                if k in per_line_today:
                    per_line_today[k]["yday_asof"] = yc
            asof = {
                "cutoff": latest.get("Cutoff"),
                "yesterday_date": latest.get("YesterdayDate"),
                "yesterday_clicks": y_clicks,
                "yesterday_ips": y_ips,
                "delta": today_t["clicks"] - y_clicks,
                "domains": n,
            }

    top = sorted(per_line_today.items(),
                 key=lambda kv: kv[1]["clicks"], reverse=True)[:10]

    return jsonify({
        "date": report_date,
        "is_today": report_date == today_utc(),
        "has_data": today_t["clicks"] > 0 or today_t["ips"] > 0
                    or bool(per_line_today),
        "totals": {"clicks": today_t["clicks"], "ips": today_t["ips"],
                   "domains_reporting": len(per_line_today),
                   "domains_tracked": len(keys)},
        "prev": {"date": prev_date, "clicks": prev_t["clicks"],
                 "ips": prev_t["ips"],
                 "delta": today_t["clicks"] - prev_t["clicks"]},
        "asof": asof,
        "daily": [{"date": dt, **daily[dt]} for dt in dates],
        "teams": [{"team": lead, **per_team[lead]}
                  for lead in sorted(per_team,
                                     key=lambda l: per_team[l]["clicks"],
                                     reverse=True)],
        "top_domains": [{"key": k, **v} for k, v in top],
    })

def _start_run(force_no_email=False):
    """Shared by /run and /stats."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()

    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400

    teams_param = p.get("teams") or ""
    if isinstance(teams_param, list):
        team_filter = [str(t) for t in teams_param]
    else:
        team_filter = [t for t in str(teams_param).split(",") if t.strip()]
    if any(t.strip().lower() == "all" for t in team_filter):
        team_filter = []
    dry_run = is_true(p.get("dry_run"))
    redirect_to = [e.strip() for e in str(p.get("to") or "").split(",") if e.strip()]
    mode = str(p.get("mode") or "sync").strip().lower()
    send_email = False if force_no_email else opt_bool(p.get("email"), True)
    write_sheet = opt_bool(p.get("sheet"), True)

    kwargs = {
        "report_date": report_date,
        "team_filter": team_filter or None,
        "dry_run": dry_run,
        "redirect_to": redirect_to or None,
        "send_email": send_email,
        "write_sheet": write_sheet,
    }

    if mode == "async":
        job_id = _job_new("report", {"date": report_date,
                                     "teams": team_filter or "all",
                                     "dry_run": dry_run,
                                     "email": send_email,
                                     "sheet": write_sheet})
        threading.Thread(target=_run_report_job, args=(job_id, kwargs),
                         daemon=True).start()
        return jsonify({"job_id": job_id, "status": "queued",
                        "poll": f"/jobs/{job_id}"}), 202

    try:
        run = ReportRun(**kwargs)
        result = run.send_all()
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception:
        return jsonify({"error": traceback.format_exc()}), 500

@app.route("/run", methods=["GET", "POST"])
def run_report():
    return _start_run()

@app.route("/stats", methods=["GET", "POST"])
def run_stats_only():
    """Stats + sheet only — never sends email."""
    return _start_run(force_no_email=True)

@app.route("/jobs/<job_id>")
def job_status(job_id):
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    with _jobs_lock:
        job = _jobs.get(job_id)
        job = dict(job) if job else None
        if job and isinstance(job.get("progress"), dict):
            job["progress"] = dict(job["progress"])   # snapshot for JSON
    if job is None:
        return jsonify({"error": "unknown job id"}), 404
    return jsonify(job)

@app.route("/jobs")
def jobs_list():
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    with _jobs_lock:
        items = []
        for jid, j in _jobs.items():
            item = {k: v for k, v in j.items() if k not in ("result",)}
            if isinstance(item.get("progress"), dict):
                item["progress"] = dict(item["progress"])
            item["job_id"] = jid
            res = j.get("result") or {}
            item["teams_sent"] = len(res.get("teams_sent", res.get("teams", [])))
            items.append(item)
    items.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    return jsonify({"jobs": items[:20]})

# ---------------- preview: page, cached data, refresh job ----------------
def _preview_docs(report_date, leads):
    """Cached previews for (date, leads) in the given order."""
    try:
        docs = list(previews_col.find(
            {"Date": report_date, "Team": {"$in": leads}},
            max_time_ms=MONGO_MAX_TIME_MS))
    except Exception:
        docs = []
    by_team = {d["Team"]: d for d in docs}
    out = []
    for lead in leads:
        d = by_team.get(lead)
        if not d:
            continue
        out.append({
            "team": lead,
            "html": d.get("Html", ""),
            "subject": d.get("Subject", ""),
            "as_of": d.get("AsOf"),
            "compare_mode": d.get("CompareMode"),
            "generated_at": iso_utc(d.get("GeneratedAt")),
        })
    return out

@app.route("/preview")
def preview():
    """The preview PAGE (shell). Data is loaded by the page from
    /preview/data; a cache miss or the Refresh button starts a job via
    /preview/refresh and the page shows a skeleton + live progress.
    raw=true keeps the old behaviour: build synchronously, return bare HTML."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()
    team = (p.get("team") or "").strip()
    key = request.args.get("key", "")

    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400

    if is_true(p.get("raw")):
        try:
            run = ReportRun(report_date,
                            team_filter=[team] if team and team.lower() != "all" else None,
                            dry_run=True)
            lead = run.selected_leads[0] if run.selected_leads else None
            if lead is None:
                return jsonify({"error": "no teams found in sheet"}), 404
            html = run.build_preview(lead)
            if html is None:
                return jsonify({"error": f"team '{lead}' has no reportable domains"}), 404
            return Response(html, mimetype="text/html")
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception:
            return jsonify({"error": traceback.format_exc()}), 500

    # resolve the team name now so the page + chips use the canonical one
    try:
        _, _, team_domains, _ = load_sheet_config()
    except Exception as e:
        return jsonify({"error": f"failed to read sheet: {e}"}), 502
    if team.lower() == "all":
        team_param = "all"
    elif not team:
        # blank team -> first team in the sheet (old behaviour)
        leads = list(team_domains)
        if not leads:
            return jsonify({"error": "no teams found in sheet"}), 404
        team_param = leads[0]
    else:
        lead = resolve_lead(team, team_domains)
        if lead is None:
            return jsonify({"error": f"unknown team '{team}'. Available: "
                                     f"{', '.join(sorted(team_domains))}"}), 400
        team_param = lead

    cfg = {"date": report_date, "team": team_param, "key": key,
           "today": today_utc(), "yesterday": yesterday_utc(),
           "is_today": report_date == today_utc()}
    return Response(PREVIEW_SHELL_HTML.replace("__CFG__", json.dumps(cfg)),
                    mimetype="text/html")

@app.route("/preview/data")
def preview_data():
    """Cached previews as JSON: {date, team, previews:[{team, html, ...}],
    missing:[teams with no cache yet]}."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()
    team = (p.get("team") or "").strip()
    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400
    try:
        _, _, team_domains, _ = load_sheet_config()
    except Exception as e:
        return jsonify({"error": f"failed to read sheet: {e}"}), 502

    if not team or team.lower() == "all":
        leads = list(team_domains)
        team = "all"
    else:
        lead = resolve_lead(team, team_domains)
        if lead is None:
            return jsonify({"error": f"unknown team '{team}'"}), 400
        leads = [lead]
        team = lead

    previews = _preview_docs(report_date, leads)
    found = {x["team"] for x in previews}
    generated = [x["generated_at"] for x in previews if x.get("generated_at")]
    return jsonify({
        "date": report_date,
        "team": team,
        "is_today": report_date == today_utc(),
        "previews": previews,
        "missing": [l for l in leads if l not in found],
        "generated_at": max(generated) if generated else None,
        "as_of": next((x["as_of"] for x in previews if x.get("as_of")), None),
    })

@app.route("/preview/refresh", methods=["GET", "POST"])
def preview_refresh():
    """Start an async job that re-runs the report for date+team and stores
    fresh previews. Returns a job id to poll at /jobs/<id>."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()
    team = (p.get("team") or "").strip()
    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400
    try:
        _, _, team_domains, _ = load_sheet_config()
    except Exception as e:
        return jsonify({"error": f"failed to read sheet: {e}"}), 502
    if not team or team.lower() == "all":
        team = "all"
    else:
        lead = resolve_lead(team, team_domains)
        if lead is None:
            return jsonify({"error": f"unknown team '{team}'"}), 400
        team = lead

    job_id = _job_new("preview", {"date": report_date, "teams": team})
    threading.Thread(target=_run_preview_job, args=(job_id, report_date, team),
                     daemon=True).start()
    return jsonify({"job_id": job_id, "status": "queued",
                    "poll": f"/jobs/{job_id}"}), 202

@app.route("/diagnose")
def diagnose():
    """Per-line health for a date: which db each line resolves to, whether
    creds exist, clicks found, yesterday's comparison figure under the
    active rule, and the exact failure reason.
    Read-only: never sends email, never writes stats or the sheet."""
    if not check_key():
        return jsonify({"error": "invalid or missing API key"}), 401
    p = parse_params()
    teams_param = str(p.get("teams") or "")
    team_filter = [t for t in teams_param.split(",") if t.strip()]
    if any(t.strip().lower() == "all" for t in team_filter):
        team_filter = []
    team_filter = team_filter or None
    try:
        report_date = resolve_date(p.get("date"))
    except ValueError:
        return jsonify({"error": DATE_ERROR.format(p.get("date"))}), 400
    try:
        run = ReportRun(report_date, team_filter=team_filter, dry_run=True)
        run.fetch_and_aggregate()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception:
        return jsonify({"error": traceback.format_exc()}), 500

    report = []
    for key, ln in run.run_lines.items():
        entry = {
            "domain": ln["Domain"],
            "employer_id": ln["EmployerId"] or "(all)",
            "key": key,
            "team": ", ".join(ln["Leads"]),
            "teams": ln["Leads"],
            "type": ln["Domain Type"],
            "database": db_for(ln) or "(unresolved)",
        }
        if key in run.failures:
            entry["status"] = "FAILED"
            entry["reason"] = run.failures[key]
        else:
            d = run.domains.get(key, {})
            entry["status"] = "ok"
            entry["clicks_on_date"] = d.get("clicks", 0)
            entry["unique_ips"] = d.get("ips", 0)
            entry["yesterday_clicks"] = d.get("yday_clicks", 0)
            entry["yesterday_unique"] = d.get("yday_ips", 0)
            entry["delta_vs_yesterday"] = d.get("clicks", 0) - d.get("yday_clicks", 0)
            entry["postings_expires"] = run.postings_expires.get(key)
        report.append(entry)
    return jsonify({"date": report_date, "compare": run.compare_info(),
                    "domains": report, "warnings": run.log})

# ==========================================================
# PREVIEW SHELL — Today / Yesterday chips, team tabs, Refresh, cached
# previews from /preview/data, shimmer skeleton + live progress while a
# refresh job runs. Plain string; __CFG__ is replaced with JSON.
# ==========================================================
PREVIEW_SHELL_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Email preview</title>
<style>
 *{box-sizing:border-box}
 :root{--ink:#1d1d1f;--grey:#86868b;--grey-l:#aeaeb2;--faint:#c7c7cc;--hair:#e8e8ed;--bg:#f5f5f7;--green:#30d158;--green-d:#248a3d;--red-d:#d70015}
 html,body{height:100%}
 body{margin:0;background:var(--bg);font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display','Segoe UI',Helvetica,Arial,sans-serif;display:flex;flex-direction:column;-webkit-font-smoothing:antialiased;color:var(--ink)}
 header{display:flex;align-items:center;gap:10px;padding:12px 16px;background:rgba(255,255,255,.88);backdrop-filter:blur(14px);border-bottom:1px solid var(--hair);position:sticky;top:0;z-index:5;flex-wrap:wrap}
 .hl{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
 .title{font-size:15px;font-weight:600;letter-spacing:-0.01em}
 .date{font-size:12px;color:var(--grey)}
 .note{font-size:12px;font-weight:600;color:var(--green-d)}
 .dchips,.tabs{display:flex;gap:6px;flex-wrap:wrap}
 .tabs{margin-left:auto}
 .dchip,.tab{border:0;border-radius:100px;padding:7px 13px;font-size:12px;font-weight:600;text-decoration:none;background:var(--bg);color:var(--ink);cursor:pointer;font-family:inherit;transition:background .15s;white-space:nowrap}
 .dchip:hover,.tab:hover{background:var(--hair)}
 .dchip.active{background:var(--green-d);color:#fff}
 .tab.active{background:var(--ink);color:#fff}
 .refresh{border:0;border-radius:100px;padding:8px 15px;font-size:12px;font-weight:600;background:var(--ink);color:#fff;cursor:pointer;font-family:inherit;white-space:nowrap;display:inline-flex;align-items:center;gap:6px}
 .refresh:disabled{opacity:.45;cursor:default}
 .refresh .ico{display:inline-block;transition:transform .3s}
 .refresh.busy .ico{animation:spin 1s linear infinite}
 @keyframes spin{to{transform:rotate(360deg)}}
 .meta{width:100%;font-size:12px;color:var(--grey);display:flex;gap:12px;flex-wrap:wrap;align-items:center}
 .meta .subject{color:var(--grey-l);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%}
 .stage{flex:1;display:flex;flex-direction:column;min-height:0}
 .frame{flex:1;width:100%;border:0;display:none;background:#fff}
 .frame.active{display:block}
 .err{margin:40px auto;max-width:560px;background:#fff;border-radius:22px;padding:24px;font-size:13px;color:var(--red-d);white-space:pre-wrap}
 .err b{display:block;color:var(--ink);margin-bottom:8px}
 .err button{margin-top:14px}

 /* ---------- skeleton (mirrors the email layout) ---------- */
 .skel{display:none;flex:1;overflow:auto;background:#fff}
 .skel.show{display:block}
 .sk-wrap{max-width:580px;margin:0 auto;padding:36px 20px 80px}
 .sk{position:relative;overflow:hidden;background:#eeeef1;border-radius:10px}
 .sk::after{content:"";position:absolute;inset:0;transform:translateX(-100%);background:linear-gradient(90deg,rgba(255,255,255,0) 0,rgba(255,255,255,.65) 50%,rgba(255,255,255,0) 100%);animation:shimmer 1.5s ease-in-out infinite}
 @keyframes shimmer{100%{transform:translateX(100%)}}
 .sk-card{background:var(--bg);border-radius:26px;padding:32px 26px;margin-top:20px}
 .sk-card .sk{background:#e6e6ea}
 .sk-center{margin-left:auto;margin-right:auto}
 .sk-row{display:flex;gap:16px;align-items:flex-start}
 .sk-row>div{flex:1}
 .sk-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:0;margin-top:44px;border-top:1px solid #f2f2f4;border-bottom:1px solid #f2f2f4;padding:18px 0}
 .sk-stats>div{display:flex;flex-direction:column;align-items:center;gap:8px}
 .sk-stats>div+div{border-left:1px solid #f2f2f4}
 .sk-nums{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:28px}
 .sk-nums>div{display:flex;flex-direction:column;gap:8px}
 .sk-nums>div+div{border-left:1px solid var(--hair);padding-left:14px}
 .sk-strip{display:grid;grid-template-columns:repeat(7,1fr);gap:6px;margin-top:14px}
 .sk-strip>div{display:flex;flex-direction:column;align-items:center;gap:6px}
 .sk-lines{margin-top:26px;display:flex;flex-direction:column;gap:12px}
 .sk-line{display:flex;justify-content:space-between;align-items:center;padding-top:11px;border-top:1px solid var(--hair)}
 @media(max-width:480px){.sk-wrap{padding:28px 12px 60px}.sk-card{padding:24px 18px;border-radius:22px}.sk-nums{grid-template-columns:1fr}.sk-nums>div+div{border-left:0;padding-left:0;border-top:1px solid var(--hair);padding-top:14px}}

 /* progress (same as the dashboard) */
 .prog{position:sticky;top:0;background:#fff;padding:14px 0 18px;z-index:2}
 .prog-track{height:8px;background:var(--bg);border-radius:100px;overflow:hidden}
 .prog-fill{height:100%;width:0%;background:var(--green-d);border-radius:100px;transition:width .4s ease}
 .prog-lbl{font-size:12px;color:var(--grey);margin-top:8px;display:flex;justify-content:space-between;gap:10px}
 .prog-lbl b{color:var(--ink);font-weight:600}
</style></head><body>
<header>
 <div class="hl">
  <span class="title">Email preview</span>
  <span class="date" id="dateLbl"></span>
  <span class="note" id="note"></span>
 </div>
 <div class="dchips">
  <a class="dchip" id="cY">Yesterday</a>
  <a class="dchip" id="cT">Today</a>
 </div>
 <div class="tabs" id="tabs"></div>
 <button class="refresh" id="refreshBtn" onclick="refresh()"><span class="ico">&#8635;</span><span>Refresh</span></button>
 <div class="meta" id="meta"></div>
</header>
<div class="stage" id="stage">
 <div class="skel" id="skel">
  <div class="sk-wrap">
   <div class="prog">
    <div class="prog-track"><div class="prog-fill" id="pfill"></div></div>
    <div class="prog-lbl"><span id="plbl">Loading preview&hellip;</span><b id="ppct"></b></div>
   </div>
   <div class="sk sk-center" style="width:180px;height:26px"></div>
   <div class="sk sk-center" style="width:300px;height:14px;margin-top:12px"></div>
   <div class="sk sk-center" style="width:150px;height:14px;margin-top:56px"></div>
   <div class="sk sk-center" style="width:230px;height:84px;margin-top:18px;border-radius:18px"></div>
   <div class="sk sk-center" style="width:210px;height:14px;margin-top:16px"></div>
   <div class="sk-stats">
    <div><div class="sk" style="width:64px;height:22px"></div><div class="sk" style="width:90px;height:12px"></div></div>
    <div><div class="sk" style="width:64px;height:22px"></div><div class="sk" style="width:90px;height:12px"></div></div>
    <div><div class="sk" style="width:64px;height:22px"></div><div class="sk" style="width:90px;height:12px"></div></div>
   </div>
   <div style="height:28px"></div>
   <div class="sk-card" id="skcards"></div>
  </div>
 </div>
 <div id="frames" style="display:contents"></div>
</div>
<script>
const CFG=__CFG__;
const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function qp(obj){const p=new URLSearchParams();if(CFG.key)p.set('key',CFG.key);Object.entries(obj||{}).forEach(([k,v])=>{if(v!==''&&v!=null)p.set(k,v)});return p.toString();}
async function api(path,params){const r=await fetch(path+'?'+qp(params));return r.json();}
function timeAgo(iso){if(!iso)return '';const s=Math.max(0,(Date.now()-new Date(iso).getTime())/1e3);if(s<60)return 'just now';if(s<3600)return Math.round(s/60)+' min ago';if(s<86400)return Math.round(s/3600)+' h ago';return Math.round(s/86400)+' d ago';}
function niceDate(d){try{return new Date(d+'T00:00:00Z').toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',timeZone:'UTC'})}catch(e){return d}}

// header
$('dateLbl').textContent=niceDate(CFG.date)+' · '+CFG.date;
$('cY').href='/preview?'+qp({date:'yesterday',team:CFG.team});
$('cT').href='/preview?'+qp({date:'today',team:CFG.team});
$('cY').classList.toggle('active',CFG.date===CFG.yesterday);
$('cT').classList.toggle('active',CFG.date===CFG.today);
document.title='Preview · '+CFG.date+(CFG.team!=='all'?' · '+CFG.team:'');

// skeleton cards (two domain cards, like the email)
function skCard(){return `
  <div class="sk-row"><div><div class="sk" style="width:160px;height:15px"></div><div class="sk" style="width:90px;height:12px;margin-top:8px"></div></div><div style="flex:0 0 auto"><div class="sk" style="width:130px;height:26px;border-radius:100px"></div></div></div>
  <div class="sk-nums">
   <div><div class="sk" style="width:110px;height:40px"></div><div class="sk" style="width:80px;height:12px"></div><div class="sk" style="width:60px;height:10px"></div></div>
   <div><div class="sk" style="width:90px;height:40px"></div><div class="sk" style="width:80px;height:12px"></div><div class="sk" style="width:60px;height:10px"></div></div>
   <div><div class="sk" style="width:90px;height:40px"></div><div class="sk" style="width:110px;height:12px"></div><div class="sk" style="width:90px;height:10px"></div></div>
  </div>
  <div class="sk" style="width:100%;height:120px;margin-top:26px;border-radius:14px"></div>
  <div class="sk" style="width:180px;height:11px;margin-top:12px"></div>
  <div class="sk-strip">${'<div><div class="sk" style="width:10px;height:10px"></div><div class="sk" style="width:26px;height:12px"></div><div class="sk" style="width:20px;height:9px"></div></div>'.repeat(7)}</div>
  <div class="sk-lines">${'<div class="sk-line"><div class="sk" style="width:45%;height:14px"></div><div class="sk" style="width:60px;height:14px"></div></div>'.repeat(4)}</div>`;}
$('skcards').innerHTML=skCard()+'</div><div class="sk-card">'+skCard();

let busy=false,pollTimer=null;
function showSkel(){$('skel').classList.add('show');$('frames').style.display='none';$('refreshBtn').disabled=true;$('refreshBtn').classList.add('busy');busy=true;}
function hideSkel(){$('skel').classList.remove('show');$('frames').style.display='contents';$('refreshBtn').disabled=false;$('refreshBtn').classList.remove('busy');busy=false;}
function paintProg(p){
 const phase=(p&&p.phase)||'queued';let pct=0,lbl='Queued…';
 if(p&&p.total>0)pct=Math.min(100,p.done/p.total*100);
 if(phase==='queued'||phase==='pending'){pct=3;lbl='Starting run…';}
 else if(phase==='fetching'){lbl=`Running report for domains ${p.done}/${p.total}`;pct=3+pct*0.82;}
 else if(phase==='aggregating'){pct=88;lbl='Aggregating results…';}
 else if(phase==='rendering'){lbl=`Rendering team emails ${p.done}/${p.total}`;pct=90+pct*0.09;}
 else if(phase==='finished'){pct=100;lbl='Finished — loading preview';}
 else if(phase==='loading'){pct=100;lbl='Loading cached preview…';}
 $('pfill').style.width=pct.toFixed(0)+'%';$('plbl').textContent=lbl;$('ppct').textContent=pct.toFixed(0)+'%';
}
function showErr(msg){
 hideSkel();
 $('frames').innerHTML=`<div class="err"><b>Couldn't build the preview</b>${esc(msg)}<br><button class="refresh" onclick="refresh()"><span class="ico">&#8635;</span><span>Try again</span></button></div>`;
}
function render(d){
 const list=d.previews||[];
 if(!list.length){showErr('No reportable domains for '+CFG.date+(CFG.team!=='all'?' in team '+CFG.team:'')+'.');return;}
 $('tabs').innerHTML=list.length>1?list.map((p,i)=>`<button class="tab${i?'':' active'}" data-i="${i}">${esc(p.team)}</button>`).join(''):'';
 $('frames').innerHTML=list.map((p,i)=>`<iframe class="frame${i?'':' active'}" data-i="${i}" title="Team ${esc(p.team)}"></iframe>`).join('');
 list.forEach((p,i)=>{document.querySelector(`.frame[data-i="${i}"]`).srcdoc=p.html;});
 document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x===t));
  document.querySelectorAll('.frame').forEach(f=>f.classList.toggle('active',f.dataset.i===t.dataset.i));
  paintMeta(list[+t.dataset.i]);
 }));
 $('note').textContent=d.as_of?('as of '+d.as_of):(CFG.is_today?'':'full day');
 paintMeta(list[0],d);
 if(d.missing&&d.missing.length){$('meta').insertAdjacentHTML('beforeend',`<span style="color:#b25000">No cached preview yet for: ${esc(d.missing.join(', '))} — hit Refresh.</span>`);}
 hideSkel();
}
function paintMeta(p,d){
 const gen=p.generated_at||(d&&d.generated_at);
 $('meta').innerHTML=`<span>Generated <b>${esc(timeAgo(gen))}</b>${gen?' · '+esc(new Date(gen).toLocaleString()):''}</span><span class="subject" title="${esc(p.subject||'')}">Subject · ${esc(p.subject||'')}</span>`;
}
async function load(auto){
 showSkel();paintProg({phase:'loading'});
 try{
  const d=await api('/preview/data',{date:CFG.date,team:CFG.team});
  if(d.error){showErr(d.error);return;}
  const need=!d.previews.length||(d.missing&&d.missing.length);
  if(need&&auto){refresh();return;}
  render(d);
 }catch(e){showErr(String(e));}
}
async function refresh(){
 if(busy&&pollTimer)return;
 showSkel();paintProg({phase:'queued'});
 try{
  const j=await api('/preview/refresh',{date:CFG.date,team:CFG.team});
  if(j.error){showErr(j.error);return;}
  poll(j.job_id);
 }catch(e){showErr(String(e));}
}
async function poll(id){
 for(let i=0;i<400;i++){
  await new Promise(r=>{pollTimer=setTimeout(r,1200)});
  let j;try{j=await api('/jobs/'+id,{});}catch(e){continue;}
  paintProg(j.progress||{phase:j.status});
  if(j.status==='done'){pollTimer=null;const fails=Object.keys((j.result||{}).domain_failures||{});await load(false);if(fails.length)$('meta').insertAdjacentHTML('beforeend',`<span style="color:#d70015">${fails.length} line(s) failed: ${esc(fails.join(', '))}</span>`);return;}
  if(j.status==='failed'){pollTimer=null;showErr(j.error||'job failed');return;}
 }
 pollTimer=null;showErr('Timed out waiting for the refresh job.');
}
load(true);
</script>
</body></html>"""

# ==========================================================
# DASHBOARD — same aesthetic as the email: hero number, hairline stats,
# clicks/unique chart with day strip, grey team cards (click -> preview),
# receipt rows and the dark "Grow this report" card. Overview served from
# /summary (instant); skeleton loaders while data loads; ?key= forwarded.
# ==========================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Daily Clicks Report</title>
<style>
 *{box-sizing:border-box}
 :root{--ink:#1d1d1f;--grey:#86868b;--grey-l:#aeaeb2;--faint:#c7c7cc;--hair:#e8e8ed;--bg:#f5f5f7;--green:#30d158;--green-d:#248a3d;--red:#ff453a;--red-d:#d70015}
 html{scroll-behavior:smooth}
 body{font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display','Segoe UI',Helvetica,Arial,sans-serif;background:var(--bg);color:var(--ink);margin:0;padding:0;-webkit-font-smoothing:antialiased}
 .wrap{max-width:880px;margin:0 auto;padding:0 20px 80px}
 @media(max-width:520px){.wrap{padding:0 12px 60px}}
 a{color:inherit}

 /* header */
 header{padding:44px 0 8px;display:flex;align-items:flex-end;justify-content:space-between;gap:14px;flex-wrap:wrap}
 h1{font-size:clamp(24px,6vw,30px);font-weight:600;letter-spacing:-0.025em;margin:0;line-height:1.05}
 .sub{font-size:13px;color:var(--grey);margin-top:8px}
 .status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--faint);margin-right:7px;vertical-align:1px}
 .status-dot.ok{background:var(--green)}
 .status-dot.err{background:var(--red)}
 .hdr-chips{display:flex;gap:6px;align-items:center;flex-wrap:wrap}

 /* cards */
 .card{background:#fff;border-radius:26px;padding:30px 28px;margin-top:20px;box-shadow:0 1px 2px rgba(0,0,0,.04);min-width:0}
 .card-title{font-size:12px;font-weight:600;color:var(--grey-l);letter-spacing:0.06em;text-transform:uppercase;margin-bottom:18px;display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}
 .card-title .act-link{font-size:11px;letter-spacing:0;text-transform:none;font-weight:600;color:var(--ink);background:var(--bg);border-radius:100px;padding:5px 11px;text-decoration:none;cursor:pointer;border:0;font-family:inherit}
 .two{display:grid;grid-template-columns:1fr 1fr;gap:20px}
 @media(max-width:720px){.two{grid-template-columns:1fr}.card{padding:24px 18px;border-radius:22px}}

 /* skeleton shimmer */
 .sk{position:relative;overflow:hidden;background:#eeeef1;border-radius:10px}
 .sk::after{content:"";position:absolute;inset:0;transform:translateX(-100%);background:linear-gradient(90deg,rgba(255,255,255,0) 0,rgba(255,255,255,.65) 50%,rgba(255,255,255,0) 100%);animation:shimmer 1.5s ease-in-out infinite}
 @keyframes shimmer{100%{transform:translateX(100%)}}
 .sk-c{margin-left:auto;margin-right:auto}

 /* hero (mirrors the email) */
 .hero{text-align:center;padding:12px 0 8px}
 .headline{font-size:15px;font-weight:600;padding-bottom:18px}
 .headline.up{color:var(--green-d)} .headline.down{color:var(--red-d)} .headline.flat{color:var(--grey)}
 .hero-num{font-size:clamp(58px,16vw,88px);font-weight:300;letter-spacing:-0.05em;line-height:.95}
 .hero-sub{font-size:16px;color:var(--grey);padding-top:16px;line-height:1.4}
 .stats{display:grid;grid-template-columns:repeat(3,1fr);margin:40px 0 8px;border-top:1px solid #f2f2f4;border-bottom:1px solid #f2f2f4;padding:18px 0}
 .stat{text-align:center;padding:4px 6px;min-width:0}
 .stat+.stat{border-left:1px solid #f2f2f4}
 .stat-num{font-size:22px;font-weight:500;letter-spacing:-0.02em;white-space:nowrap}
 .stat-num .of{font-size:14px;color:var(--grey-l);font-weight:400}
 .stat-num.up{color:var(--green-d)} .stat-num.down{color:var(--red-d)}
 .stat-lbl{font-size:12px;color:var(--grey);padding-top:6px;line-height:1.3}
 @media(max-width:520px){.hero-sub{font-size:14px}.stat-num{font-size:17px}.stat-lbl{font-size:10px}.stats{margin-top:30px}}

 /* chart + day strip */
 .chart{margin-top:30px}
 .chart svg{width:100%;height:150px;display:block}
 .legend{font-size:11px;color:var(--grey-l);padding-top:10px;letter-spacing:.01em}
 .legend b{font-weight:700}
 .dstrip{display:grid;grid-template-columns:repeat(14,1fr);margin-top:12px;text-align:center}
 .d-lbl{font-size:11px;color:var(--faint)}
 .d-val{font-size:12px;color:var(--grey-l);padding-top:4px;letter-spacing:-0.01em}
 .d-ip{font-size:10px;color:var(--faint);padding-top:2px}
 .dcell.last .d-lbl{font-weight:600}
 .dcell.last .d-val{color:var(--ink);font-weight:600}
 .dcell.last .d-ip{color:var(--grey)}
 @media(max-width:600px){.d-val{font-size:9px;letter-spacing:-0.03em}.d-ip{display:none}.d-lbl{font-size:9px}.chart svg{height:120px}}
 .nodata{font-size:14px;color:var(--grey);padding:22px 0;text-align:center;line-height:1.6}
 .nodata b{color:var(--ink);font-weight:600}

 /* team cards */
 .tgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
 @media(max-width:520px){.tgrid{grid-template-columns:1fr}}
 .tcard{background:var(--bg);border-radius:20px;padding:18px 18px 16px;cursor:pointer;border:1.5px solid transparent;transition:border-color .15s,transform .08s,background .15s;min-width:0}
 .tcard:hover{border-color:var(--hair);background:#fff}
 .tcard:active{transform:scale(.985)}
 .tcard.active{border-color:var(--ink);background:#fff}
 .tname{font-size:14px;font-weight:600;letter-spacing:-0.01em;display:flex;justify-content:space-between;align-items:center;gap:8px}
 .tname span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
 .tname .tpill{font-size:11px;font-weight:600;color:var(--grey);background:#fff;border-radius:100px;padding:4px 9px;white-space:nowrap;flex:0 0 auto}
 .tcard.active .tpill,.tcard:hover .tpill{background:var(--bg)}
 .tnum{font-size:clamp(28px,7vw,34px);font-weight:300;letter-spacing:-0.035em;line-height:.95;padding-top:16px}
 .tcap{font-size:12px;color:var(--grey);padding-top:8px;line-height:1.35}
 .ttrack{height:6px;background:#fff;border-radius:100px;overflow:hidden;margin-top:14px}
 .tcard.active .ttrack,.tcard:hover .ttrack{background:var(--bg)}
 .tfill{height:100%;background:var(--ink);border-radius:100px;transition:width .5s cubic-bezier(.2,.7,.2,1)}
 .tcard.active .tfill{background:var(--green-d)}
 .tacts{display:flex;justify-content:space-between;align-items:center;margin-top:14px;gap:8px}
 .tbtn{border:0;border-radius:100px;padding:6px 11px;font-size:11px;font-weight:600;background:#fff;color:var(--ink);cursor:pointer;font-family:inherit}
 .tcard.active .tbtn,.tcard:hover .tbtn{background:var(--bg)}
 .tcard.active .tbtn{background:var(--ink);color:#fff}
 .tlink{font-size:11px;font-weight:600;color:var(--green-d);white-space:nowrap}

 /* receipt rows (top domains) */
 .tscroll{overflow-x:auto;-webkit-overflow-scrolling:touch}
 table.rcpt{width:100%;border-collapse:collapse}
 .rcpt th{font-size:10px;font-weight:600;color:var(--faint);letter-spacing:.06em;text-transform:uppercase;text-align:right;padding:0 0 6px 14px;white-space:nowrap}
 .rcpt th:first-child{text-align:left;padding-left:0}
 .rcpt td{font-size:14px;color:#6e6e73;padding:11px 0;border-top:1px solid var(--hair);word-break:break-word}
 .rcpt td.v{text-align:right;color:var(--ink);white-space:nowrap;padding-left:14px}
 .rcpt td.u{text-align:right;color:var(--grey);font-size:13px;white-space:nowrap;padding-left:14px}
 .rcpt td .tm{font-size:11px;color:var(--grey-l);padding-top:2px}
 @media(max-width:520px){.rcpt td{font-size:13px}}

 /* controls */
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
 @media(max-width:560px){.grid{grid-template-columns:1fr}}
 label{font-size:12px;font-weight:600;color:var(--grey);display:block;margin-bottom:7px;letter-spacing:0.04em;text-transform:uppercase}
 input[type=text],input[type=date],select{width:100%;padding:12px 14px;border:1px solid var(--hair);border-radius:12px;font-size:14px;background:#fff;color:var(--ink);appearance:none;-webkit-appearance:none;font-family:inherit;min-width:0}
 select{background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6'><path d='M1 1l4 4 4-4' stroke='%2386868b' stroke-width='1.6' fill='none' stroke-linecap='round'/></svg>");background-repeat:no-repeat;background-position:right 14px center;padding-right:34px}
 input:focus,select:focus{outline:none;border-color:var(--ink)}
 .full{grid-column:1/-1}
 .date-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
 .date-row input{flex:1 1 160px}
 .qchip{border:0;background:#fff;color:var(--ink);border-radius:100px;padding:9px 14px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;font-family:inherit;box-shadow:0 1px 2px rgba(0,0,0,.04)}
 .card .qchip{background:var(--bg);box-shadow:none}
 .qchip:hover{background:var(--hair)}
 .qchip.active{background:var(--ink);color:#fff}
 .switch-row{display:flex;align-items:center;gap:12px;margin-top:18px}
 .switch{position:relative;width:46px;height:27px;flex:0 0 auto}
 .switch input{opacity:0;width:0;height:0}
 .slider{position:absolute;inset:0;background:var(--hair);border-radius:100px;cursor:pointer;transition:.2s}
 .slider:before{content:"";position:absolute;height:23px;width:23px;left:2px;top:2px;background:#fff;border-radius:50%;transition:.2s;box-shadow:0 1px 3px rgba(0,0,0,.2)}
 .switch input:checked+.slider{background:var(--green)}
 .switch input:checked+.slider:before{transform:translateX(19px)}
 .switch-lbl{font-size:14px}
 .switch-sub{font-size:12px;color:var(--grey)}
 .btns{display:flex;gap:10px;flex-wrap:wrap;margin-top:24px}
 button.act{border:0;border-radius:100px;padding:12px 22px;font-size:14px;font-weight:600;cursor:pointer;transition:transform .08s,background .15s;font-family:inherit}
 button.act:active{transform:scale(.97)}
 .primary{background:var(--ink);color:#fff}.primary:hover{background:#000}
 .ghost{background:var(--bg);color:var(--ink)}.ghost:hover{background:var(--hair)}
 .hint{font-size:12px;color:var(--grey);margin-top:14px;min-height:16px}

 /* progress */
 .prog{display:none;margin-top:18px}
 .prog.show{display:block}
 .prog-track{height:8px;background:var(--bg);border-radius:100px;overflow:hidden}
 .prog-fill{height:100%;width:0%;background:var(--green-d);border-radius:100px;transition:width .4s ease}
 .prog-lbl{font-size:12px;color:var(--grey);margin-top:8px;display:flex;justify-content:space-between}

 /* output */
 .out-head{display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap}
 .out-title{font-size:15px;font-weight:600;letter-spacing:-0.01em}
 .chip{border-radius:100px;padding:4px 11px;font-size:11px;font-weight:600;display:inline-block;margin-bottom:4px}
 .chip.ok{background:#e7f8ec;color:#248a3d}
 .chip.warn{background:#fff4e5;color:#b25000}
 .chip.err{background:#fff1f0;color:#d70015}
 .chip.info{background:var(--bg);color:#6e6e73}
 .search{margin:4px 0 12px;max-width:280px}
 table.tbl{width:100%;border-collapse:collapse;font-size:13px;min-width:520px}
 .tbl th{font-size:10px;font-weight:600;color:var(--grey-l);letter-spacing:0.06em;text-transform:uppercase;text-align:left;padding:8px 10px 8px 0;border-bottom:1px solid var(--hair);white-space:nowrap}
 .tbl th.num,.tbl td.num{text-align:right;white-space:nowrap}
 .tbl td{padding:10px 10px 10px 0;border-bottom:1px solid #f2f2f4;vertical-align:top}
 .tbl td.dim{color:var(--grey)}
 .dl{font-size:11px;font-weight:600;margin-left:6px;white-space:nowrap}
 .dl.up{color:var(--green-d)} .dl.down{color:var(--red-d)} .dl.flat{color:var(--grey-l)}
 details{margin-top:16px}
 summary{font-size:12px;color:var(--grey);cursor:pointer;user-select:none}
 pre{background:var(--bg);border-radius:12px;padding:14px;font-size:12px;overflow:auto;max-height:320px;white-space:pre-wrap;margin-top:10px}
 .spin{display:inline-block;width:14px;height:14px;border:2px solid var(--hair);border-top-color:var(--ink);border-radius:50%;animation:sp .7s linear infinite;vertical-align:-2px;margin-right:8px}
 @keyframes sp{to{transform:rotate(360deg)}}
 .empty{font-size:13px;color:var(--grey-l);padding:8px 0}

 /* dark card (mirrors the email) */
 .darkcard{background:var(--ink);border-radius:26px;padding:36px 30px 32px;margin-top:20px;color:#fff}
 .darkcard .k{font-size:13px;font-weight:600;color:var(--grey);padding-bottom:14px}
 .darkcard .insight{font-size:21px;line-height:1.4;font-weight:500;letter-spacing:-0.018em;max-width:560px}
 .darkcard a.btn{display:inline-block;margin-top:28px;background:#fff;color:var(--ink);border-radius:100px;padding:12px 26px;font-size:15px;font-weight:600;text-decoration:none}
 @media(max-width:520px){.darkcard{padding:28px 22px;border-radius:20px}.darkcard .insight{font-size:18px}}
 footer{font-size:12px;color:var(--grey-l);text-align:center;padding:40px 0 0;line-height:1.8}
</style></head><body>
<div class="wrap">

<header>
 <div>
  <h1>Daily Clicks Report</h1>
  <div class="sub"><span class="status-dot" id="dot"></span><span id="statustext">Connecting&hellip;</span></div>
 </div>
 <div class="hdr-chips">
  <button class="qchip" id="chipY" onclick="setDate(-1)">Yesterday</button>
  <button class="qchip" id="chipT" onclick="setDate(0)">Today</button>
 </div>
</header>

<!-- ============ OVERVIEW ============ -->
<section class="card">
 <div class="card-title"><span id="ovtitle">Overview</span><a class="act-link" id="ovpreview" href="#" target="_blank">Open preview &rarr;</a></div>
 <div id="ovbody"></div>
</section>

<!-- ============ TEAMS + TOP DOMAINS ============ -->
<div class="two">
 <section class="card">
  <div class="card-title"><span id="teamstitle">Teams</span><span style="font-weight:500;letter-spacing:0;text-transform:none;color:var(--grey-l)">tap a team to open its preview</span></div>
  <div id="teams"></div>
 </section>
 <section class="card">
  <div class="card-title"><span id="topdtitle">Top domains</span></div>
  <div id="topdoms"></div>
 </section>
</div>

<!-- ============ CONTROLS ============ -->
<section class="card" id="controls">
 <div class="card-title">Run &amp; preview</div>
 <div class="grid">
  <div>
   <label for="date">Report date</label>
   <div class="date-row">
    <input type="date" id="date">
    <button class="qchip" onclick="setDate(-1)">Yesterday</button>
    <button class="qchip" onclick="setDate(0)">Today</button>
   </div>
  </div>
  <div>
   <label for="team">Team</label>
   <select id="team"><option value="all">Loading teams&hellip;</option></select>
  </div>
  <div class="full">
   <label for="to">Redirect all emails to <span style="text-transform:none;font-weight:400">(blank = real recipients)</span></label>
   <input type="text" id="to" placeholder="you@example.com, teammate@example.com">
  </div>
 </div>

 <div class="switch-row">
  <label class="switch"><input type="checkbox" id="sheetw" checked><span class="slider"></span></label>
  <div>
   <div class="switch-lbl">Write stats to Google Sheet</div>
   <div class="switch-sub">Master Stats tab + one tab per team</div>
  </div>
 </div>

 <div class="switch-row">
  <label class="switch"><input type="checkbox" id="dry"><span class="slider"></span></label>
  <div>
   <div class="switch-lbl">Dry run</div>
   <div class="switch-sub">Build everything but send no emails, write no stats and skip the sheet</div>
  </div>
 </div>

 <div class="btns">
  <button class="act primary" onclick="runReport(true)">Run report</button>
  <button class="act ghost" onclick="runReport(false)">Stats only (no emails)</button>
  <button class="act ghost" onclick="openPreview()">Preview email</button>
  <button class="act ghost" onclick="diagnose()">Diagnose domains</button>
  <button class="act ghost" onclick="loadJobs()">Recent jobs</button>
 </div>

 <div class="prog" id="prog">
  <div class="prog-track"><div class="prog-fill" id="progfill"></div></div>
  <div class="prog-lbl"><span id="proglbl"></span><span id="progpct"></span></div>
 </div>
 <div class="hint" id="hint"></div>
</section>

<!-- ============ OUTPUT ============ -->
<section class="card">
 <div class="out-head">
  <span class="out-title" id="outtitle">Output</span>
  <span id="outchips"></span>
 </div>
 <div id="outbody"><div class="empty">Run something to see results here.</div></div>
 <details id="rawwrap" style="display:none"><summary>Raw JSON</summary><pre id="raw"></pre></details>
</section>

<!-- ============ GROW ============ -->
<section class="darkcard">
 <div class="k">Grow this report</div>
 <div class="insight">Want more domains in tomorrow's report? Add them to the spreadsheet and they'll show up automatically.</div>
 <a class="btn" href="__SHEET_URL__" target="_blank" rel="noopener">Add domains</a>
</section>

<footer>Daily Click Report &middot; Delivered every morning &middot; intraday runs compare like-for-like at the same clock time</footer>
</div>
<script>
const $=id=>document.getElementById(id);
const KEY=new URLSearchParams(location.search).get('key')||'';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=n=>Number(n||0).toLocaleString();
const fmtSigned=n=>(n>=0?'+':'\u2212')+fmt(Math.abs(n));
const isoDay=off=>new Date(Date.now()+off*864e5).toISOString().slice(0,10);
const TODAY=isoDay(0),YDAY=isoDay(-1);
const niceDate=d=>{try{return new Date(d+'T00:00:00Z').toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',timeZone:'UTC'})}catch(e){return d}};
const dayLetter=d=>{try{return new Date(d+'T00:00:00Z').toLocaleDateString('en-US',{weekday:'short',timeZone:'UTC'})[0]}catch(e){return ''}};
$('date').value=YDAY;

function setDate(offsetDays){$('date').value=isoDay(offsetDays);refreshOverview();}
function paintChips(){const v=$('date').value;$('chipY').classList.toggle('active',v===YDAY);$('chipT').classList.toggle('active',v===TODAY);}
function qp(obj){const p=new URLSearchParams();if(KEY)p.set('key',KEY);Object.entries(obj||{}).forEach(([k,v])=>{if(v!==''&&v!=null)p.set(k,v)});return p.toString();}
async function api(path,params){const r=await fetch(path+'?'+qp(params));return r.json();}
function setOut(title,chips,bodyHtml,raw){
 $('outtitle').textContent=title;$('outchips').innerHTML=chips||'';$('outbody').innerHTML=bodyHtml||'';
 if(raw!==undefined){$('rawwrap').style.display='';$('raw').textContent=JSON.stringify(raw,null,2);}else{$('rawwrap').style.display='none';}
}
function busy(title){setOut(title,'','<div class="empty"><span class="spin"></span>Working&hellip;</div>');}
function chip(cls,txt){return `<span class="chip ${cls}">${esc(txt)}</span> `;}
function ydayHdr(c){return c&&c.as_of?`Yesterday @ ${esc(c.as_of)}`:'Yesterday (full day)';}
function cmpChip(c){return c?chip('info',c.as_of?`vs yesterday as of ${c.as_of}`:'vs full previous day'):'';}
function deltaTag(now,prev){
 if(prev==null||now==null)return '';
 const d=now-prev,cls=d>0?'up':d<0?'down':'flat',pct=prev>0?` · ${Math.abs(d/prev*100).toFixed(1)}%`:'';
 return `<span class="dl ${cls}">${d>0?'↑':d<0?'↓':'•'} ${fmtSigned(d)}${pct}</span>`;
}
function previewUrl(team){return '/preview?'+qp({date:$('date').value,team:team||'all'});}
function empTag(e){return e&&e!=='(all)'?`<div class="tm">${esc(e)}</div>`:'';}

// ================= skeletons =================
const sk=(w,h,extra='')=>`<div class="sk" style="width:${w};height:${h}px;${extra}"></div>`;
function skelOverview(){
 return `<div class="hero">${sk('140px',14,'margin:0 auto')}${sk('230px',80,'margin:18px auto 0;border-radius:18px')}${sk('220px',14,'margin:16px auto 0')}</div>
  <div class="stats">${`<div class="stat">${sk('60px',22,'margin:0 auto')}${sk('90px',12,'margin:8px auto 0')}</div>`.repeat(3)}</div>
  <div class="chart">${sk('100%',150,'border-radius:14px')}</div>${sk('180px',11,'margin-top:12px')}
  <div class="dstrip" style="margin-top:14px">${`<div>${sk('10px',10,'margin:0 auto')}${sk('24px',12,'margin:4px auto 0')}</div>`.repeat(14)}</div>`;
}
function skelTeams(n){
 return `<div class="tgrid">${`<div class="tcard" style="cursor:default"><div class="tname">${sk('90px',14)}${sk('80px',22,'border-radius:100px')}</div>${sk('120px',34,'margin-top:16px')}${sk('150px',12,'margin-top:10px')}${sk('100%',6,'margin-top:14px;border-radius:100px')}</div>`.repeat(n||2)}</div>`;
}
function skelRows(n){
 return `<div>${`<div style="display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-top:1px solid var(--hair)">${sk('45%',14)}${sk('60px',14)}</div>`.repeat(n||5)}</div>`;
}

// ================= teams (sheet) =================
async function loadTeams(){
 try{
  const j=await api('/teams',{});
  if(j.error)throw new Error(j.error);
  const sel=$('team');const prev=sel.value;sel.innerHTML='';
  const total=j.teams.reduce((a,t)=>a+t.domains.length,0);
  const optAll=document.createElement('option');optAll.value='all';optAll.textContent=`All teams (${total} domains)`;sel.appendChild(optAll);
  j.teams.forEach(t=>{const o=document.createElement('option');o.value=t.team;o.textContent=`${t.team} (${t.domains.length} domain${t.domains.length!==1?'s':''})`;sel.appendChild(o);});
  if([...sel.options].some(o=>o.value===prev))sel.value=prev;
  $('dot').classList.add('ok');$('statustext').textContent=`${j.teams.length} teams · ${total} domains from the sheet`;
 }catch(e){
  $('dot').classList.add('err');$('statustext').textContent='Could not load teams: '+(e.message||e);
  $('team').innerHTML='<option value="all">All teams</option>';
 }
}

// ================= chart (same treatment as the email) =================
function smooth(vals,seg){
 seg=seg||18;const n=vals.length;if(n<2)return vals.map((v,i)=>[i,v]);
 const p=[vals[0],...vals,vals[n-1]],out=[];
 for(let i=1;i<n;i++){const p0=p[i-1],p1=p[i],p2=p[i+1],p3=p[i+2];
  for(let s=0;s<seg;s++){const t=s/seg,t2=t*t,t3=t2*t;out.push([(i-1)+t,0.5*((2*p1)+(-p0+p2)*t+(2*p0-5*p1+4*p2-p3)*t2+(-p0+3*p1-3*p2+p3)*t3)]);}}
 out.push([n-1,vals[n-1]]);return out;
}
function chartSVG(daily,up){
 const w=600,h=150,px=6,py=12;
 const clicks=daily.map(d=>d.clicks),ips=daily.map(d=>d.ips);
 const hasIps=ips.some(v=>v>0);const all=hasIps?clicks.concat(ips):clicks;
 let lo=Math.min(...all),hi=Math.max(...all);const pad=Math.max((hi-lo)*.18,1);lo-=pad;hi+=pad;
 const X=i=>px+i*(w-2*px)/(daily.length-1||1),Y=v=>h-py-((v-lo)/(hi-lo||1))*(h-2*py);
 const path=pts=>pts.map(([x,y],i)=>(i?'L':'M')+X(x).toFixed(1)+','+Y(y).toFixed(1)).join(' ');
 const c=up?'#30d158':'#ff453a';const cp=smooth(clicks),ip=smooth(ips);
 const area=path(cp)+` L${X(clicks.length-1).toFixed(1)},${h} L${X(0).toFixed(1)},${h} Z`;const last=clicks.length-1;
 return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-label="14-day clicks">
  <path d="${area}" fill="${c}" opacity="0.14"/>
  ${hasIps?`<path d="${path(ip)}" fill="none" stroke="${c}" stroke-width="2" stroke-dasharray="1 5" stroke-linecap="round" opacity="0.55"/>`:''}
  <path d="${path(cp)}" fill="none" stroke="${c}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
  ${hasIps?`<circle cx="${X(last)}" cy="${Y(ips[last])}" r="4" fill="#fff" stroke="${c}" stroke-width="1.8" opacity="0.9"/>`:''}
  <circle cx="${X(last)}" cy="${Y(clicks[last])}" r="5.5" fill="${c}" stroke="#fff" stroke-width="2.2"/>
 </svg>`;
}
function dayStrip(daily,isToday){
 return `<div class="dstrip">${daily.map((d,i)=>{const last=i===daily.length-1;const lbl=last?(isToday?'Now':dayLetter(d.date)):dayLetter(d.date);
  return `<div class="dcell${last?' last':''}" title="${esc(d.date)}"><div class="d-lbl"${last?` style="color:var(--green-d)"`:''}>${esc(lbl)}</div><div class="d-val">${fmt(d.clicks)}</div><div class="d-ip">${fmt(d.ips)}</div></div>`;}).join('')}</div>`;
}

// ================= overview =================
async function refreshOverview(){
 paintChips();
 const date=$('date').value,team=$('team').value;
 $('ovtitle').textContent='Overview · '+date+(team!=='all'?' · Team '+team:'');
 $('ovpreview').href=previewUrl(team);
 $('teamstitle').textContent='Teams · '+date;
 $('topdtitle').textContent='Top domains · '+(team!=='all'?'Team '+team:'all teams');
 $('ovbody').innerHTML=skelOverview();
 if(!$('teams').querySelector('.tcard:not([style])'))$('teams').innerHTML=skelTeams(2);
 $('topdoms').innerHTML=skelRows(5);
 try{
  const [j,jall]=await Promise.all([api('/summary',{date,team}),team!=='all'?api('/summary',{date,team:'all'}):null]);
  if(j.error){$('ovbody').innerHTML=`<div class="nodata">${esc(j.error)}</div>`;$('teams').innerHTML='';$('topdoms').innerHTML='';return;}
  paintTeams(jall||j);
  if(!j.has_data){
   $('ovbody').innerHTML=`<div class="nodata">No stored stats for <b>${esc(niceDate(j.date))}</b> yet.<br>Stats appear after a real (non-dry) run for this date — use <b>Run report</b> or <b>Stats only</b> below, or pick another date.</div>`;
   $('topdoms').innerHTML='<div class="empty">No data for this date.</div>';return;
  }
  const cmp=j.asof;const d=cmp?cmp.delta:j.prev.delta;const cls=d>0?'up':d<0?'down':'flat';
  const prevVals=j.daily.slice(0,-1).map(x=>x.clicks);const daysWithData=prevVals.filter(v=>v>0).length;
  let head,arrow;
  if(!daysWithData){head="Here's how the day went";arrow='•';}
  else if(j.totals.clicks>Math.max(...prevVals)&&j.totals.clicks>0){head='Best day in two weeks'+(j.is_today?', so far':'');arrow='↑';}
  else if(d>0){head='Trending up'+(j.is_today?' today':'');arrow='↑';}
  else if(d<0){head='A slower day'+(j.is_today?', so far':'');arrow='↓';}
  else{head='Holding steady';arrow='•';}
  const hcls=!daysWithData?'flat':(head.startsWith('Best')||d>0)?'up':d<0?'down':'flat';
  const sub=j.is_today?`clicks so far today${cmp?' · as of '+esc(cmp.cutoff):''} · ${fmt(j.totals.ips)} unique`:`clicks on ${esc(niceDate(j.date))} · ${fmt(j.totals.ips)} unique`;
  const vsLbl=cmp?`vs Yesterday at ${esc(cmp.cutoff)}`:'vs Yesterday';
  const ydayNum=cmp?cmp.yesterday_clicks:j.prev.clicks;const ydayLbl=cmp?`Yesterday as of ${esc(cmp.cutoff)}`:'Yesterday · full day';
  const up=j.daily.length>1?j.totals.clicks>=(cmp?cmp.yesterday_clicks:j.daily[j.daily.length-2].clicks):true;
  $('ovbody').innerHTML=`
   <div class="hero"><div class="headline ${hcls}">${arrow}&#8202; ${esc(head)}</div><div class="hero-num">${fmt(j.totals.clicks)}</div><div class="hero-sub">${sub}</div></div>
   <div class="stats">
    <div class="stat"><div class="stat-num ${cls}">${d===0?'—':fmtSigned(d)}</div><div class="stat-lbl">${vsLbl}</div></div>
    <div class="stat"><div class="stat-num">${fmt(ydayNum)}</div><div class="stat-lbl">${ydayLbl}</div></div>
    <div class="stat"><div class="stat-num">${fmt(j.totals.domains_reporting)}<span class="of"> / ${fmt(j.totals.domains_tracked)}</span></div><div class="stat-lbl">Domains with Clicks</div></div>
   </div>
   <div class="chart">${chartSVG(j.daily,up)}</div>
   <div class="legend"><b style="color:${up?'#248a3d':'#d70015'}">──</b>&nbsp;Total clicks &nbsp;&nbsp;&nbsp;&nbsp; <b style="color:${up?'#248a3d':'#d70015'};letter-spacing:2px">•••</b>&nbsp;Unique clicks</div>
   ${dayStrip(j.daily,j.is_today)}`;
  paintTopDomains(j);
 }catch(e){$('ovbody').innerHTML=`<div class="nodata">${esc(e)}</div>`;}
}
function paintTeams(j){
 const sel=$('team').value;const teams=(j.teams||[]);
 if(!teams.length){$('teams').innerHTML='<div class="empty">No teams.</div>';return;}
 const maxT=Math.max(...teams.map(t=>t.clicks),1);
 $('teams').innerHTML=`<div class="tgrid">${teams.map(t=>`
  <div class="tcard${sel===t.team?' active':''}" onclick="openTeamPreview('${esc(t.team)}')" title="Open preview for Team ${esc(t.team)}">
   <div class="tname"><span>${esc(t.team)}</span><span class="tpill">${t.domains}/${t.domains_tracked??t.domains} domains</span></div>
   <div class="tnum">${fmt(t.clicks)}</div>
   <div class="tcap">clicks · ${fmt(t.ips)} unique${t.yday_asof!=null?` · yday ${fmt(t.yday_asof)}`:''}</div>
   <div class="ttrack"><div class="tfill" style="width:${(t.clicks/maxT*100).toFixed(1)}%"></div></div>
   <div class="tacts"><button class="tbtn" onclick="event.stopPropagation();focusTeam('${esc(t.team)}')">${sel===t.team?'Focused':'Focus'}</button><span class="tlink">Open preview &rarr;</span></div>
  </div>`).join('')}</div>`;
}
function paintTopDomains(j){
 const rows=j.top_domains||[];
 if(!rows.length){$('topdoms').innerHTML='<div class="empty">No domains reported.</div>';return;}
 const hasY=rows.some(r=>r.yday_asof!=null);
 $('topdoms').innerHTML=`<div class="tscroll"><table class="rcpt"><tr><th>Domain</th><th>Clicks</th><th>Unique</th>${hasY?`<th>Yday @ ${esc(j.asof?.cutoff||'')}</th>`:''}</tr>${rows.map(r=>`
  <tr><td>${esc(r.domain)}<div class="tm">${esc(r.team||'')}${r.employer_id?' · '+esc(r.employer_id):''}</div></td><td class="v">${fmt(r.clicks)}</td><td class="u">${fmt(r.ips)}</td>${hasY?`<td class="u">${r.yday_asof!=null?fmt(r.yday_asof):'—'}</td>`:''}</tr>`).join('')}</table></div>`;
}
function focusTeam(t){const sel=$('team');sel.value=(sel.value===t)?'all':t;refreshOverview();}
function openTeamPreview(t){window.open(previewUrl(t),'_blank');$('hint').textContent=`Preview opened for Team ${t} — use Refresh there to re-run it.`;}
$('date').addEventListener('change',refreshOverview);
$('team').addEventListener('change',refreshOverview);

// ================= run + progress =================
function showProg(on){$('prog').classList.toggle('show',!!on);if(!on){$('progfill').style.width='0%';$('proglbl').textContent='';$('progpct').textContent='';}}
function paintProg(p){
 if(!p){return;}
 const phase=p.phase||'running';let pct=0,lbl=phase;
 if(p.total>0){pct=Math.min(100,p.done/p.total*100);}
 if(phase==='queued'||phase==='pending'){pct=3;lbl='Starting run…';}
 else if(phase==='fetching'){lbl=`Running report for domains ${p.done}/${p.total}`;pct=3+pct*0.80;}
 else if(phase==='aggregating'){pct=86;lbl='Aggregating results…';}
 else if(phase==='sending'){lbl=`Sending team emails ${p.done}/${p.total}`;pct=88+pct*0.08;}
 else if(phase==='building'){lbl=`Building team reports ${p.done}/${p.total}`;pct=88+pct*0.08;}
 else if(phase==='rendering'){lbl=`Rendering team emails ${p.done}/${p.total}`;pct=88+pct*0.08;}
 else if(phase==='sheet'){pct=97;lbl='Writing stats to Google Sheet…';}
 else if(phase==='finished'){pct=100;lbl='Finished';}
 $('progfill').style.width=pct.toFixed(0)+'%';$('proglbl').textContent=lbl;$('progpct').textContent=pct.toFixed(0)+'%';
}
async function runReport(sendEmail){
 const title=sendEmail?'Run report':'Stats only';
 busy(title);showProg(true);paintProg({phase:'queued',done:0,total:0});
 const params={date:$('date').value,mode:'async',email:sendEmail?'true':'false',sheet:$('sheetw').checked?'true':'false'};
 const team=$('team').value;
 if(team&&team!=='all')params.teams=team;if($('dry').checked)params.dry_run='true';if($('to').value.trim())params.to=$('to').value.trim();
 try{
  const j=await api('/run',params);
  if(j.error){showProg(false);setOut(title,chip('err','error'),`<div class="empty">${esc(j.error)}</div>`,j);return;}
  $('hint').textContent=`Job ${j.job_id} queued`;poll(j.job_id,title);
 }catch(e){showProg(false);setOut(title,chip('err','error'),`<div class="empty">${esc(e)}</div>`);}
}
async function poll(id,title){
 for(let i=0;i<400;i++){
  await new Promise(r=>setTimeout(r,1500));
  let j;try{j=await api('/jobs/'+id,{});}catch(e){continue;}
  paintProg(j.progress);
  if(j.status==='done'){showProg(false);$('hint').textContent=`Job ${id} done`;renderRun(j.result,j,title);refreshOverview();return;}
  if(j.status==='failed'){showProg(false);$('hint').textContent=`Job ${id} failed`;setOut(title,chip('err','failed'),`<pre>${esc(j.error||'')}</pre>`,j);return;}
 }
 showProg(false);
}
function sheetChip(ss){
 if(!ss)return '';
 if(ss.error)return chip('err','sheet failed');
 return chip('ok',`sheet: ${ss.rows} rows`);
}
function sheetLine(ss){
 if(!ss)return '';
 if(ss.error)return `<div style="margin-top:14px;font-size:12px;color:#d70015">Google Sheet write failed: ${esc(ss.error)}</div>`;
 const parts=[];
 if(ss.master)parts.push(`${esc(ss.master.tab)}: ${ss.master.updated} updated · ${ss.master.added} added`);
 Object.values(ss.teams||{}).forEach(t=>parts.push(`${esc(t.tab)}: ${t.updated} updated · ${t.added} added`));
 return parts.length?`<div style="margin-top:14px;font-size:12px;color:#248a3d">Sheet &middot; ${parts.join(' &middot; ')}</div>`:'';
}
function renderRun(res,raw,title){
 const modeChip=res.dry_run?chip('warn','dry run'):res.emailed?chip('ok','emails sent'):chip('info','stats only');
 const chips=modeChip+chip('info',res.date)+cmpChip(res.compare)+sheetChip(res.sheet_stats)+chip('info',res.duration_sec+'s');
 let h='';
 if(res.teams_sent&&res.teams_sent.length){
  h+=`<div class="tscroll"><table class="tbl"><tr><th>Team</th><th>Recipients</th><th class="num">Lines</th><th class="num">Clicks</th><th class="num">${ydayHdr(res.compare)}</th><th></th></tr>`;
  res.teams_sent.forEach(t=>{
   const rcp=t.emailed?(t.recipients||[]).join(', '):'—';
   h+=`<tr><td>${esc(t.team)}<div style="font-size:11px;color:#aeaeb2;margin-top:3px">${esc(t.subject||'')}</div></td><td class="dim">${esc(rcp)}</td><td class="num">${t.domains}</td><td class="num">${fmt(t.total_clicks)}</td><td class="num">${t.yesterday_clicks!=null?fmt(t.yesterday_clicks)+deltaTag(t.total_clicks,t.yesterday_clicks):'—'}</td><td class="num"><a class="tlink" href="${previewUrl(t.team)}" target="_blank">Preview &rarr;</a></td></tr>`;
  });
  h+='</table></div>';
 }
 if(res.teams_skipped&&res.teams_skipped.length){h+=`<div style="margin-top:14px;font-size:12px;color:#b25000">Skipped: ${res.teams_skipped.map(s=>esc(s.team+' — '+s.reason)).join(' · ')}</div>`;}
 const fails=Object.entries(res.domain_failures||{});
 if(fails.length){h+=`<div style="margin-top:14px;font-size:12px;color:#d70015">Failed lines: ${fails.map(([d])=>esc(d)).join(', ')} (details in raw JSON)</div>`;}
 h+=sheetLine(res.sheet_stats);
 if(res.warnings&&res.warnings.length){h+=`<div style="margin-top:10px;font-size:12px;color:#86868b">${res.warnings.length} warning(s) — see raw JSON</div>`;}
 setOut(title||'Run report',chips,h||'<div class="empty">Nothing processed.</div>',raw);
}

// ================= preview =================
function openPreview(){
 const team=$('team').value||'all';window.open(previewUrl(team),'_blank');
 $('hint').textContent=team==='all'?'Preview opened — team tabs, Today / Yesterday chips and Refresh are in its header.':`Preview opened for Team ${team}.`;
}

// ================= diagnose =================
let _diagRows=[],_diagCmp=null;
async function diagnose(){
 busy('Diagnose domains');
 const params={date:$('date').value};const team=$('team').value;if(team&&team!=='all')params.teams=team;
 try{
  const j=await api('/diagnose',params);
  if(j.error){setOut('Diagnose domains',chip('err','error'),`<pre>${esc(j.error)}</pre>`,j);return;}
  _diagRows=j.domains||[];_diagCmp=j.compare||null;
  const bad=_diagRows.filter(d=>d.status!=='ok').length;
  const chips=chip(bad?'warn':'ok',`${_diagRows.length-bad}/${_diagRows.length} healthy`)+chip('info',j.date)+cmpChip(_diagCmp);
  setOut('Diagnose domains',chips,`<input class="search" type="text" id="diagq" placeholder="Filter domains…" oninput="paintDiag()"><div id="diagtbl"></div>`,j);
  paintDiag();
 }catch(e){setOut('Diagnose domains',chip('err','error'),`<div class="empty">${esc(e)}</div>`);}
}
function paintDiag(){
 const q=($('diagq')?.value||'').toLowerCase();
 const rows=_diagRows.filter(d=>!q||d.domain.toLowerCase().includes(q)||d.team.toLowerCase().includes(q)||(d.employer_id||'').toLowerCase().includes(q)||(d.status||'').toLowerCase().includes(q));
 let h=`<div class="tscroll"><table class="tbl"><tr><th>Domain</th><th>Team</th><th>Status</th><th class="num">Clicks</th><th class="num">Unique</th><th class="num">${ydayHdr(_diagCmp)}</th></tr>`;
 rows.forEach(d=>{
  const st=d.status==='ok'?chip('ok','ok'):chip('err','failed');
  const yd=d.yesterday_clicks!=null?fmt(d.yesterday_clicks)+deltaTag(d.clicks_on_date,d.yesterday_clicks):'—';
  h+=`<tr><td>${esc(d.domain)}${empTag(d.employer_id)}</td><td class="dim">${esc(d.team)}</td><td>${st}${d.reason?`<div style="font-size:11px;color:#86868b;margin-top:3px">${esc(d.reason)}</div>`:''}</td><td class="num">${d.clicks_on_date!=null?fmt(d.clicks_on_date):'—'}</td><td class="num">${d.unique_ips!=null?fmt(d.unique_ips):'—'}</td><td class="num">${yd}</td></tr>`;
 });
 h+='</table></div>';
 if(!rows.length)h='<div class="empty">No domains match.</div>';
 $('diagtbl').innerHTML=h;
}

// ================= jobs =================
async function loadJobs(){
 busy('Recent jobs');
 try{
  const j=await api('/jobs',{});const items=j.jobs||[];
  if(!items.length){setOut('Recent jobs','','<div class="empty">No jobs yet this session.</div>',j);return;}
  let h='<div class="tscroll"><table class="tbl"><tr><th>Job</th><th>Kind</th><th>Status</th><th>Date</th><th>Teams</th><th>Mode</th><th>Progress</th><th>Started</th></tr>';
  items.forEach(x=>{
   const st=x.status==='done'?chip('ok','done'):x.status==='failed'?chip('err','failed'):chip('info',x.status);
   const teams=Array.isArray(x.params?.teams)?x.params.teams.join(', '):(x.params?.teams||'all');
   const mode=x.kind==='preview'?'preview':x.params?.dry_run?'dry run':x.params?.email===false?'stats only':'emails';
   const pr=x.progress?`${esc(x.progress.phase)} ${x.progress.total?x.progress.done+'/'+x.progress.total:''}`:'—';
   h+=`<tr><td class="dim">${esc(x.job_id)}</td><td>${esc(x.kind||'report')}</td><td>${st}</td><td>${esc(x.params?.date||'')}</td><td class="dim">${esc(teams)}</td><td class="dim">${esc(mode)}</td><td class="dim">${pr}</td><td class="dim">${esc((x.started_at||'').slice(11,19))}</td></tr>`;
  });
  h+='</table></div>';
  setOut('Recent jobs','',h,j);
 }catch(e){setOut('Recent jobs',chip('err','error'),`<div class="empty">${esc(e)}</div>`);}
}

// boot
$('ovbody').innerHTML=skelOverview();$('teams').innerHTML=skelTeams(2);$('topdoms').innerHTML=skelRows(5);
loadTeams().then(refreshOverview);
</script>
</body></html>"""

@app.route("/")
def dashboard():
    return Response(DASHBOARD_HTML.replace("__SHEET_URL__", SHEET_URL),
                    mimetype="text/html")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    print(f"Daily Clicks Report API on 0.0.0.0:{HTTP_PORT}"
          f" (auth {'ON' if API_KEY else 'OFF'}, workers={MAX_WORKERS}, "
          f"sheet cache {SHEET_CACHE_TTL}s, report tz {REPORT_TZ}, "
          f"sheet stats {'ON' if SHEET_STATS_ENABLED else 'OFF'})")
    app.run(host="0.0.0.0", port=HTTP_PORT, threaded=True)