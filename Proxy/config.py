import os
from dotenv import load_dotenv

load_dotenv()

# ================================================
# MONGO & GOOGLE CONFIGURATION
# ================================================
REMOTE_MONGO_URI = os.getenv(
    "LOCAL_MONGO_URI",
    "mongodb://localhost:27017/admin"
)

GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "EarnHaus")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1cNFAvS1ANrPAx6RErmFwqkjN6gx7fARZqSAgR5nJ1xo")

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ================================================
# DATABASES & COLLECTIONS
# ================================================
CREDS_DB = "mongo_creds"
PROXY_DB = "proxy_stats"
LOG_COLLECTION = "run_logs"
CONFIG_COLLECTION = "config"

JOB_DOMAIN_KEY = "prod_jobiak_ai"
JOB_DB = "prod_jobiak_ai"
JOB_COLLECTION = "job"
USER_ANALYTICS_COLLECTION = "userAnalytics"

POSTINGS_DB = "data_cleansing"
POSTINGS_COLLECTION = "ETL_AUD_Data"
POSTINGS_TYPE = 1

ETL_SCRAPING_DB = "ETL_Scrapping_Data"
ETL_SCRAPING_COLLECTION = "ETL_Scraping_Data"

EXPIRES_DB = "domain_expires"
SEMRUSH_MONGO_URI = os.getenv("SEMRUSH_MONGO_URI")
SEMRUSH_DB_NAME = os.getenv("SEMRUSH_DB_NAME", "domain_expires")
SEMRUSH_COLLECTION = os.getenv("SEMRUSH_COLLECTION", "semrush_url's")

# Default excluded survey titles
EXCLUDED_TITLES = [
    "Online Survey Taker",
    "Online Survey Taker - Earn up to $1K every month",
    "Survey Taker"
]

# ================================================
# TARGET P4 / DESCRIPTION AUDIT PIPELINE
# ================================================
# Spreadsheet holding the 'Domains' tab (defaults to the sheet named "Postings").
# Set TARGETP4_SPREADSHEET_ID to open by key instead of by name.
TARGETP4_SPREADSHEET_ID = os.getenv("TARGETP4_SPREADSHEET_ID")
TARGETP4_SPREADSHEET_NAME = os.getenv("TARGETP4_SPREADSHEET_NAME", "Postings")

TARGETP4_DOMAINS_TAB = os.getenv("TARGETP4_DOMAINS_TAB", "Domains")
TARGETP4_TARGET_TAB = os.getenv("TARGETP4_TARGET_TAB", "targetp4")
TARGETP4_DESC_TAB = os.getenv("TARGETP4_DESC_TAB", "Desc Not Found")
TARGETP4_STATS_TAB = os.getenv("TARGETP4_STATS_TAB", "Stats")

TARGETP4_COLLECTION = os.getenv("TARGETP4_COLLECTION", "Target_P4_Opt")
DESC_CHANGE_COLLECTION = os.getenv("DESC_CHANGE_COLLECTION", "alternative_desc_change")

IS_REMOTE_API_URL = os.getenv("IS_REMOTE_API_URL", "http://104.251.219.45:5111/update_is_remote")
IS_REMOTE_API_TIMEOUT = int(os.getenv("IS_REMOTE_API_TIMEOUT", "100"))

MAX_DOMAIN_WORKERS = int(os.getenv("MAX_DOMAIN_WORKERS", "10"))
DOMAIN_RETRIES = int(os.getenv("DOMAIN_RETRIES", "3"))
# JSON string, e.g. {"earnhaus_prod": "earnhaus.com"}
DOMAIN_API_MAP = os.getenv("DOMAIN_API_MAP", "{}")

POSTINGS_TITLES_DEFAULT = [
    # "Feedback Associate (Up to $25/Task)",
    # "Digital Tester (Earn $25 Per Review)",
    # "Insight Provider (Immediate Start)",
    # "Remote Front Desk Clerk",
    # "Grocery Shopping Survey",
    # "Market Researcher (100% Remote)",
    # "Product Support",
    # "Automotive Survey",
    "Part Time - Email Processor - No Experience",
"Admin Assistant",
"Market Researcher (100% Remote)",
"Automotive Survey",
"Call Center Agent",
"Insight Provider (Immediate Start)",
"Customer Service Representative (CSR)",
"Proofreader - Online - Weekly Pay",
"Digital Tester (Earn $25 Per Review)",
"Part-Time Team Member",
"Product Support",
"User Experience Evaluator",
"Data Entry - Typist Part-Time - Work Online",
"Product Validation Assistant",
"Survey Data Gig",
"Research Operations Assistant",
"Remote Front Desk Clerk",
"Online Feedback Specialist",
"Work From Home Survey Respondent",
"Research Data Assistant",
"Online Opinion Contributor",
"Digital Opinion Analyst",
"Administrative Data Clerk",
"Remote Feedback Provider",
"Part Time Data Entry Clerk",
"Digital Survey Associate",
"Product Reviewer",
"Live Chat Agent - Entry Level - No Degree Needed",
"Flexible Schedule Research Participant",
"Entry Level Product Tester",
"Consumer & Market Insights Gig",
"Part-Time Survey Panelist",
"Usability Tester - No Experience",
"Remote User Tester",
"Consumer Feedback Associate",
"Data Collection Associate",
"Feedback Associate (Up to $25/Task)",
"Grocery Shopping Survey",
"Market Research Associate",
"Digital Product Tester (Entry Level)",
"Online Research Panel Member",
"Online Research Support Assistant",
"Market Feedback Participant"
    
]
