import os
import pymongo
from dotenv import load_dotenv

load_dotenv()

# CONNECT TO LOCAL MONGO
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = pymongo.MongoClient(MONGO_URI)
db = client["domain_expires"]
col = db["config_domains"]

# DROP OLD CONFIG TO ENSURE FRESH START
col.drop()

# CONFIGURATION DATA
domains_data = [
    # ---------------- PROGRAMMATIC DOMAINS ----------------
    {
        "domain": "v2valuejobs_prod",
        "type": "programmatic",
        "cron_time": "10:30",  # IST TIME
        "quota": 30000,
        "semrush_domain": "v2valuejobs.com",
        "active": True
    },
    {
        "domain": "justopinionjobs_prod",
        "type": "programmatic",
        "cron_time": "11:00",
        "quota": 30000,
        "semrush_domain": "justopinionjobs.com",
        "active": True
    },
    {
        "domain": "earnwithwfh_prod",
        "type": "programmatic",
        "cron_time": "11:30",
        "quota": 30000,
        "semrush_domain": "earnwithwfh.com",
        "active": True
    },

    # ---------------- DIRECT CLIENT (DC) DOMAINS ----------------
    # Note: For DC, we map Employer IDs. If 'employer_ids' is empty, it checks ALL.
    {
        "domain": "directclients_prod", 
        "type": "direct_apply",
        "cron_time": "09:00",
        "quota": 50000, 
        "semrush_domain": None, # DC doesn't use Semrush
        "employer_ids": ["emp-jbk-apexfocusgroup", "emp-jbk-levelsurveys"],
        "active": True
    }
]

col.insert_many(domains_data)
print("✅ Database configured with 4 domains.")