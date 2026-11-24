import os
import time
import random
import logging
import threading
import requests

from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, PyMongoError
from dotenv import load_dotenv

# =========================================================
# LOAD ENV
# =========================================================

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

# =========================================================
# CONFIG
# =========================================================

OPENROUTER_MODEL = "openrouter/free"

WORKER_COUNT = 15

REQUEST_TIMEOUT = 15

RETRY_COUNT = 3

PROXY_VALIDATION_TIMEOUT = 5

PROXY_COOLDOWN_SECONDS = 300

MAX_PROXY_FAILURES = 10

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================================================
# GLOBALS
# =========================================================

PROXIES = []

COOLDOWN_PROXIES = {}

PROXY_FAILURES = {}

API_KEYS = []

proxy_lock = threading.Lock()

api_lock = threading.Lock()

thread_local = threading.local()

# =========================================================
# SESSION REUSE
# =========================================================

def get_session():

    if not hasattr(thread_local, "session"):

        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        thread_local.session = session

    return thread_local.session

# =========================================================
# CREATE INDEXES
# =========================================================

def create_indexes(db):

    logging.info("CREATING INDEXES")

    faq_collection = db["faq"]

    proxy_collection = db["proxy"]

    api_collection = db["api"]

    # FAQ INDEXES

    faq_collection.create_index(
        [("status", 1)]
    )

    faq_collection.create_index(
        [("ai_response", 1)]
    )

    faq_collection.create_index(
        [("status", 1), ("ai_response", 1)]
    )

    # PROXY INDEXES

    proxy_collection.create_index(
        [("tablescraper-selected-row", 1)]
    )

    proxy_collection.create_index(
        [("tablescraper-selected-row 2", 1)]
    )

    # API INDEXES

    api_collection.create_index(
        [("api_key", 1)]
    )

    logging.info("INDEXES READY")

# =========================================================
# LOAD API KEYS
# =========================================================

def load_api_keys(db):

    global API_KEYS

    logging.info("LOADING API KEYS")

    api_collection = db["api"]

    docs = api_collection.find(
        {},
        {"api_key": 1}
    )

    for doc in docs:

        api_key = doc.get("api_key")

        if api_key:

            API_KEYS.append(api_key)

    logging.info(
        f"LOADED {len(API_KEYS)} API KEYS"
    )

# =========================================================
# GET RANDOM API KEY
# =========================================================

def get_api_key():

    with api_lock:

        if not API_KEYS:
            return None

        return random.choice(API_KEYS)

# =========================================================
# LOAD PROXIES
# =========================================================

def load_proxies(db):

    global PROXIES

    logging.info("LOADING PROXIES")

    proxy_collection = db["proxy"]

    docs = proxy_collection.find(
        {},
        {
            "tablescraper-selected-row": 1,
            "tablescraper-selected-row 2": 1
        }
    )

    for doc in docs:

        ip = doc.get(
            "tablescraper-selected-row"
        )

        port = doc.get(
            "tablescraper-selected-row 2"
        )

        if not ip or not port:
            continue

        proxy = f"http://{ip}:{port}"

        PROXIES.append(proxy)

    logging.info(
        f"LOADED {len(PROXIES)} PROXIES"
    )

# =========================================================
# VALIDATE PROXY
# =========================================================

def validate_proxy(proxy):

    try:

        proxies = {
            "http": proxy,
            "https": proxy
        }

        session = get_session()

        response = session.get(
            "https://openrouter.ai/api/v1/models",
            proxies=proxies,
            timeout=PROXY_VALIDATION_TIMEOUT
        )

        return response.status_code == 200

    except Exception:

        return False

# =========================================================
# VALIDATE ALL PROXIES
# =========================================================

def validate_all_proxies():

    global PROXIES

    logging.info("VALIDATING PROXIES")

    working = []

    with ThreadPoolExecutor(
        max_workers=50
    ) as executor:

        results = executor.map(
            validate_proxy,
            PROXIES
        )

        for proxy, valid in zip(PROXIES, results):

            if valid:

                working.append(proxy)

    PROXIES = working

    logging.info(
        f"WORKING PROXIES: {len(PROXIES)}"
    )

# =========================================================
# GET RANDOM PROXY
# =========================================================

def get_proxy():

    with proxy_lock:

        if not PROXIES:
            return None

        return random.choice(PROXIES)

# =========================================================
# COOLDOWN PROXY
# =========================================================

def cooldown_proxy(proxy):

    global PROXIES

    with proxy_lock:

        PROXY_FAILURES[proxy] = (
            PROXY_FAILURES.get(proxy, 0) + 1
        )

        failures = PROXY_FAILURES[proxy]

        # BLACKLIST

        if failures >= MAX_PROXY_FAILURES:

            if proxy in PROXIES:

                PROXIES.remove(proxy)

            logging.warning(
                f"BLACKLISTED: {proxy}"
            )

            return

        # TEMP COOLDOWN

        if proxy in PROXIES:

            PROXIES.remove(proxy)

            COOLDOWN_PROXIES[proxy] = (
                time.time() +
                PROXY_COOLDOWN_SECONDS
            )

# =========================================================
# RESTORE PROXIES
# =========================================================

def restore_proxies():

    while True:

        now = time.time()

        with proxy_lock:

            restore_list = []

            for proxy, restore_time in (
                COOLDOWN_PROXIES.items()
            ):

                if now >= restore_time:

                    restore_list.append(proxy)

            for proxy in restore_list:

                PROXIES.append(proxy)

                del COOLDOWN_PROXIES[proxy]

                logging.info(
                    f"RESTORED: {proxy}"
                )

        time.sleep(30)

# =========================================================
# OPENROUTER REQUEST
# =========================================================

def call_openrouter(prompt, worker_name):

    url = (
        "https://openrouter.ai/"
        "api/v1/chat/completions"
    )

    session = get_session()

    for attempt in range(RETRY_COUNT):

        proxy = get_proxy()

        api_key = get_api_key()

        if not proxy:

            logging.warning(
                "NO ACTIVE PROXIES"
            )

            time.sleep(10)

            continue

        proxies = {
            "http": proxy,
            "https": proxy
        }

        headers = {
            "Authorization":
                f"Bearer {api_key}",
            "Content-Type":
                "application/json"
        }

        payload = {
            "model": OPENROUTER_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }

        try:

            response = session.post(
                url,
                headers=headers,
                json=payload,
                proxies=proxies,
                timeout=REQUEST_TIMEOUT
            )

            # SUCCESS

            if response.status_code == 200:

                data = response.json()

                if "choices" in data:

                    print(
                        f"[SUCCESS] "
                        f"{worker_name} | "
                        f"{proxy}"
                    )

                    return (
                        data["choices"][0]
                        ["message"]["content"]
                    )

            # RETRYABLE

            if response.status_code in [
                429,
                500,
                502,
                503,
                504
            ]:

                wait_time = (
                    (2 ** attempt) +
                    random.uniform(1, 3)
                )

                time.sleep(wait_time)

                continue

        except requests.exceptions.RequestException:

            cooldown_proxy(proxy)

    return None

# =========================================================
# GET NEXT FAQ
# =========================================================

def get_next_faq(collection):

    return collection.find_one_and_update(

        {
            "ai_response": {
                "$exists": False
            },

            "status": {
                "$ne": "processing"
            }
        },

        {
            "$set": {
                "status": "processing",
                "started_at": time.time()
            }
        },

        return_document=ReturnDocument.AFTER
    )

# =========================================================
# PROCESS FAQ
# =========================================================

def process_single_faq(doc, collection):

    worker_name = (
        threading.current_thread().name
    )

    doc_id = doc["_id"]

    prompt = doc.get("AI")

    if not prompt:

        collection.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "status": "failed"
                }
            }
        )

        return

    generated_content = call_openrouter(
        prompt,
        worker_name
    )

    if generated_content:

        collection.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "ai_response":
                        generated_content,

                    "status":
                        "completed",

                    "updated_at":
                        time.time()
                }
            }
        )

    else:

        collection.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "status": "failed"
                }
            }
        )

# =========================================================
# WORKER LOOP
# =========================================================

def worker_loop(collection):

    while True:

        doc = get_next_faq(collection)

        if not doc:

            logging.info(
                "NO MORE FAQS"
            )

            return

        try:

            process_single_faq(
                doc,
                collection
            )

        except Exception as e:

            logging.error(e)

# =========================================================
# MAIN
# =========================================================

def process_faq_prompts():

    try:

        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000
        )

        client.admin.command("ping")

        db = client["GenAI"]

        faq_collection = db["faq"]

        logging.info(
            "CONNECTED TO MONGODB"
        )

    except ConnectionFailure:

        logging.error(
            "FAILED TO CONNECT MONGODB"
        )

        return

    # CREATE INDEXES

    create_indexes(db)

    # LOAD API KEYS

    load_api_keys(db)

    if not API_KEYS:

        logging.error(
            "NO API KEYS FOUND"
        )

        return

    # LOAD PROXIES

    load_proxies(db)

    validate_all_proxies()

    if not PROXIES:

        logging.error(
            "NO VALID PROXIES"
        )

        return

    # START RESTORE THREAD

    threading.Thread(
        target=restore_proxies,
        daemon=True
    ).start()

    # START WORKERS

    logging.info(
        f"STARTING "
        f"{WORKER_COUNT} WORKERS"
    )

    with ThreadPoolExecutor(
        max_workers=WORKER_COUNT,
        thread_name_prefix="Worker"
    ) as executor:

        futures = [

            executor.submit(
                worker_loop,
                faq_collection
            )

            for _ in range(WORKER_COUNT)
        ]

        for future in futures:

            future.result()

# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    process_faq_prompts()