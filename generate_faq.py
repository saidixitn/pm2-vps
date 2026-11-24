import os
import time
import json
import random
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from dotenv import load_dotenv  

# Load environment variables
load_dotenv()

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = "openrouter/free" 
WORKER_COUNT = 3 # Number of concurrent threads

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Thread-%(threadName)s] - %(message)s'
)

def call_openrouter(prompt, retries=5, backoff_factor=2):
    """
    Calls OpenRouter API with the given prompt. Includes exponential backoff.
    """
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://yourdomain.com", 
        "X-Title": "FAQ Generator Script"         
    }
    
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are an expert copywriter. You must format your entire response using ONLY valid HTML tags: <p>, <ul>, <li>, and <strong>. Do not use Markdown formatting (like ** or ##). Do not wrap the output in markdown code blocks."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            
            # Handle Rate Limits (429) or Server Errors
            if response.status_code in [429, 500, 502, 503, 504]:
                wait_time = (backoff_factor ** attempt) + random.uniform(0, 1) # Added jitter
                logging.warning(f"API returned {response.status_code}. Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status() 
            
            data = response.json()
            if "choices" in data and len(data["choices"]) > 0:
                return data["choices"][0]["message"]["content"]
            else:
                logging.error(f"Unexpected API response structure: {data}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt == retries - 1:
                return None
            time.sleep((backoff_factor ** attempt) + random.uniform(0, 1))

    return None

def process_single_faq(doc, collection):
    """
    Worker function: Processes a single MongoDB document.
    """
    doc_id = doc.get("_id")
    prompt = doc.get("AI")
    faq_title = doc.get("faq", "Unknown FAQ")
    
    if not prompt:
        logging.warning(f"Skipping document {doc_id}: Missing 'ai' prompt field.")
        return False

    logging.info(f"Processing: {faq_title}")
    
    generated_content = call_openrouter(prompt)
    
    if generated_content:
        try:
            collection.update_one(
                {"_id": doc_id},
                {"$set": {
                    "ai_response": generated_content,
                    "status": "completed",
                    "updated_at": time.time()
                }}
            )
            logging.info(f"Successfully saved response for: {faq_title}")
            
            # Base delay to prevent instantly hammering the API again
            time.sleep(2) 
            return True
            
        except PyMongoError as e:
            logging.error(f"Failed to update document {doc_id} in MongoDB: {e}")
            return False
    else:
        logging.error(f"Failed to generate content for document {doc_id}. Marking as failed.")
        collection.update_one(
            {"_id": doc_id},
            {"$set": {"status": "failed", "updated_at": time.time()}}
        )
        return False

def process_faq_prompts():
    """
    Connects to MongoDB, fetches pending records, and dispatches them to workers.
    """
    if not OPENROUTER_API_KEY:
        logging.error("OPENROUTER_API_KEY is missing. Please set it in your .env file.")
        return

    try:
        # MongoClient is thread-safe, so passing the collection to workers is perfectly safe
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client.get_database("GenAI")
        collection = db.get_collection("faq")
        logging.info("Successfully connected to MongoDB.")
    except ConnectionFailure:
        logging.error(f"Failed to connect to MongoDB at {MONGO_URI}.")
        return

    query = {"ai_response": {"$exists": False}, "status": {"$ne": "failed"}}
    
    try:
        # Load documents into memory (safe for ~137k docs, prevents Mongo cursor timeout issues on long runs)
        documents = list(collection.find(query))
        total_docs = len(documents)
        logging.info(f"Found {total_docs} documents pending processing.")
    except PyMongoError as e:
        logging.error(f"Database query failed: {e}")
        return

    if total_docs == 0:
        logging.info("No documents to process. Exiting.")
        return

    # Dispatch tasks to the ThreadPoolExecutor
    logging.info(f"Starting {WORKER_COUNT} concurrent workers...")
    
    success_count = 0
    with ThreadPoolExecutor(max_workers=WORKER_COUNT, thread_name_prefix="Worker") as executor:
        # Submit all tasks to the executor
        futures = {executor.submit(process_single_faq, doc, collection): doc for doc in documents}
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    success_count += 1
            except Exception as e:
                logging.error(f"An unhandled exception occurred in a worker thread: {e}")

    logging.info(f"Batch complete. Successfully processed {success_count}/{total_docs} documents.")

if __name__ == "__main__":
    process_faq_prompts()