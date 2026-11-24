import os
import re
import datetime
import logging
import pandas as pd
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# FastAPI Imports (Cleaned and combined)
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status, Header, Depends, BackgroundTasks
from fastapi.responses import JSONResponse

# Custom modules
from google_auth import get_gspread_client
from pipeline import run_pipeline, import_uploaded_report
from semrush_import import router as semrush_router

load_dotenv()

# =====================================================
# 1. CONFIGURATION & LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("JobGetAPI")

LOCAL_MONGO_URI = os.getenv("MONGO_URI")
PROD_MONGO_URI = os.getenv(
    "MONGO_URI",
    "xxxxxxxxxxxx/"
)

# =====================================================
# 2. FASTAPI APP SETUP
# =====================================================
app = FastAPI(
    title="JobGet API",
    version="1.1.0",
    description="API for JobGet Pipeline and Mongo Credential Management"
)


# =====================================================
# 3. HELPER FUNCTIONS
# =====================================================
def validate_date(date: str):
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise HTTPException(
            status_code=400,
            detail="Invalid date. Use YYYY-MM-DD format."
        )


def get_local_db():
    try:
        client = MongoClient(LOCAL_MONGO_URI, serverSelectionTimeoutMS=2000)
        return client["mongo_creds"]
    except errors.ServerSelectionTimeoutError:
        logger.error("Could not connect to Local MongoDB.")
        raise HTTPException(status_code=503, detail="Local Database Unavailable")


# =====================================================
# 4. ENDPOINTS
# =====================================================

# Dependency to check password
async def verify_password(x_run_password: str = Header(...)):
    if x_run_password != "8000":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid execution password."
        )
    return True

@app.post("/jobget/run-clicks", tags=["JobGet Pipeline"])
async def run_clicks_report(
    date: str = Form(...), 
    background_tasks: BackgroundTasks = None, # FastAPI injects this automatically
    authenticated: bool = Depends(verify_password)
):
    """
    Triggers the Daily Domain Click Report pipeline.
    Requires header: 'x-run-password: 8000'
    """
    validate_date(date)
    
    # Import the service function
    from clicks_service import run_clicks_pipeline 
    
    # Add to background tasks
    background_tasks.add_task(run_clicks_pipeline, date)
    
    return {
        "status": "success", 
        "message": f"Clicks pipeline triggered for {date}. Process running in background."
    }

    
@app.post("/add-mongo-uri", tags=["Config"])
def add_mongo_uri(uri: str = Form(...)):
    """
    Add a new MongoDB Connection URI.
    Paste the full URI string in the box.
    """
    if not uri.strip().startswith("mongodb"):
        raise HTTPException(400, "Invalid URI. Must start with 'mongodb://' or 'mongodb+srv://'")

    try:
        db = get_local_db()
        col = db["uri"]

        if col.find_one({"mongo_uri": uri}):
            return {"status": "skipped", "message": "This URI is already saved."}

        col.insert_one({
            "mongo_uri": uri,
            "added_at": datetime.datetime.now(),
            "connected": False,
            "last_checked": None,
            "source": "api"
        })

        logger.info(f"Added new URI: {uri[:20]}...")
        return {"status": "success", "message": "URI saved successfully."}

    except Exception as e:
        logger.error(f"Error saving URI: {str(e)}")
        return {"status": "error", "message": str(e)}


@app.post(
    "/jobget/run",
    tags=["JobGet Pipeline"],
    summary="Run Pipeline",
    response_class=JSONResponse,
)
async def run_jobget(
        date: str = Form(...),
        cpa_file: UploadFile = File(...),
        cpc_file: UploadFile = File(...),
):
    logger.info(f"Starting pipeline for {date}")

    try:
        validate_date(date)
        gc = get_gspread_client(PROD_MONGO_URI)

        cpa_df = import_uploaded_report(cpa_file, "CPA", date)
        cpc_df = import_uploaded_report(cpc_file, "CPC", date)

        if cpa_df.empty and cpc_df.empty:
            raise HTTPException(400, "Both uploaded files are empty.")

        report_df = pd.concat([cpa_df, cpc_df], ignore_index=True)

        result = run_pipeline(
            report_df=report_df,
            date=date,
            gspread_client=gc,
            mongo_uri=PROD_MONGO_URI,
        )

        logger.info("Pipeline finished successfully.")
        return {
            "status": "success",
            "date": date,
            "domains_processed": result.get("domains", 0),
            "views_processed": result.get("views", 0)
        }

    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Pipeline Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# 5. SEMRUSH ROUTER
# =====================================================
app.include_router(semrush_router)