import sys
import io
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Safe stdout handling for background web servers
class SafeStdout:
    def write(self, data):
        try:
            sys.__stdout__.write(data)
        except Exception:
            pass
    def flush(self):
        try:
            sys.__stdout__.flush()
        except Exception:
            pass

sys.stdout = SafeStdout()

import config
from sheets_handler import SheetsHandler
from mongo_handler import MongoHandler
from main import run_pipeline_for_domain
from targetp4_handler import TargetP4Handler

app = FastAPI(
    title="EarnHaus Google for Jobs Export API",
    version="1.0.0",
    description="API for running daily job exports and updating Google Sheets",
)

_RUN_LOCK = threading.Lock()
_TARGETP4_LOCK = threading.Lock()


class ExportRequest(BaseModel):
    employerId: str = Field(default="68b8133b060e8f5df1a6c3a7")
    company: str = Field(default="Earn Haus")
    domain: str = Field(default="earnhaus")

    postings: Optional[int] = Field(default=None, ge=0)
    updates: Optional[int] = Field(default=None, ge=0)
    expires: Optional[int] = Field(default=None, ge=0)
    datePosted: Optional[int] = Field(default=None, ge=0)

    rebuildExpires: bool = Field(default=True, description="Always rebuild the earnhaus_expires collection before fetching expires")
    dryRun: bool = Field(default=False, description="Run query verification without writing to Google Sheets")


class TargetP4Request(BaseModel):
    domains: Optional[List[str]] = Field(
        default=None,
        description="Subset of domains from the 'Domains' tab. Omit to process every row.",
    )
    updateIsRemote: bool = Field(default=True, description="Call the isRemote API for rows where isRemote=TRUE")
    maxWorkers: Optional[int] = Field(default=None, ge=1, le=32, description="Parallel domain workers")
    retries: Optional[int] = Field(default=None, ge=1, le=10, description="Retries per domain on failure")
    dryRun: bool = Field(default=False, description="Run queries without writing to Google Sheets or calling the API")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "EarnHaus Google for Jobs Export API",
        "spreadsheet_id": config.SPREADSHEET_ID,
        "time_utc": datetime.now(timezone.utc),
    }


@app.post("/export")
def export_jobs(req: ExportRequest):
    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="An export pipeline is already running")

    try:
        sheets = SheetsHandler()
        mongo = MongoHandler()

        # Load counts from request body if provided, else from Google Sheets domain details tab
        counts = {}
        for key in ["postings", "updates", "expires", "datePosted"]:
            val = getattr(req, key)
            if val is not None:
                counts[key] = val

        if not counts:
            sheet_config = sheets.read_config_sheet(sheet_name="domain details")
            counts = sheet_config.get("counts", {"postings": 1000, "updates": 1000, "expires": 500, "datePosted": 5000})

        run_pipeline_for_domain(
            sheets=sheets,
            mongo=mongo,
            employer_id=req.employerId,
            company=req.company,
            domain=req.domain,
            counts=counts,
            rebuild_expires=req.rebuildExpires,
            dry_run=req.dryRun,
        )

        return {
            "status": "success",
            "message": "Export completed successfully and Google Sheets updated",
            "employerId": req.employerId,
            "company": req.company,
            "counts": counts,
            "rebuildExpires": req.rebuildExpires,
            "dryRun": req.dryRun,
            "spreadsheetUrl": f"https://docs.google.com/spreadsheets/d/{config.SPREADSHEET_ID}/edit",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        _RUN_LOCK.release()


@app.post("/targetp4")
def targetp4_export(req: TargetP4Request):
    """
    Runs the TargetP4 / description-audit pipeline:
      1. Reads the domain list from the 'Domains' tab
      2. Queries Target_P4_Opt + alternative_desc_change per domain (in parallel)
      3. Flips isRemote TRUE -> false through the external API
      4. Writes 'targetp4', 'Desc Not Found' and 'Stats' tabs
    """
    if not _TARGETP4_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A targetp4 pipeline is already running")

    try:
        handler = TargetP4Handler(max_workers=req.maxWorkers, retries=req.retries)
        summary = handler.run(
            domains=req.domains,
            update_is_remote=req.updateIsRemote,
            dry_run=req.dryRun,
        )

        spreadsheet_url = (
            f"https://docs.google.com/spreadsheets/d/{config.TARGETP4_SPREADSHEET_ID}/edit"
            if config.TARGETP4_SPREADSHEET_ID
            else None
        )

        return {
            "status": "success",
            "message": "TargetP4 export completed successfully",
            "summary": summary,
            "spreadsheetName": config.TARGETP4_SPREADSHEET_NAME,
            "spreadsheetUrl": spreadsheet_url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        _TARGETP4_LOCK.release()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005, use_colors=False)
