import os
import gspread
from typing import Dict, List, Optional, Any
from google.oauth2.service_account import Credentials
import config


class SheetsHandler:
    def __init__(
        self,
        creds_file: str = config.GOOGLE_CREDS_FILE,
        spreadsheet_id: Optional[str] = config.SPREADSHEET_ID,
        spreadsheet_name: Optional[str] = None,
    ):
        self.creds_file = creds_file
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_name = spreadsheet_name or config.SPREADSHEET_NAME
        self._client = None
        self._spreadsheet = None

    def get_client(self) -> gspread.Client:
        if self._client is None:
            if not os.path.exists(self.creds_file):
                raise RuntimeError(f"Service account file not found: {self.creds_file}")
            creds = Credentials.from_service_account_file(
                self.creds_file, scopes=config.GOOGLE_SCOPES
            )
            self._client = gspread.authorize(creds)
        return self._client

    def open_spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet is None:
            gc = self.get_client()
            if self.spreadsheet_id:
                self._spreadsheet = gc.open_by_key(self.spreadsheet_id)
            else:
                self._spreadsheet = gc.open(self.spreadsheet_name)
        return self._spreadsheet

    def get_or_create_worksheet(
        self,
        tab_name: str,
        rows: int = 200,
        cols: int = 30
    ) -> gspread.Worksheet:
        """Returns an existing tab (case-insensitive) or creates it."""
        sh = self.open_spreadsheet()
        ws = self.find_worksheet_case_insensitive(sh, tab_name)
        if ws is None:
            ws = sh.add_worksheet(title=tab_name, rows=rows, cols=cols)
        return ws

    def read_config_sheet(self, sheet_name: str = "domain details") -> Dict[str, Any]:
        """
        Reads configuration from the Google Sheet. Supports:
        1. Vertical Key-Value pairs (e.g. ['postings', '1000'], ['updates', '1000'])
        2. Horizontal table records with headers.
        """
        sh = self.open_spreadsheet()
        target_ws = None

        # Case-insensitive tab search
        for ws in sh.worksheets():
            if ws.title.strip().lower() in [sheet_name.lower(), "config", "domain details", "sheet1"]:
                target_ws = ws
                break

        if not target_ws:
            target_ws = sh.sheet1

        print(f"📊 Reading configuration from Google Sheet tab '{target_ws.title}'...")
        rows = target_ws.get_all_values()
        
        counts = {}
        row_dict = {
            "EmployerId": "68b8133b060e8f5df1a6c3a7",
            "Company": "Earn Haus",
            "Domain": "earnhaus",
        }

        for row in rows:
            if len(row) >= 2 and row[0] and str(row[1]).strip().isdigit():
                key = str(row[0]).strip().lower()
                val = int(str(row[1]).strip())
                if key in ["postings", "updates", "expires", "dateposted", "date_posted"]:
                    std_key = "datePosted" if key in ["dateposted", "date_posted"] else key
                    counts[std_key] = val
            elif len(row) >= 2 and row[0] and str(row[0]).strip().lower() in ["employerid", "company", "domain"]:
                row_dict[row[0].strip()] = str(row[1]).strip()

        if counts:
            row_dict["counts"] = counts
            print(f"✅ Dynamic Counts Loaded: {counts}")
            return row_dict

        # Fallback to get_all_records if standard table
        try:
            records = target_ws.get_all_records()
            if records:
                return records[0]
        except Exception:
            pass

        return row_dict

    def find_worksheet_case_insensitive(self, sh: gspread.Spreadsheet, tab_name: str) -> Optional[gspread.Worksheet]:
        clean_target = tab_name.strip().lower()
        for ws in sh.worksheets():
            if ws.title.strip().lower() == clean_target:
                return ws
        return None

    def write_tab(
        self,
        tab_name: str,
        header: List[str],
        rows: List[List[Any]],
        mode: str = "replace"
    ) -> int:
        """
        Writes data into a target worksheet tab (case-insensitive matching).
        """
        sh = self.open_spreadsheet()
        n_cols = max(len(header), 1)

        ws = self.find_worksheet_case_insensitive(sh, tab_name)
        if not ws:
            ws = sh.add_worksheet(title=tab_name, rows=max(len(rows) + 10, 100), cols=n_cols)

        if mode == "append":
            if rows:
                ws.append_rows(rows, value_input_option="RAW")
            return len(rows)

        # Replace mode
        values = [header] + rows
        ws.clear()
        ws.resize(rows=max(len(values), 2), cols=n_cols)
        try:
            ws.update(range_name="A1", values=values, value_input_option="RAW")
        except TypeError:
            ws.update("A1", values)

        try:
            ws.freeze(rows=1)
        except Exception:
            pass

        print(f"✅ Successfully written {len(rows)} rows to tab '{ws.title}'")
        return len(rows)

    def write_run_log(self, summary_rows: List[List[Any]]) -> None:
        """
        Appends run summary logs into the 'RunLog' worksheet tab.
        """
        header = ["RunAt (UTC)", "Company", "EmployerId", "Type", "Requested", "Exported", "Note"]
        sh = self.open_spreadsheet()
        ws = self.find_worksheet_case_insensitive(sh, "RunLog")
        if not ws:
            ws = sh.add_worksheet(title="RunLog", rows=500, cols=len(header))
            try:
                ws.update(range_name="A1", values=[header], value_input_option="RAW")
            except TypeError:
                ws.update("A1", [header])

        if summary_rows:
            ws.append_rows(summary_rows, value_input_option="RAW")
            print("📝 Appended run logs to 'RunLog' tab")
