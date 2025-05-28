import requests
import os
import json
from datetime import datetime, timezone
import gspread # <-- NEW: Import gspread
from google.oauth2.service_account import Credentials # <-- NEW: Import Google Auth

# --- Configuration: Load from Environment Variables ---
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")
JIRA_PROJECT_KEY = "SEC"

# --- NEW: Google Sheets Config ---
GOOGLE_SHEET_NAME = "DevSecOps Metrics" # <-- Make sure this matches your sheet name
# !! SET THIS ENV VARIABLE to the path of your downloaded JSON key file !!
GOOGLE_SHEETS_CREDS_JSON_PATH = os.environ.get("GOOGLE_SHEETS_CREDS_JSON_PATH")

# --- Check if config is set ---
if not all([JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN, GOOGLE_SHEETS_CREDS_JSON_PATH]):
    print("FATAL ERROR: Please set JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN,")
    print("             AND GOOGLE_SHEETS_CREDS_JSON_PATH environment variables.")
    exit(1)

# --- Function to Get Data from Jira (Same as before) ---
def fetch_jira_data():
    print("Fetching data from Jira...")
    jql_query = f"project = {JIRA_PROJECT_KEY} AND status = Done AND resolutiondate is not empty"
    search_url = f"{JIRA_URL}/rest/api/3/search"
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json"}
    params = {'jql': jql_query, 'fields': 'created,resolutiondate,summary', 'maxResults': 100}

    try:
        response = requests.get(search_url, headers=headers, params=params, auth=auth, timeout=15)
        response.raise_for_status()
        issues = response.json().get('issues', [])
        print(f"Found {len(issues)} resolved issues.")
        return issues
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Jira data: {e}")
        return []

# --- Function to Calculate Metrics (Updated Date Parsing) ---
def calculate_mttr(issues):
    print("Calculating MTTR...")
    metrics = []
    for issue in issues:
        fields = issue.get('fields', {})
        key = issue.get('key')
        created_str = fields.get('created')
        resolved_str = fields.get('resolutiondate')

        if created_str and resolved_str:
            try:
                # Helper function to reformat Jira's timestamp if needed
                def reformat_jira_timestamp(ts_str):
                    if ts_str is None: return None
                    # Handle Zulu time first
                    ts_str = ts_str.replace('Z', '+00:00')
                    # Check if timezone offset has a colon, if not, add it
                    # e.g., turns +0700 into +07:00 or -0500 into -05:00
                    if len(ts_str) >= 5 and ts_str[-5] in ['+', '-'] and ts_str[-3] != ':':
                        return ts_str[:-2] + ":" + ts_str[-2:]
                    return ts_str

                created_dt_str_reformatted = reformat_jira_timestamp(created_str)
                resolved_dt_str_reformatted = reformat_jira_timestamp(resolved_str)
                
                created_dt = datetime.fromisoformat(created_dt_str_reformatted)
                resolved_dt = datetime.fromisoformat(resolved_dt_str_reformatted)
                
                time_to_resolve = resolved_dt - created_dt
                mttr_hours = time_to_resolve.total_seconds() / 3600
                
                metrics.append({
                    "key": key,
                    "summary": fields.get('summary'),
                    "created": created_str, # Store original for sheet
                    "resolved": resolved_str, # Store original for sheet
                    "mttr_hours": round(mttr_hours, 2)
                })
            except ValueError as ve:
                 print(f"  - {key}: Could not parse dates. Original created: '{created_str}', Reformatted: '{created_dt_str_reformatted}'. Original resolved: '{resolved_str}', Reformatted: '{resolved_dt_str_reformatted}'. Error: {ve}")
        else:
            print(f"  - {key}: Missing created or resolved date.")
            
    if metrics: # Only print this if we successfully calculated some metrics
        print(f"Successfully calculated MTTR for {len(metrics)} issues.")
    else:
        print("No issues had valid dates to calculate MTTR.")
    return metrics

# --- NEW: Function to Write to Google Sheets ---
def write_to_google_sheet(metrics):
    if not metrics:
        print("No metrics to write to Google Sheets.")
        return

    print(f"Writing {len(metrics)} records to Google Sheets...")
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDS_JSON_PATH, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1 # Opens the *first* sheet

        # Prepare data (Header + Rows)
        data_to_upload = [["Issue Key", "Summary", "Created Date", "Resolved Date", "MTTR (Hours)"]]
        for metric in metrics:
            data_to_upload.append([
                metric["key"], metric["summary"], metric["created"],
                metric["resolved"], metric["mttr_hours"]
            ])

        sheet.clear() # Clear the sheet before writing new data
        sheet.update('A1', data_to_upload) # Write all data starting at A1

        print("Successfully wrote data to Google Sheets.")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERROR: Spreadsheet '{GOOGLE_SHEET_NAME}' not found. Did you name it correctly?")
    except gspread.exceptions.APIError as ge:
        print(f"ERROR: Google Sheets API error: {ge}. Did you share the sheet with the service account email?")
    except Exception as e:
        print(f"An unexpected error occurred writing to Google Sheets: {e}")

# --- Main Execution (Now includes writing to sheets) ---
if __name__ == "__main__":
    print("Starting DevSecOps Metrics ETL process...")
    jira_issues = fetch_jira_data()
    if jira_issues:
        calculated_metrics = calculate_mttr(jira_issues)
        write_to_google_sheet(calculated_metrics)
    print("ETL process finished.")