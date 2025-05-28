import requests
import os
import json
from datetime import datetime, timezone
import csv # For writing to CSV

# --- Configuration: Load from Environment Variables ---
# !! Make sure to set these in your terminal before running !!
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")
JIRA_PROJECT_KEY = "SEC" # Your Jira project key

# --- CSV Filename ---
CSV_FILENAME = "metrics.csv"

# --- Check if config is set ---
if not all([JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN]):
    print("FATAL ERROR: Please set JIRA_URL, JIRA_EMAIL, and JIRA_API_TOKEN environment variables.")
    exit(1)

# --- Function to Get Data from Jira ---
def fetch_jira_data():
    print("Fetching data from Jira...")
    # JQL to find 'Done' issues in our project that have a resolution date
    jql_query = f"project = {JIRA_PROJECT_KEY} AND status = Done AND resolutiondate is not empty"

    search_url = f"{JIRA_URL}/rest/api/3/search"
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json"}
    params = {
        'jql': jql_query,
        'fields': 'created,resolutiondate,summary', # Ask for the key fields
        'maxResults': 100 # Get up to 100 issues (can be expanded with pagination later)
    }

    try:
        response = requests.get(search_url, headers=headers, params=params, auth=auth, timeout=15)
        response.raise_for_status() # Check for errors (4xx or 5xx)
        issues = response.json().get('issues', [])
        print(f"Found {len(issues)} resolved issues.")
        return issues
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Jira data: {e}")
        if e.response is not None:
            print(f"Response Status: {e.response.status_code}")
            print(f"Response Text: {e.response.text}")
        return []

# --- Function to Calculate Metrics ---
def calculate_mttr(issues):
    print("Calculating MTTR...")
    metrics_data = []
    for issue in issues:
        fields = issue.get('fields', {})
        key = issue.get('key')
        summary = fields.get('summary')
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

                metrics_data.append({
                    "key": key,
                    "summary": summary,
                    "created": created_str, # Store original for sheet
                    "resolved": resolved_str, # Store original for sheet
                    "mttr_hours": round(mttr_hours, 2)
                })
            except ValueError as ve:
                 print(f"  - {key}: Could not parse dates. Original created: '{created_str}', Reformatted: '{created_dt_str_reformatted if 'created_dt_str_reformatted' in locals() else 'N/A'}'. Original resolved: '{resolved_str}', Reformatted: '{resolved_dt_str_reformatted if 'resolved_dt_str_reformatted' in locals() else 'N/A'}'. Error: {ve}")
        else:
            print(f"  - {key}: Missing created or resolved date, skipping MTTR calculation.")

    if metrics_data:
        print(f"Successfully calculated MTTR for {len(metrics_data)} issues.")
    else:
        print("No issues had valid dates to calculate MTTR.")
    return metrics_data

# --- Function to Write to CSV File ---
def write_to_csv(metrics, filename=CSV_FILENAME):
    if not metrics:
        print("No metrics to write to CSV.")
        return

    print(f"Writing {len(metrics)} records to {filename}...")
    try:
        # Define the headers for your CSV file
        headers = ["Issue Key", "Summary", "Created Date", "Resolved Date", "MTTR (Hours)"]

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers) # Write the header row
            for metric_item in metrics:
                writer.writerow([
                    metric_item["key"],
                    metric_item["summary"],
                    metric_item["created"],
                    metric_item["resolved"],
                    metric_item["mttr_hours"]
                ])
        print(f"Successfully wrote data to {filename}.")
    except Exception as e:
        print(f"An unexpected error occurred writing to CSV: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting DevSecOps Metrics ETL process...")
    jira_issues = fetch_jira_data()
    if jira_issues: # Only proceed if we actually got some issues
        calculated_metrics_data = calculate_mttr(jira_issues)
        if calculated_metrics_data: # Only write if we calculated some metrics
            write_to_csv(calculated_metrics_data)
    print("ETL process finished.")
