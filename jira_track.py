import csv
import json
import requests
from datetime import datetime, timedelta
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Load configuration
with open("config.json") as config_file:
    config = json.load(config_file)

JIRA_BASE_URL = config["jira_url"]
API_TOKEN = config["api_token"]
MANAGER_EMAIL = config["manager_email"]
SMTP_SERVER = config["smtp_server"]
SMTP_PORT = config["smtp_port"]
FROM_EMAIL = config["from_email"]
FROM_EMAIL_PASSWORD = config["from_email_password"]

# Load employee data
def load_employees(file_path="employees.csv"):
    employees = []
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            employees.append(row)
    return employees

# Fetch JIRA issues assigned to an employee
def fetch_jiras(assignee):
    url = f"{JIRA_BASE_URL}/rest/api/2/search"
    query = f"assignee={assignee}"
    params = {"jql": query, "fields": "id,comment,summary", "maxResults": 100}
    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("issues", [])
    else:
        print(f"Error fetching JIRAs for {assignee}: {response.status_code}")
        return []

# Check for comments in the last 24 hours
def has_recent_comment(issue):
    comments = issue["fields"]["comment"]["comments"]
    for comment in comments:
        updated = datetime.strptime(comment["updated"][:-9], "%Y-%m-%dT%H:%M:%S")
        if updated >= datetime.now() - timedelta(days=1):
            return True
    return False

# Send HTML email
def send_email(to_email, subject, body):
    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        with SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
            server.sendmail(FROM_EMAIL, to_email, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Main workflow
def main():
    employees = load_employees()

    no_jira_employees = []
    no_comment_jiras = []
    multiple_jiras = []

    for employee in employees:
        jiras = fetch_jiras(employee["Email"])

        if not jiras:
            no_jira_employees.append(employee["Name"])
        else:
            if len(jiras) > 1:
                multiple_jiras.append({"name": employee["Name"], "jiras": [jira["key"] for jira in jiras]})

            for jira in jiras:
                if not has_recent_comment(jira):
                    no_comment_jiras.append({"name": employee["Name"], "jira": jira["key"]})

    # Generate HTML email content
    html_body = "<h1>JIRA Status Report</h1>"

    if no_jira_employees:
        html_body += "<h2>Employees without assigned JIRAs</h2><ul>"
        html_body += "".join(f"<li>{name}</li>" for name in no_jira_employees)
        html_body += "</ul>"

    if no_comment_jiras:
        html_body += "<h2>JIRAs without recent comments</h2><ul>"
        html_body += "".join(
            f"<li>Employee: {item['name']}, JIRA: {item['jira']}</li>" for item in no_comment_jiras
        )
        html_body += "</ul>"

    if multiple_jiras:
        html_body += "<h2>Employees with multiple JIRAs</h2><ul>"
        html_body += "".join(
            f"<li>Employee: {item['name']}, JIRAs: {', '.join(item['jiras'])}</li>" for item in multiple_jiras
        )
        html_body += "</ul>"

    # Send email
    send_email(MANAGER_EMAIL, "JIRA Status Report", html_body)

if __name__ == "__main__":
    main()