def load_holidays(file_path="holidays.csv"):
    """Load holiday dates from a CSV file."""
    holidays = set()
    with open(file_path, mode="r") as file:
        reader = csv.reader(file)
        for row in reader:
            holidays.add(datetime.strptime(row[0], "%Y-%m-%d").date())
    return holidays

def is_working_day(date, holidays):
    """Check if a given date is a working day."""
    # Exclude weekends (Saturday and Sunday)
    if date.weekday() in (5, 6):  # 5 = Saturday, 6 = Sunday
        return False
    # Exclude holidays
    if date in holidays:
        return False
    return True

def has_recent_comment(issue, holidays):
    """Check if the JIRA issue has a comment updated on the last working day."""
    comments = issue["fields"]["comment"]["comments"]
    
    # Get the last working day
    today = datetime.now().date()
    last_working_day = today - timedelta(days=1)
    while not is_working_day(last_working_day, holidays):
        last_working_day -= timedelta(days=1)
    
    # If there are no comments, return False
    if not comments:
        return False
    
    # Get the last comment's updated date
    last_comment = comments[-1]
    updated = datetime.strptime(last_comment["updated"][:-9], "%Y-%m-%dT%H:%M:%S").date()

    # Check if the last comment was updated on the last working day
    return updated == last_working_day

def main():
    employees = load_employees()
    holidays = load_holidays()  # Load holidays from CSV

    no_jira_employees = []
    no_comment_jiras = []
    multiple_jiras = []
    detailed_jiras = []

    for employee in employees:
        jiras = fetch_jiras(employee["Email"])

        if not jiras:
            no_jira_employees.append({"Employee": employee["Name"]})
        else:
            if len(jiras) > 1:
                multiple_jiras.append({
                    "Employee": employee["Name"],
                    "JIRAs": ", ".join(
                        [f"<a href='{JIRA_BASE_URL}/browse/{jira['key']}'>{jira['key']}</a>" for jira in jiras]
                    )
                })

            for jira in jiras:
                last_comment = jira["fields"]["comment"]["comments"][-1] if jira["fields"]["comment"]["comments"] else None
                detailed_jiras.append({
                    "JIRA": f"<a href='{JIRA_BASE_URL}/browse/{jira['key']}'>{jira['key']}</a>",
                    "Assignee": employee["Name"],
                    "Last Comment": last_comment["body"] if last_comment else "No Comments",
                    "Last Comment Timestamp": last_comment["updated"] if last_comment else "N/A"
                })

                if not has_recent_comment(jira, holidays):
                    no_comment_jiras.append({
                        "Employee": employee["Name"],
                        "JIRA": f"<a href='{JIRA_BASE_URL}/browse/{jira['key']}'>{jira['key']}</a>"
                    })

    # Generate HTML email content
    html_body = "<h1>JIRA Status Report</h1>"

    if no_jira_employees:
        html_body += "<h2>Employees without assigned JIRAs</h2>"
        html_body += generate_table(no_jira_employees, ["Employee"])

    if no_comment_jiras:
        html_body += "<h2>JIRAs without recent comments</h2>"
        html_body += generate_table(no_comment_jiras, ["Employee", "JIRA"])

    if multiple_jiras:
        html_body += "<h2>Employees with multiple JIRAs</h2>"
        html_body += generate_table(multiple_jiras, ["Employee", "JIRAs"])

    if detailed_jiras:
        html_body += "<h2>Detailed JIRA Information</h2>"
        html_body += generate_table(detailed_jiras, ["JIRA", "Assignee", "Last Comment", "Last Comment Timestamp"])

    # Send email
    send_email(MANAGER_EMAIL, "JIRA Status Report", html_body)

if __name__ == "__main__":
    main()