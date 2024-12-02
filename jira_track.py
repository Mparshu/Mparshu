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
    """Check if the JIRA issue has a recent comment on a working day."""
    comments = issue["fields"]["comment"]["comments"]
    for comment in comments:
        updated = datetime.strptime(comment["updated"][:-9], "%Y-%m-%dT%H:%M:%S").date()
        # Check if the comment was updated in the last 24 hours and on a working day
        for days_back in range(1, 8):  # Look back up to 7 days to account for weekends and holidays
            check_date = datetime.now().date() - timedelta(days=days_back)
            if is_working_day(check_date, holidays) and updated == check_date:
                return True
    return False

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