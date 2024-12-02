def generate_table(data, headers):
    """
    Generate an HTML table with styling.
    """
    table_style = """
    <style>
        table {
            border-collapse: collapse;
            margin: auto; /* Center align table */
            padding: 10px;
            width: 80%; /* Adjust width as needed */
        }
        th, td {
            border: 1px solid black;
            text-align: center;
            padding: 8px;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
    """
    table_html = "<table>"
    # Add table headers
    table_html += "<tr>"
    for header in headers:
        table_html += f"<th>{header}</th>"
    table_html += "</tr>"

    # Add table rows
    for row in data:
        table_html += "<tr>"
        for header in headers:
            table_html += f"<td>{row.get(header, '')}</td>"
        table_html += "</tr>"
    
    table_html += "</table>"
    return table_style + table_html



footer_message = """
<p style="text-align: center; margin-top: 20px;">
    <strong>Thanks and Regards,</strong><br>
    Jira Admin Team
</p>
<p style="text-align: center; font-size: 12px; color: gray;">
    This is an automated mail, please do not reply to this mail.
</p>
"""



    html_body = "<h1 style='text-align: center;'>JIRA Status Report</h1>"

if no_jira_employees:
    html_body += "<h2 style='text-align: center;'>Employees without assigned JIRAs</h2>"
    html_body += generate_table(no_jira_employees, ["Employee"])

if no_comment_jiras:
    html_body += "<h2 style='text-align: center;'>JIRAs without recent comments</h2>"
    html_body += generate_table(no_comment_jiras, ["Employee", "JIRA"])

if multiple_jiras:
    html_body += "<h2 style='text-align: center;'>Employees with multiple JIRAs</h2>"
    html_body += generate_table(multiple_jiras, ["Employee", "JIRAs"])

if detailed_jiras:
    html_body += "<h2 style='text-align: center;'>Detailed JIRA Information</h2>"
    html_body += generate_table(detailed_jiras, ["JIRA", "Assignee", "Last Comment", "Last Comment Timestamp"])

# Append the footer message
html_body += footer_message


    # Send email
    send_email(MANAGER_EMAIL, "JIRA Status Report", html_body)

if __name__ == "__main__":
    main()