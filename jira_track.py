import http.client
import json
import urllib.parse

# Configuration
jira_api_token = 'your_bearer_token'  # Use your actual Bearer token here
jira_instance = 'your_jira_instance'  # e.g., 'yourdomain.atlassian.net'
assignee_username = 'assignee_username'

def get_issues_assigned_to_user(assignee):
    conn = http.client.HTTPSConnection(jira_instance)
    jql_query = f"assignee={assignee}"
    params = urllib.parse.urlencode({
        'jql': jql_query,
        'fields': 'key',
        'maxResults': 100
    })
    
    headers = {
        'Authorization': f'Bearer {jira_api_token}',
        'Accept': 'application/json'
    }
    
    conn.request("GET", f"/rest/api/2/search?{params}", headers=headers)
    
    response = conn.getresponse()
    data = response.read().decode()
    
    if response.status == 200:
        return json.loads(data).get('issues', [])
    else:
        print(f"Failed to fetch issues: {response.status} - {data}")
        return []

def get_last_comment(issue_key):
    conn = http.client.HTTPSConnection(jira_instance)
    
    headers = {
        'Authorization': f'Bearer {jira_api_token}',
        'Accept': 'application/json'
    }
    
    conn.request("GET", f"/rest/api/2/issue/{issue_key}/comment?orderBy=-created&maxResults=1", headers=headers)
    
    response = conn.getresponse()
    data = response.read().decode()
    
    if response.status == 200:
        comments = json.loads(data).get('comments', [])
        if comments:
            last_comment = comments[0]
            return last_comment['body'], last_comment['created']
        else:
            return None, None
    else:
        print(f"Failed to fetch comments for issue {issue_key}: {response.status} - {data}")
        return None, None

def main():
    issues = get_issues_assigned_to_user(assignee_username)
    
    for issue in issues:
        issue_key = issue['key']
        last_comment_body, last_comment_time = get_last_comment(issue_key)
        
        if last_comment_body and last_comment_time:
            print(f"Issue: {issue_key}")
            print(f"Last Comment: {last_comment_body}")
            print(f"Comment Time: {last_comment_time}")
            print("-" * 40)

if __name__ == "__main__":
    main()




import requests

# Configuration
jira_url = 'https://your_jira_instance.atlassian.net/rest/api/2/serverInfo'  # Replace with your JIRA instance URL
jira_api_token = 'your_api_token'  # Your JIRA API token
jira_email = 'your_email@example.com'  # Your JIRA email

def test_jira_connection():
    headers = {
        'Authorization': f'Bearer {jira_api_token}',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(jira_url, headers=headers)
        
        if response.status_code == 200:
            print("Connection successful!")
            print("Response:", response.json())
        else:
            print(f"Failed to connect: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_jira_connection()