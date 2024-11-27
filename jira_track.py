import requests
from requests.auth import HTTPBasicAuth

# Configuration
jira_email = 'your_email@example.com'
jira_api_token = 'your_api_token'
jira_instance = 'https://your_jira_instance'
assignee_username = 'assignee_username'

def get_issues_assigned_to_user(assignee):
    url = f"{jira_instance}/rest/api/2/search"
    jql_query = f"assignee={assignee}"
    params = {
        'jql': jql_query,
        'fields': 'key',  # We only need the issue key for further requests
        'maxResults': 100  # Adjust based on your needs
    }
    
    response = requests.get(url, params=params, auth=HTTPBasicAuth(jira_email, jira_api_token))
    
    if response.status_code == 200:
        return response.json().get('issues', [])
    else:
        print(f"Failed to fetch issues: {response.status_code} - {response.text}")
        return []

def get_last_comment(issue_key):
    url = f"{jira_instance}/rest/api/2/issue/{issue_key}/comment"
    params = {
        'orderBy': '-created',
        'maxResults': 1
    }
    
    response = requests.get(url, params=params, auth=HTTPBasicAuth(jira_email, jira_api_token))
    
    if response.status_code == 200:
        comments = response.json().get('comments', [])
        if comments:
            last_comment = comments[0]
            return last_comment['body'], last_comment['created']
        else:
            return None, None
    else:
        print(f"Failed to fetch comments for issue {issue_key}: {response.status_code} - {response.text}")
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