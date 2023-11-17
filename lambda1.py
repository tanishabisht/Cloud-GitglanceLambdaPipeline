import requests
import json
import boto3
from botocore.exceptions import ClientError

# === variables ===
GH_TOKEN = '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'
URL = 'https://api.github.com/search/issues'
HEADERS = {
    'Authorization': f'Bearer {GH_TOKEN}'
}
TABLE_NAME = 'training_data'


def lambda_handler(event, context):
    print('event: ', event)
    
    page = event.get('page', 1)
    
    PARAMS = {
        'q': 'state:open',
        'per_page': 100,
        'page': page
    }
    
    response = requests.get(url=URL, params=PARAMS, headers=HEADERS)
    issues_repos = response.json()
    
    issues_list = []
    if response.status_code == 200:

        for repo in issues_repos['items']:
            api_repo_url = repo['repository_url']

            repo_info_response = requests.get(api_repo_url, headers=HEADERS)
            
            if repo_info_response.status_code == 200:
                repo_info_data = repo_info_response.json()
                
                _repo_url = '/'.join(repo['html_url'].split('/')[:-2])
                _repo_title = repo['title']
                _repo_language = repo_info_data.get('language', '')
                _repo_topics = repo_info_data.get('topics', [])
                _repo_forks_count = repo_info_data.get('forks_count', 0)
                _repo_stargazers_count = repo_info_data.get('stargazers_count', 0)
                _repo_open_issues_count = repo_info_data.get('open_issues_count', 0)
                
                issue_info = {
                    'repo_url': _repo_url,
                    'title': _repo_title,
                    'language': _repo_language,
                    'topics': _repo_topics,
                    'forks_count': _repo_forks_count,
                    'stargazers_count': _repo_stargazers_count,
                    'open_issues_count': _repo_open_issues_count,
                }

                issues_list.append(issue_info)
                insert_data(issue_info)
            else:
                print(repo_info_data)
                print(f"ERROR: Repository Info --> {repo_info_response.status_code}")

        next_page = page + 1
        updated_event = {'page': next_page}
        print("Inserted successfully")
        return updated_event

    else:
        print(f"Error: {response.status_code} :: Text: {response.text}")

def insert_data(data, db=None, table=TABLE_NAME):
    if not db:
        db = boto3.resource('dynamodb')
        _table = db.Table(table)
    if data:
        response = _table.put_item(Item=data)
