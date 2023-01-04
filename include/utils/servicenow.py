import requests
import json

def create_incident(context):
    print("in failure callback")
    print(context)
    username = "api_user"
    password = "your_password"

    url = "https://dev114787.service-now.com/api/now/v1/table/incident"

    payload = json.dumps({
    "assigned_to": "manmeet",
    "due_date": "2023-01-15",
    "business_impact": "High",
    "category": "Airflow",
    "short_description":"Auto-generated ticket for Airflow DAG " + context['dag'].dag_id
    })
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
    }

    response = requests.request("POST", url, auth=(username, password), headers=headers, data=payload)
    print(response.status_code)
    print(response.text)
    return (response.status_code, response.text)


