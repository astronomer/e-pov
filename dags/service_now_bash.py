from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
from datetime import timedelta
from airflow.operators.bash import BashOperator

def submit_ticket_to_service_now():
    # Set the request parameters
    url = '<to be changed> https://dev123451.service-now.com/api/now/table/incident'
    user = '<to be changed> admin'
    pwd = '<to be changed> cwW8CzdTD+4%'

    # Set the request headers
    headers = {"Content-Type":"application/json","Accept":"application/json"}

    # Set the request payload
    payload = {
        "short_description": "This is a test ticket",
        "description": "This is a test ticket description"
    }

    # Make the request to ServiceNow
    response = requests.post(url, auth=(user, pwd), headers=headers, json=payload)

    # Check the response status code
    if response.status_code == 201:
        print("Ticket successfully submitted to ServiceNow")
    else:
        print("Error submitting ticket to ServiceNow")

# Define the default_args dictionary
default_args = {
    'owner': 'me',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'submit_ticket_to_service_now_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Define the task
# submit_ticket_task = PythonOperator(
#     task_id='submit_ticket_to_service_now',
#     python_callable=submit_ticket_to_service_now,
#     dag=dag
# )

bash_task = BashOperator(
        task_id="bash_task",
        bash_command="exit 123",
        on_failure_callback=submit_ticket_to_service_now
    )

#submit_ticket_task
bash_task

