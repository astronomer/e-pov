import os
from datetime import datetime
from include.utils.servicenow import create_incident
from airflow.decorators import dag, task

from airflow.models import Variable, Connection

@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    default_args={
        "email_on_failure": False,
        "retries": 0,
        "on_failure_callback": create_incident,
    },
    catchup=False,
    
    tags = ['EY']
)
def test_servicenow():

    @task
    def var_test():
        print(os.environ)
        credentials = Variable.get('google_cloud_default')

    @task
    def get_credentials():
        conn = Connection('google_cloud_default')
        print(conn)
        print(Connection.get_connection_from_secrets('google_cloud_default'))

    
    var_test() >> get_credentials()
 
test_servicenow()
