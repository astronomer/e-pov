import os
import json
import random
import io
import pandas as pd
from datetime import timedelta, datetime
from include.utils.servicenow import create_incident

from airflow import DAG, XComArg
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, task_group
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.hooks.synapse import AzureSynapseHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator


default_args={
    "email": ["manmeet.rangoola@astronomer.io"],
    "email_on_failure": False,
    "retries": 0,
    "on_failure_callback": create_incident,  ## create a service now incident automatically on failure
}

def check_file_count(**kwargs):
    num_of_files = kwargs['ti'].xcom_pull(task_ids='read_data_from_file')
    print(len(num_of_files))
    if len(num_of_files) > 2:
        return True ## means continue with the DAG run
    else:
        return False ## False means short circuit the DAG and stop running


def choose_branch(options):
    return random.choice(options)


class HttpOperator(SimpleHttpOperator):
    def __init__(self, local_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_path = local_path
    
    def execute(self, context):
        data = json.loads(super().execute(context))
        with open(self.local_path, 'w') as f:
            for row in data:
                f.write(json.dumps(row) + '\n')

with DAG(dag_id='ey_usecase',
    default_args=default_args,
    description="EY Use Case",
    start_date=datetime(2022,12,22),
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=["ey"]
):


    skip_or_run=ShortCircuitOperator(
        task_id="skip_or_run",
        python_callable=check_file_count
    )

    @task
    def read_data_from_file():
        azurehook = WasbHook(wasb_conn_id="wasb_astrofedemo")
        print(azurehook)

        data = azurehook.read_file(container_name='astrofedemo', blob_name='incoming/sample.csv')
        print(data)
        buffer = io.StringIO(data)
        df = pd.read_csv(buffer)
        return df['name'].tolist()

    @task
    def loop_and_create_sql(table_list: list):
        sql_list = []
        for table_name in table_list:
            sql_list.append(f"create or replace table demo.aws_raw_cosmic_energy.{table_name.strip()}(id int, name string)")
        
        print(sql_list)
        return sql_list

    table_list = read_data_from_file()
    sql_list = loop_and_create_sql(table_list)

    table_list >> skip_or_run >> sql_list
    

    task_load_data = SnowflakeOperator.partial(
        task_id='create_table',
        snowflake_conn_id='snowflake'
    ).expand(sql=sql_list)
    

    options = ["one", "two"]

    task_choose_branch = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        op_args=[options]
    )

    api_data_pull=HttpOperator(
        task_id='api_data_pull',
        method='GET',
        http_conn_id='api_test',
        endpoint='users/',
        local_path=os.path.join(os.getcwd(), 'api_data.json'),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    for option in options:
        t = EmptyOperator(
            task_id=option
        )

        sql_list >> task_load_data >> task_choose_branch >> t >> api_data_pull