import json
import os
from datetime import datetime
from airflow.decorators import dag, task

from airflow.operators.bash import BashOperator

@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    default_args={
        "email": ["manmeet.rangoola@astronomer.io"],
        "email_on_failure": False,
        "retries": 0,
    },
    catchup=False,
    user_defined_filters={'fromjson_dict': lambda s: json.loads(json.dumps(s)), 'fromjson_string': lambda s: json.loads(s)}
)
def var_test_dag():

    @task
    def get_creds_return_dict(**kwargs):
        print(os.environ)
        kwargs['ti'].xcom_push(key="my_key", value={"my_nested_key": "my_value", "nextkey": "nextvalue", "justdummy": "dummyvalue"})

    @task
    def get_creds_return_string(**kwargs):
        print(os.environ)
        kwargs['ti'].xcom_push(key="my_key", value='{"my_nested_key": "my_value", "nextkey": "nextvalue", "justdummy": "dummyvalue"}')

    read_task_dict = BashOperator(
        task_id="read_task_dict",
        bash_command='echo "{{ (ti.xcom_pull(task_ids="get_creds_return_dict", key="my_key") | fromjson_dict)["my_nested_key"] }}" '
    )

    read_task_string = BashOperator(
        task_id="read_task_string",
        bash_command='echo "{{ (ti.xcom_pull(task_ids="get_creds_return_string", key="my_key") | fromjson_string)["my_nested_key"] }}" '
    )


    get_creds_return_dict() >> read_task_dict
    get_creds_return_string() >> read_task_string

my_dag = var_test_dag()
