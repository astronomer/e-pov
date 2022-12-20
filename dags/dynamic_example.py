#example from https://docs.astronomer.io/learn/dynamic-tasks

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'Astronomer'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dynamic_task',
        default_args=default_args,
        description='Generates tasks dynamically',
        start_date=datetime(2022,12,1),
        schedule_interval='@daily',
        catchup=False,
        tags=['example']
):

        @task
        def input_array():
            return [1,2,3]

        @task
        def plus_10(x):
            return x+10

        plus_10.partial().expand(x=input_array())