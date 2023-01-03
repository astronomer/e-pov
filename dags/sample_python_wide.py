#This DAG reads the variables "DAG_COUNT" and "COUNT_PYTHON" to create the specified number of DAGs and Tasks
#Intended to be used for creating autoscaling examples

from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API

import time

DAG_COUNT = int(Variable.get("DAG_COUNT"))

for n in range(DAG_COUNT):
    with DAG(
        "simple_wide_python_" + str(n).zfill(3),
        schedule_interval=timedelta(seconds=60),
        start_date=datetime(2021, 8, 1),
        catchup=False,
        max_active_tasks=2000,
        concurrency=2000,
    ):
        COUNT = int(Variable.get("COUNT_PYTHON"))
        parent = None
        for i in range(int(COUNT)):

            @task(task_id="python_" + str(n).zfill(3))
            def pytask(msg=None, parent=None, queue='scale-test', **kwargs):
                """Print the Airflow context and ds variable from the context."""
                x = 2**32
                print(x)
                time.sleep(30)
                return msg
            this_task = pytask(msg=f'hello flat earth {i}', parent=parent)
            if parent:
                this_task.set_upstream(parent)
            parent = this_task

