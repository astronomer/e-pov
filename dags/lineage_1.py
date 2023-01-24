"""
This is the Main DAG for Cosmic Energy Organization to process the public data present in S3 Bucket noaa-ghcn-pds. 

The tasks that it performs are as below:
1. Check if the Control File ghcnd-version.txt is present in the location s3://noaa-ghcn-pds/
2. List files starting with prefix ghcnd from s3://noaa-ghcn-pds/
3. Use Dynamic Task mapping for each of the files returned by Step #2 to do
  3a. Copy over from the Public Bucket to Cosmic Energy's Incoming Bucket
  3b. Process and save the files in Cosmic Energy's Outgoing Bucket using Databricks
4. 
"""

import os
from datetime import datetime, timedelta
from include.helpers.callbacks import success_email
from typing import List
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.models import Variable

# from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator


default_args = {
    'owner': 'Cosmic Energy DE',
    'email': ['manmeet.rangoola@astronomer.io'],
    'email_on_failure': False,
    'sla': timedelta(minutes=30) ## applicable to only scheduled tasks ; relative to DAG Execution Date not Task Start Time
}

with DAG('process_gcs_s3_snowflake_data_pipeline'
        , start_date=datetime(2022,8,18)
        , catchup=False
        , max_active_runs=1
        , schedule_interval='@daily'
        , default_args=default_args
        , tags = ['transform', 'daily', 's3', 'snowflake', 'lineage', 'gcs', 'sql'],
    ) as dag:

    task_covid_spread=SnowflakeOperator(
        task_id='snowflake_run_sql_from_file_spread',
        sql='sql/covid19/covid_spread.sql',
        snowflake_conn_id='snowflake',
    )


    task_covid_vacc=SnowflakeOperator(
        task_id='snowflake_run_sql_from_file_vacc',
        sql='sql/covid19/covid_vacc.sql',
        snowflake_conn_id='snowflake',
    )

    # The set of files to be processed..
    # In production this set of files can be set via variable or
    # enumerate from the S3 bucket 
    in_files = [
        "index.csv",
        "demographics.csv",
        "epidemiology.csv",
        "vaccinations.csv"
    ]

    for idx, in_file in enumerate(in_files):
        # file_name
        task_copy_files=GCSToS3Operator(
            task_id="gcs_to_s3_{}".format(in_file.split('.')[0]), 
            bucket='covid19-open-data',
            delimiter='.csv',
            dest_aws_conn_id='aws_default',
            prefix=f'v3/{in_file}',
            dest_s3_key='s3://astronomer-field-engineering-demo/incoming/covid19/{{ ds_nodash }}',
            replace=True,
        )

        task_load_files=S3ToSnowflakeOperator(
            task_id='s3_to_snowflake_{}'.format(in_file.split('.')[0]),
            snowflake_conn_id='snowflake',
            s3_keys=[os.path.join('{{ ds_nodash }}', 'v3', in_file)],
            table='s3_to_snowflake_{}'.format(in_file.split('.')[0]),
            schema='aws_stg_cosmic_energy',
            stage='s3_covid',
            file_format="(type = CSV, field_delimiter = ',', SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )
        
        task_transform=SnowflakeOperator(
            task_id='snowflake_op_{}'.format(in_file.split('.')[0]),
            sql='sql/covid19/{}.sql'.format(in_file.split('.')[0]),
            snowflake_conn_id='snowflake',
            on_success_callback=success_email
        )

        task_copy_files >> task_load_files >> task_transform >> task_covid_spread >> task_covid_vacc

