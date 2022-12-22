from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from astronomer.providers.databricks.operators.databricks import DatabricksSubmitRunOperatorAsync
from alerts.slack import (
    task_success_slack_alert,
    task_failure_slack_alert,
    task_retry_slack_alert,
    slack_sla_miss_alert
)
from tempfile import NamedTemporaryFile

dag_owner = 'astronomer'
default_args = {"owner": dag_owner,
                "depends_on_past": False, 
                "retries": 2,
                "retry_delay": timedelta(minutes=5)
                }

AZURE_CONTAINER_NAME = 'sftp-transfer'
json_databricks = {
  "existing_cluster_id" : '0815-184738-451nanbo',
  "notebook_task": {
    "notebook_path" : f"/Users/grayson.stream@astronomer.io/sftp_transform",
    "base_parameters": {"airflow_job":"azure_sftp_transfer",
                        "load_date": """{{ ds_nodash }}""",
                        }
  }
}

# def copy_files_to_wasb(self, sftp_files: list[SftpFile]) -> list[str]:
#         """Upload a list of files from sftp_files to Azure Blob Storage with a new Blob Name."""
#         uploaded_files = []
#         wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
#         for file in sftp_files:
#             with NamedTemporaryFile("w") as tmp:
#                 self.sftp_hook.retrieve_file(file.sftp_file_path, tmp.name)
#                 self.log.info(
#                     "Uploading %s to wasb://%s as %s",
#                     file.sftp_file_path,
#                     self.container_name,
#                     file.blob_name,
#                 )
#                 wasb_hook.load_file(
#                     tmp.name,
#                     self.container_name,
#                     file.blob_name,
#                     self.create_container,
#                     **self.load_options,
#                 )


with DAG(dag_id='azure_sftp_transfer',
    default_args=default_args,
    description="Pull files from an SFTP lcoation and move to a blob storage location",
    start_date=datetime(2022,11,13),
    schedule_interval="0 14 * * *",
    catchup=False,
    tags=["sftp", "azure_blob", "dynamic_task_mapping"]
):
    exec_date = "{{ ds_nodash }}"

    transfer_sftp = SFTPToWasbOperator(
            task_id="transfer_files_from_sftp_to_wasb",
            # SFTP args
            sftp_conn_id="sftp_docker",
            sftp_source_path=f"/home/fe_shepard/{exec_date}",
            # AZURE args
            container_name=AZURE_CONTAINER_NAME,
            blob_prefix=f"raw/{exec_date}/",
            wasb_conn_id="wasb_docker",
            wasb_overwrite_object=True,
            on_failure_callback=task_failure_slack_alert,
            on_success_callback=task_success_slack_alert,
            on_retry_callback=task_retry_slack_alert)
    
    databricks_transform_async = DatabricksSubmitRunOperatorAsync(
            task_id='databricks_transform', 
            json=json_databricks,
            databricks_conn_id='databricks_docker',
            polling_period_seconds=10,
            do_xcom_push=True,
            on_failure_callback=task_failure_slack_alert,
            on_success_callback=task_success_slack_alert,
            on_retry_callback=task_retry_slack_alert
      )
    
    transfer_sftp >> databricks_transform_async