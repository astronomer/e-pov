#Just throwing some code in here. Has not been wired to work. DAG load will fail and this may not even be the best way to do it. 

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

from airflow.providers.microsoft.azure.operators.asb import AzureServiceBusSendMessageOperator #import error

def _copy_files_to_wasb(self, copy_files: list[CopyFile], **kwargs) -> list[str]:
        """Upload a list of files from sftp_files to Azure Blob Storage with a new Blob Name."""
        uploaded_files = []
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for file in copy_files:
            with NamedTemporaryFile("w") as tmp:
                self.sftp_hook.retrieve_file(file.sftp_file_path, tmp.name) #change to wasb get_file
                self.log.info(
                    "Uploading %s to wasb://%s as %s",
                    file.sftp_file_path,
                    self.container_name,
                    file.blob_name,
                )
                wasb_hook.load_file(
                    tmp.name,
                    self.container_name,
                    file.blob_name,
                    self.create_container,
                    **self.load_options,
                )

dag_owner = 'Astronomer'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='extraction_pipeline',
        default_args=default_args,
        description='',
        start_date=datetime(2022,12,1),
        schedule_interval='@daily',
        catchup=False,
        tags=['tdf']
):

        #Fetch metadata from control table, get schedule
        #This should probably run as a seperate pipeline that dynamically creates DAGs
        tdf_dhb_data_extration = EmptyOperator(task_id='tdf_dhb_data_extration')

        #Validate connections, trigger sftpconnector pipeline
        tdf_dhb_data_extraction_portal_sftp = EmptyOperator(task_id='tdf_dhb_data_extraction_portal_sftp')

        #Once SFTP pipeline is succeeded update status in extraction audit table
        tdf_dhb_data_extraction_check_status = EmptyOperator(task_id='tdf_dhb_data_extraction_check_status')
        
        #Connect using keyvault value of sftp creds and downlod sftp files into elz location
        #https://registry.astronomer.io/providers/microsoft-azure/modules/sftptowasboperator
        SftpConnector = SFTPToWasbOperator(
            task_id="SftpConnector",
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

        #Ingestion pipeline - Move elz to ilz
        MoveELZtoILZ = PythonOperator(
                task_id='MoveELZtoILZ',
                python_callable = _copy_files_to_wasb,
                op_kwargs={
                        #Add args here
                }
        )

        #Push message to Azure Service Bus
        #https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/5.0.1/operators/asb.html#send-message-to-azure-service-bus-queue
        PushASB = AzureServiceBusSendMessageOperator(
            task_id="PushASB",
            message=MESSAGE,
            queue_name=QUEUE_NAME,
            batch=False,
        )

        tdf_dhb_data_extration >> tdf_dhb_data_extraction_portal_sftp >> SftpConnector >> MoveELZtoILZ >> PushASB
        MoveELZtoILZ >> tdf_dhb_data_extraction_check_status