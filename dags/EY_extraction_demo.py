from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

from airflow.providers.microsoft.azure.operators.asb import AzureServiceBusSendMessageOperator #import error

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
        SftpConnector = EmptyOperator(task_id='SftpConnector')

        #Ingestion pipeline - Move elz to ilz
        MoveELZtoILZ = EmptyOperator(task_id='MoveELZtoILZ')

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