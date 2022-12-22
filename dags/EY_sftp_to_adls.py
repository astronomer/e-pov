import re, os, tempfile, json
from datetime import datetime, timedelta 

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from airflow import DAG

from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

def _list_sftp_files(sftp_conn_id, sftp_directory, file_regex, **kwargs):
    file_regex = re.compile(file_regex, re.I)
    print(file_regex)
    lb_ts = kwargs["execution_date"].timestamp()
    ub_ts = kwargs["next_execution_date"].timestamp()
    try:
        sftp_hook = SFTPHook(sftp_conn_id)
        sftp_dir_list = sftp_hook.describe_directory(sftp_directory)
        files = {}
        for file_name, file_info in sftp_dir_list.items():
            file_mtime = datetime.strptime(file_info["modify"], '%Y%m%d%H%M%S').timestamp()
            if lb_ts <= file_mtime < ub_ts:
                if file_regex.fullmatch(file_name):
                    files[file_name] = {
                        "size": file_info["size"],
                        "type": file_info["type"],
                        "mtime": file_mtime,
                    }
    finally:
        sftp_hook.close_conn()

    json_files = json.dumps(files)
    return json_files 

def file_check(files, **kwargs):
    print(files)
    if files == "{}":
        return False
    else:
        return True


# ADLS Upload
def _connect_adls(storage_account_name, adls_conn_id):

    adls_conn_creds = BaseHook.get_connection(adls_conn_id)
    client_id = adls_conn_creds.login
    client_secret = adls_conn_creds.password
    tenant_id = adls_conn_creds.extra_dejson["extra__azure_data_lake__tenant"]

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)

    print("ADLS Connection successful.")

    return service_client

def _alds_chunk_transfer(file_client, local_filepath, upload_chunksize):
    try:
        offset = 0

        with open(local_filepath, "rb") as _local_filepath:
            while True:
                file_chunk = _local_filepath.read(upload_chunksize)
                len_file_chunk = len(file_chunk)

                if file_chunk:
                    file_client.append_data(data=file_chunk, offset=offset, length=len_file_chunk)

                    print(f"Sent file_chunk {offset} to {offset + len_file_chunk}.")
                    offset += len_file_chunk

                else:
                    file_client.flush_data(offset)
                    break
    
    except Exception:
        print(f"Deleting {local_filepath} on ADLS, as the connection failed.")
        file_client.delete_file()
        raise
    
    else:
        print("File transfer successfully completed.")

def _adls_file_upload(local_filepath, storage_account_name, adls_conn_id, adls_file_system, adls_directory, alds_filename, upload_chunksize):

    service_client = _connect_adls(storage_account_name, adls_conn_id)

    file_system_client = service_client.get_file_system_client(file_system=adls_file_system)
    
    directory_client = file_system_client.get_directory_client(adls_directory)

    file_client = directory_client.create_file(alds_filename)

    _alds_chunk_transfer(file_client, local_filepath, upload_chunksize)
# ADLS Upload

# SFTP Download
def _sftp_file_download(ssh_conn_id, sftp_filepath, local_filepath):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()

        print(f"Downloading file {sftp_filepath} to {local_filepath}.")
        sftp_client.get(sftp_filepath, local_filepath)

    print(f"Downloaded file {sftp_filepath} to {local_filepath}.")
# SFTP Download

def _sftp_to_adls(file_dict, ssh_conn_id, sftp_directory, storage_account_name, adls_conn_id, adls_file_system, adls_directory, upload_chunksize):

    file_dict_loads = json.loads(file_dict)
    for file_name in file_dict_loads.keys():

        sftp_filepath = os.path.join(sftp_directory, file_name)

        with tempfile.TemporaryDirectory(dir=os.path.expanduser("~")) as temp_download_dir:
            local_filepath = os.path.join(temp_download_dir, file_name)

            _sftp_file_download(ssh_conn_id, sftp_filepath, local_filepath)

            _adls_file_upload(local_filepath, storage_account_name, adls_conn_id, adls_file_system, adls_directory, file_name, upload_chunksize)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['teat@aol.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SFTP_DIRECTORY = "/upload"

with DAG(
    dag_id='SFTP_to_ADLS',
    start_date=datetime(2021, 7, 27, 16, 0, 0),
    schedule_interval='*/3 * * * *',
    catchup=True,
    default_args=default_args
) as dag:

    tsk_sftp_check = PythonOperator(
        task_id="tsk_sftp_check",
        python_callable=_list_sftp_files,  # make sure you don't include the () of the function
        op_kwargs={
            "sftp_conn_id": "af_sftp_conn_id",
            "sftp_directory": SFTP_DIRECTORY,
            "file_regex": r"sftp_.*",
        }
    )

    tsk_files_present = ShortCircuitOperator(
        task_id='tsk_files_present',
        python_callable=file_check,
        op_kwargs = {
            "files": """{{ task_instance.xcom_pull(task_ids="tsk_sftp_check") }}"""
        }
    )

    tsk_upload_file = PythonOperator(
        task_id="tsk_sftp_download_adls_upload_file",
        python_callable=_sftp_to_adls,
        op_kwargs={
            "file_dict": """{{ task_instance.xcom_pull(task_ids="tsk_sftp_check") }}""",
            "ssh_conn_id": "",
            "sftp_directory": SFTP_DIRECTORY,
            "storage_account_name": "example",
            "adls_conn_id": "af_adls_conn_id",
            "adls_file_system": "source",
            "adls_directory": "/RAW_DATA/path",
            "upload_chunksize": 1024
        }
    )

tsk_sftp_check >> tsk_files_present >> tsk_upload_file