from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from pathlib import Path
import logging

# def test_email_failure():
#     """Test task to verify email notifications work"""
#     raise Exception("This is a test failure email - email notifications are working!")

'''
docker-compose run --rm tethys-tasks GFS_025_TMP_CAUCASUS retrieve_store_upload_and_cleanup --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"
docker-compose run --rm tethys-tasks GFS_025_PRATE_CAUCASUS retrieve_store_upload_and_cleanup --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"
'''

# List of variables to pass to the container
TETHYS_VARS = [
    'AZURE_STORAGE_CONNECTION_STRING',
    'CLOUD_STORAGE_FOLDER',
    'CLOUD_PARALLEL_TRANSFERS',
    'LOCAL_FILE_FOLDER',
    'STORAGE_FILE_FOLDER',
    'LOCAL_FILE_FOLDER_DOCKER',
    'STORAGE_FILE_FOLDER_DOCKER',
    'CDSAPI_URL',
    'CDSAPI_KEY'
]

# Debug prints to Airflow logs
print("--- TETHYS DEBUG INFO ---")
for var in TETHYS_VARS:
    val = os.environ.get(var)
    print(f"DEBUG: {var} = {val}")
if not os.environ.get('LOCAL_FILE_FOLDER_DOCKER'):
    print("WARNING: LOCAL_FILE_FOLDER_DOCKER is empty. This will cause Docker errors.")
print("-------------------------")

# Building the container environment dictionary
container_env = {v: os.environ.get(v) for v in TETHYS_VARS if os.environ.get(v)}
container_env.update({
    'LOCAL_FILE_FOLDER': os.environ.get('LOCAL_FILE_FOLDER_DOCKER'),
    'STORAGE_FILE_FOLDER': os.environ.get('STORAGE_FILE_FOLDER_DOCKER'),
    'TMPDIR': os.environ.get('STORAGE_FILE_FOLDER_DOCKER'), # Fix for "Invalid cross-device link"
})

failure_emails = [
    email.strip()
    for email in os.environ.get('FAILURE_EMAILS', '').split(',')
    if email.strip()
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': failure_emails,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'tethys_gfs_caucasus_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve GFS Caucasus data via tethys-tasks container',
    schedule_interval='0 10 * * *',
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'gfs', 'caucasus', 'engurhesi', 'gse'],
) as dag:

    date_from = (pd.Timestamp.now()-pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    #region commands
    class_ = 'GFS_025_TMP_CAUCASUS'
    function_ = 'retrieve_store_upload_and_cleanup'
    class_args = []
    class_kwargs = dict(date_from=date_from, download_from_source=True)
    fun_args = []
    fun_kwargs = {}

    t2m = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]

    class_ = 'GFS_025_PRATE_CAUCASUS'
    tp = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]
    #endregion

    common_docker_args = {
        'image': 'tethys-tasks:latest',
        'api_version': 'auto',
        'auto_remove': 'success',
        'mounts': [
            Mount(source=os.environ.get('LOCAL_FILE_FOLDER'), target=os.environ.get('LOCAL_FILE_FOLDER_DOCKER'), type='bind'),
            Mount(source=os.environ.get('STORAGE_FILE_FOLDER'), target=os.environ.get('STORAGE_FILE_FOLDER_DOCKER'), type='bind')
        ],
        'environment': container_env,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'bridge',
        'do_xcom_push': True,
        'mount_tmp_dir': False,
        'pool': 'tethys_tasks_pool',  # Limit concurrent tethys-tasks calls
    }

    # Run the specialized container
    # This assumes retrieve_from_source PRINTS the relative result path to stdout
    t1 = DockerOperator(
        task_id='retrieve_t2m_data',
        command=t2m,
        **common_docker_args
    )

    t2 = DockerOperator(
        task_id='retrieve_tp_data',
        command=tp,
        **common_docker_args
    )

    # # Test email notification (remove after testing)
    # test_email = PythonOperator(
    #     task_id='test_email_failure',
    #     python_callable=test_email_failure,
    #     trigger_rule='all_done'  # Run regardless of previous task status
    # )

    t1 >> t2

if __name__ == "__main__":
    dag.test()