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

'''
docker-compose run --rm tethys-tasks ALARO40L_T2M retrieve_and_upload --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"
docker-compose run --rm tethys-tasks ALARO40L_TP retrieve_and_upload --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'tethys_alaro_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ALARO data via tethys-tasks container',
    schedule_interval='0 */3 * * *', # Every 3 hours (00:00, 03:00, 06:00, ...)
    # Alternatively, for specific times like 06:00 and 18:00, use: '0 6,18 * * *'
    catchup=False,
    tags=['tethys', 'alaro', 'wallonie', 'vesdre'],
) as dag:

    #region commands
    class_ = 'ALARO40L_T2M'
    function_ = 'retrieve_store_and_upload'
    class_args = []
    class_kwargs = dict(date_from='2026-02-01', download_from_source=True)
    fun_args = []
    fun_kwargs = {}

    alaro_t2m = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]

    class_ = 'ALARO40L_TP'
    alaro_tp = [
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
    }

    # Run the specialized container
    # This assumes retrieve_from_source PRINTS the relative result path to stdout
    t1 = DockerOperator(
        task_id='retrieve_alarot2m_data',
        command=alaro_t2m,
        **common_docker_args
    )

    t2 = DockerOperator(
        task_id='retrieve_alarotp_data',
        command=alaro_tp,
        **common_docker_args
    )

    t1 >> t2

if __name__ == "__main__":
    dag.test()