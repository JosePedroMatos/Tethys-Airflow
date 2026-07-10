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
from tethys_common import TETHYS_VARS, build_container_env, get_failure_emails

# def test_email_failure():
#     """Test task to verify email notifications work"""
#     raise Exception("This is a test failure email - email notifications are working!")

'''
docker-compose run --rm tethys-tasks ALARO40L_T2M update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin=True\": \"True\"}"
docker-compose run --rm tethys-tasks ALARO40L_TP update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin=True\": \"True\"}"
'''

# Debug prints to Airflow logs
print("--- TETHYS DEBUG INFO ---")
for var in TETHYS_VARS:
    val = os.environ.get(var)
    print(f"DEBUG: {var} = {val}")
if not os.environ.get('LOCAL_FILE_FOLDER_DOCKER'):
    print("WARNING: LOCAL_FILE_FOLDER_DOCKER is empty. This will cause Docker errors.")
print("-------------------------")

container_env = build_container_env()
failure_emails = get_failure_emails()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['jpgscm@gmail.com'],  # Add your email here
    'email': failure_emails,
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
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'alaro', 'wallonie', 'vesdre'],
) as dag:

    #region commands
    class_ = 'ALARO40L_T2M'
    function_ = 'update'
    class_args = []
    class_kwargs = dict(date_from='2026-02-01', download_from_origin=True)
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
        'pool': 'tethys_tasks_pool',  # Limit concurrent tethys-tasks calls
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

    # # Test email notification (remove after testing)
    # test_email = PythonOperator(
    #     task_id='test_email_failure',
    #     python_callable=test_email_failure,
    #     trigger_rule='all_done'  # Run regardless of previous task status
    # )

    t1 >> t2

if __name__ == "__main__":
    dag.test()