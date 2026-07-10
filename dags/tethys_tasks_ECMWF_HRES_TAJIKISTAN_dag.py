from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from tethys_common import TETHYS_VARS, build_container_env, get_failure_emails

'''
docker-compose run --rm tethys-tasks ECMWF_HRES_TP_TAJIKISTAN update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin\": \"True\"}"
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
    'email': failure_emails,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

zone = 'TAJIKISTAN'
zone_tags = [zone.lower()]
schedule_interval = '50 8,20 * * *'

with DAG(
    f'tethys_ecmwf_hres_{zone.lower()}_pipeline',
    default_args=default_args,
    description=f'Pipeline to retrieve ECMWF HRES {zone.capitalize()} data via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'ecmwf', 'hres'] + zone_tags,
) as dag:

    date_from = (pd.Timestamp.now() - pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    function_ = 'update'
    class_args = []
    class_kwargs = dict(date_from=date_from, download_from_origin=True)
    fun_args = []
    fun_kwargs = {}

    def make_command(class_name):
        return [
            class_name,
            function_,
            '--class_args', json.dumps(class_args),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps(fun_args),
            '--fun_kwargs', json.dumps(fun_kwargs),
        ]

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
        'pool': 'tethys_tasks_pool',
    }

    t1 = DockerOperator(
        task_id='retrieve_t2m_data',
        command=make_command(f'ECMWF_HRES_T2M_{zone}'),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_tp_data',
        command=make_command(f'ECMWF_HRES_TP_{zone}'),
        **common_docker_args,
    )

    t3 = DockerOperator(
        task_id='retrieve_sd_data',
        command=make_command(f'ECMWF_HRES_SD_{zone}'),
        **common_docker_args,
    )

    t1 >> t2 >> t3

if __name__ == "__main__":
    dag.test()