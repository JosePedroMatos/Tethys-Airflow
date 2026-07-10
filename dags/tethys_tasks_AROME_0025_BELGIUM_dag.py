from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from tethys_common import TETHYS_VARS, build_container_env, get_failure_emails

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
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': failure_emails,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

zone = 'BELGIUM'
zone_tags = [zone.lower(), 'wallonie']
schedule_interval = '45 */3 * * *'  # Run every 3 hours, offset by 45 min

with DAG(
    f'tethys_arome_0025_{zone.lower()}_pipeline',
    default_args=default_args,
    description=f'Pipeline to retrieve AROME 0.025° {zone.capitalize()} data (T2M, TP, SWE) via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'arome', 'meteofrance'] + zone_tags,
) as dag:

    date_from = (pd.Timestamp.now() - pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    common_docker_args = {
        'image': 'tethys-tasks:latest',
        'api_version': 'auto',
        'auto_remove': 'success',
        'mounts': [
            Mount(source=os.environ.get('LOCAL_FILE_FOLDER'), target=os.environ.get('LOCAL_FILE_FOLDER_DOCKER'), type='bind'),
            Mount(source=os.environ.get('STORAGE_FILE_FOLDER'), target=os.environ.get('STORAGE_FILE_FOLDER_DOCKER'), type='bind'),
        ],
        'environment': container_env,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'bridge',
        'do_xcom_push': True,
        'mount_tmp_dir': False,
        'pool': 'tethys_tasks_pool',  # Limit concurrent tethys-tasks calls
    }

    def make_command(class_: str) -> list:
        class_kwargs = dict(date_from=date_from, download_from_origin=True)
        return [
            class_,
            'update',
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps([]),
            '--fun_kwargs', json.dumps({}),
        ]

    retrieve_t2m = DockerOperator(
        task_id='retrieve_t2m',
        command=make_command(f'AROME_0025_T2M_{zone}'),
        **common_docker_args,
    )

    retrieve_tp = DockerOperator(
        task_id='retrieve_tp',
        command=make_command(f'AROME_0025_TP_{zone}'),
        **common_docker_args,
    )

    retrieve_swe = DockerOperator(
        task_id='retrieve_swe',
        command=make_command(f'AROME_0025_SWE_{zone}'),
        **common_docker_args,
    )

    retrieve_t2m >> retrieve_tp >> retrieve_swe

if __name__ == "__main__":
    dag.test()