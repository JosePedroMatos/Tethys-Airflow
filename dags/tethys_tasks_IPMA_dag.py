from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from tethys_common import build_container_env, build_mounts, get_failure_emails

'''
docker-compose run --rm tethys-tasks IPMA_TP update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin\": \"True\"}"
'''

container_env = build_container_env("tasks")
container_mounts = build_mounts("tasks")
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

schedule_interval = '0 */3 * * *'  # Every 3 hours

with DAG(
    'tethys_ipma_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve IPMA deterministic forecast data via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'ipma', 'atlantic', 'tasks'],
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
        'mounts': container_mounts,
        'environment': container_env,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'bridge',
        'do_xcom_push': True,
        'mount_tmp_dir': False,
        'pool': 'tethys_tasks_pool',
    }

    t1 = DockerOperator(
        task_id='retrieve_t2m_data',
        command=make_command('IPMA_T2M'),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_tp_data',
        command=make_command('IPMA_TP'),
        **common_docker_args,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.test()
