from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from tethys_common import build_container_env, build_mounts, get_failure_emails

container_env = build_container_env("tasks")
container_mounts = build_mounts("tasks")
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

schedule_interval = '50 */3 * * *'  # Every 3 hours, offset by 50 min

with DAG(
    'tethys_icon_ch2_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ICON-CH2-EPS data (TOT_PREC, T2M, SWE) via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'icon', 'icon_ch2', 'switzerland', 'tasks'],
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
        'pool': 'tethys_tasks_pool',  # Limit concurrent tethys-tasks calls
    }

    t1 = DockerOperator(
        task_id='retrieve_tot_prec',
        command=make_command('ICON_CH2_EPS_TOT_PREC'),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_t2m',
        command=make_command('ICON_CH2_EPS_T2M'),
        **common_docker_args,
    )

    t3 = DockerOperator(
        task_id='retrieve_swe',
        command=make_command('ICON_CH2_EPS_SWE'),
        **common_docker_args,
    )

    t1 >> t2 >> t3

if __name__ == "__main__":
    dag.test()
