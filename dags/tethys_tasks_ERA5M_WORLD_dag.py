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
    'retry_delay': timedelta(minutes=5),
    'priority_weight': -1,
}

# ERA5-Land monthly means: month M is published around the 6th of M+1, so almost every run is a
# no-op. Every 3 days at 13:00 UTC keeps it cheap and clear of the ERA5 zone block (11:50-12:30),
# GFS (10:00-10:50) and C3S (21:00).
schedule_interval = '0 13 */3 * *'

with DAG(
    'tethys_era5m_world_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ERA5 Land monthly means (world) via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'era5', 'era5m', 'monthly', 'world', 'tasks'],
) as dag:

    # DateOffset (not Timedelta) to stay month-aligned. Do not widen this back into the
    # 2022-09-01 -> 2024-02-29 ECMWF migration window: ERA5M raises for tp in that period.
    date_from = (pd.Timestamp.now() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
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

    # ERA5M is world-only (era5m.py does not call create_kml_classes): storage holds one ~123 MB
    # file per year, rebuilt whenever a month lands. Sequential, since SOURCE_PARALLEL_TRANSFERS
    # is 2 and parallel CDS requests only queue against each other.
    t1 = DockerOperator(
        task_id='retrieve_t2m',
        command=make_command('ERA5M_T2M_WORLD'),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_tp',
        command=make_command('ERA5M_TP_WORLD'),
        **common_docker_args,
    )

    t3 = DockerOperator(
        task_id='retrieve_sd',
        command=make_command('ERA5M_SD_WORLD'),
        **common_docker_args,
    )

    t1 >> t2 >> t3

if __name__ == "__main__":
    dag.test()
