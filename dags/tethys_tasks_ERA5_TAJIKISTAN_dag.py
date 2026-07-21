from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from pathlib import Path
import logging
from tethys_common import build_container_env, build_mounts, get_failure_emails

'''
docker-compose run --rm tethys-tasks ERA5_TP_BELGIUM update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin=True\": \"True\"}"
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
    'priority_weight': -1,
}

zone = 'TAJIKISTAN'
zone_tags = [zone.lower()]
schedule_interval = '50 11 * * *'    # minute hour day month weekday

with DAG(
    f'tethys_era5_{zone.lower()}_pipeline',
    default_args=default_args,
    description=f'Pipeline to retrieve ERA5 Land {zone.capitalize()} data via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'era5', 'tasks'] + zone_tags,
) as dag:

    #region commands

    date_from = (pd.Timestamp.now()-pd.Timedelta('90d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    class_ = 'ERA5W_TP_' + zone
    function_ = 'update'
    class_args = []
    class_kwargs = dict(date_from=date_from, download_from_origin=True)
    fun_args = []
    fun_kwargs = {}

    tp = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]

    class_ = 'ERA5W_T2M_' + zone
    t2m = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]

    class_ = 'ERA5W_SD_' + zone
    sd = [
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
        'mounts': container_mounts,
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

    t3 = DockerOperator(
        task_id='retrieve_sd_data',
        command=sd,
        **common_docker_args
    )

    t1 >> t3 >> t2

if __name__ == "__main__":
    dag.test()