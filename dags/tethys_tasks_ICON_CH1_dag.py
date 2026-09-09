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

# ICON-CH1-EPS runs every 3 h (00, 03, ... UTC) and a run is published ~2 h30 after its reference
# time, so each slot fires ~20 min after the corresponding publication: 02:50 for the 00 UTC run,
# 05:50 for the 03 UTC run, and so on (8 runs a day, as in the model).
schedule_interval = '50 2-23/3 * * *'

with DAG(
    'tethys_icon_ch1_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ICON-CH1-EPS data (TOT_PREC, T2M, SWE) via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'icon', 'icon_ch1', 'switzerland', 'tasks'],
) as dag:

    # MeteoSwiss only keeps ICON-CH1 assets for ~24 h, so anything older can never be downloaded
    # again; 1 day back is enough to catch up on a missed run without indexing unreachable dates.
    date_from = (pd.Timestamp.now() - pd.Timedelta('1d')).strftime('%Y-%m-%d')
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

    # Kept sequential (as for ICON-CH2): at ~1.1 km each variable is a few hundred MB per run, so
    # the three variables are never downloaded and regridded at the same time.
    t1 = DockerOperator(
        task_id='retrieve_tot_prec',
        command=make_command('ICON_CH1_EPS_TOT_PREC'),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_t2m',
        command=make_command('ICON_CH1_EPS_T2M'),
        **common_docker_args,
    )

    t3 = DockerOperator(
        task_id='retrieve_swe',
        command=make_command('ICON_CH1_EPS_SWE'),
        **common_docker_args,
    )

    t1 >> t2 >> t3

if __name__ == "__main__":
    dag.test()
