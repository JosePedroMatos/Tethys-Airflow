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
}

# C3S models: name → (t2m class, tprate class)
C3S_MODELS = {
    'ecmwf51': ('C3S_ECMWF51_T2M_WORLD',  'C3S_ECMWF51_TPRATE_WORLD'),
    'ukmo610': ('C3S_UKMO610_T2M_WORLD',   'C3S_UKMO610_TPRATE_WORLD'),
    'mf9':     ('C3S_MF9_T2M_WORLD',       'C3S_MF9_TPRATE_WORLD'),
    'dwd22':   ('C3S_DWD22_T2M_WORLD',     'C3S_DWD22_TPRATE_WORLD'),
    'cmcc4':   ('C3S_CMCC4_T2M_WORLD',     'C3S_CMCC4_TPRATE_WORLD'),
    'ncep2':   ('C3S_NCEP2_T2M_WORLD',     'C3S_NCEP2_TPRATE_WORLD'),
    'jma3':    ('C3S_JMA3_T2M_WORLD',      'C3S_JMA3_TPRATE_WORLD'),
    'eccc5':   ('C3S_ECCC5_T2M_WORLD',     'C3S_ECCC5_TPRATE_WORLD'),
    'bom2':    ('C3S_BOM2_T2M_WORLD',      'C3S_BOM2_TPRATE_WORLD'),
}

schedule_interval = '0 21 */5 * *'  # Every 5 days at 21:00 UTC

date_from = (pd.Timestamp.now() - pd.Timedelta('60d')).strftime('%Y-%m-%d')

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

# One DAG per model, two independent tasks (t2m + tprate)
for model, (cls_t2m, cls_tprate) in C3S_MODELS.items():
    with DAG(
        f'tethys_c3s_{model}_pipeline',
        default_args=default_args,
        description=f'Pipeline to retrieve C3S {model.upper()} seasonal forecast data via tethys-tasks container',
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
        tags=['tethys', 'c3s', 'seasonal', 'tasks', 'world', model],
    ):
        DockerOperator(
            task_id='retrieve_t2m',
            command=make_command(cls_t2m),
            **common_docker_args,
        )
        DockerOperator(
            task_id='retrieve_tprate',
            command=make_command(cls_tprate),
            **common_docker_args,
        )
