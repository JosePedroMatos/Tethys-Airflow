from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from docker.types import Mount
from tethys_common import build_container_env, build_mounts, get_failure_emails, load_component_config

series_config = load_component_config("series")
container_env = build_container_env("series")
container_mounts = build_mounts("series")

# The ROMANDE_ENERGIE driver authenticates to the SFTP server with a private key. In the
# tethys-series compose file this key is a Docker secret at /run/secrets/re_key; DockerOperator
# has no secrets support, so replicate it with a read-only bind mount from the host key
# (RE_KEY_HOST) to the same in-container path the driver reads (RE_KEY=/run/secrets/re_key).
container_mounts = container_mounts + [
    Mount(
        source=series_config['RE_KEY_HOST'],
        target=series_config['RE_KEY'],
        type='bind',
        read_only=True,
    ),
]

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

schedule_interval = '20 * * * *'  # Every hour, at 20 min past the hour

with DAG(
    'tethys_romande_energie_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve Romande Energie GS_Orbe data via tethys-series container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'romande energie'],
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
        'image': 'tethys-series:latest',
        'api_version': 'auto',
        'auto_remove': 'success',
        'mounts': container_mounts,
        'environment': container_env,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'bridge',
        'do_xcom_push': True,
        'mount_tmp_dir': False,
    }

    t1 = DockerOperator(
        task_id='retrieve_romande_energie',
        command=make_command('ROMANDE_ENERGIE'),
        **common_docker_args,
    )

    t1

if __name__ == "__main__":
    dag.test()
