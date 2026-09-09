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

# Gabon is excluded on purpose: ICON_WORLD_*_GABON exists (create_kml_classes covers every .kml in
# tethys_tasks/resources) but is not scheduled.
# The FIRST zone downloads the global run, the rest only remap and store it -- see the ordering
# note below, the list order is load-bearing.
ZONES = ['IBERIA', 'BELGIUM', 'CAUCASUS', 'SWITZERLAND', 'TAJIKISTAN', 'ZAMBEZI']
VARIABLES = ['TP', 'T2M', 'SD']

# ICON global runs every 12 h (00/12 UTC) and is published ~4 h later, so 04:30/16:30 leaves margin
# before DWD's ~24 h retention on opendata.dwd.de drops the run.
schedule_interval = '30 4,16 * * *'

with DAG(
    'tethys_icon_world_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ICON global (DWD, 13 km) data for all zones via tethys-tasks container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'icon', 'icon_world', 'dwd', 'tasks'] + [zone.lower() for zone in ZONES],
) as dag:

    # DWD only keeps the global gribs for ~24 h, so anything older can never be downloaded again.
    date_from = (pd.Timestamp.now() - pd.Timedelta('1d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    function_ = 'update'
    class_args = []
    fun_args = []
    fun_kwargs = {}

    def make_command(class_name, download_from_origin):
        class_kwargs = dict(date_from=date_from, download_from_origin=download_from_origin)
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

    # One DAG instead of the usual one-DAG-per-zone (ERA5/GFS/ECMWF_HRES): ICON_WORLD local and
    # cloud paths carry no zone, so all six zones read the SAME downloaded global run and only the
    # storage differs. Download-once therefore has to be enforced by task order, not by staggered
    # schedules. Everything is a single chain so only one global download (~113 leadtimes) is ever
    # in flight, as for ICON-CH.
    previous = None
    for variable in VARIABLES:
        for index, zone in enumerate(ZONES):
            download_from_origin = index == 0
            prefix = 'retrieve' if download_from_origin else 'store'

            task = DockerOperator(
                task_id=f'{prefix}_{variable.lower()}_{zone.lower()}',
                command=make_command(f'ICON_WORLD_{variable}_{zone}', download_from_origin),
                **common_docker_args,
            )

            if previous is not None:
                previous >> task
            previous = task

if __name__ == "__main__":
    dag.test()
