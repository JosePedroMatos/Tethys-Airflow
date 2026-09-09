"""Hourly tail acquisition for the Hydrometrie Wallonie (SPW KiWIS) *observed* products.

The ensemble discharge forecast used to lead the chain here. It now has its own DAG in
tethys_series_HYDROMETRIE_WALLONIE_QPREV_dag.py, which documents why: it is the one Wallonie product
with no fetch budget, so a degraded source overruns any execution_timeout, and as chain head it took
the observed products down with it. Nothing here waits on it any more -- the shared pool below is all
the two still have in common.

Two DAGs live here: the authenticated pipeline (the one to run) and an anonymous public fallback
that stays paused. The *_PRIVATE drivers are drop-in twins of the public ones -- same VARIABLE, same
STORAGE_TEMPLATE, so they write the very same parquet files -- which is exactly why only one of the
two may ever be unpaused. Swapping between them is a pause/unpause, not a code change.

Cadence: this DAG fetches only the tail (incremental=True), resuming each open month from the
timestamp already stored instead of re-downloading the whole month to add one hour. What incremental
cannot see is history arriving late outside that tail window, so it is paired with
tethys_series_HYDROMETRIE_WALLONIE_full_dag.py, which repeats the pass in full once a day at 02:00
UTC. Hour 02 is left out of the schedule below precisely so the two never merge the same monthly
parquet at once.

The tasks are chained rather than parallel: one SPW credential with one (unpublished) daily credit
allowance serves every product here, and the usage guide asks clients not to overload the service.

Chaining only orders tasks *within* a run, so every task also takes a slot in the one-slot
`tethys_wallonie` pool. That is what actually guarantees no two Wallonie containers ever merge the
same monthly parquet at once: the daily full pass runs well past 03:00 and would otherwise collide
with the hourly run. The pool must exist on the deployment -- recreate it with:

    airflow pools set tethys_wallonie 1 "<description>"

Copy of the standard tethys_series_*_dag.py template; only the class names, schedule, the pool, the
private/public factory and the task chaining differ.
"""

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from tethys_common import build_container_env, build_mounts, get_failure_emails

container_env = build_container_env("series")
container_mounts = build_mounts("series")

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

# Every hour at 10 past, except 02:00 -- that hour belongs to the full pass. The pool below is what
# makes the separation safe; this gap just keeps the common case out of the queue.
schedule_interval = '10 0-1,3-23 * * *'

VARIABLES = ('Q', 'H', 'P', 'HABS')  # discharge, level, precipitation, absolute level

# The driver bounds its own wall clock (FETCH_BUDGET) and stops issuing requests past it, storing
# what already arrived. That is the graceful stop; execution_timeout is the backstop for a hang the
# budget cannot see (a socket that never returns, a wedged container). Keep the two ordered:
#
#     FETCH_BUDGET  <  execution_timeout
#
# because whichever fires first decides the outcome. When the budget wins, the run stores a partial
# month, exits 0 and self-heals next hour. When Airflow wins, the DockerOperator abandons the log
# stream and fails to remove the running container -- the task then dies with a "409 ... container is
# running" that names nothing about the real cause, and the run's work is lost.
#
# An incremental pass costs ~2 min per variable (measured 2026-08: 17 requests for Q), so 10 minutes
# is already several times the expected cost and only bites when the source is degraded.
FETCH_BUDGET_MINUTES = 10

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
    'pool': 'tethys_wallonie',
    # Comfortably above FETCH_BUDGET_MINUTES so the driver's own graceful stop is what normally
    # ends a bad run, with room for the upload/Dropbox steps that follow the fetch. Tight for an
    # hourly DAG on purpose: every task holds the one-slot pool, so a task left hanging for the old
    # 45 minutes stalled every other Wallonie product behind it.
    'execution_timeout': timedelta(minutes=20),
}


def make_command(class_name, class_kwargs):
    return [
        class_name,
        'update',
        '--class_args', json.dumps([]),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps([]),
        # No fail_if_older here: the daily DAG carries that check, so a missing product raises one
        # alarm a day instead of 23.
        '--fun_kwargs', json.dumps({}),
    ]


def build_dag(dag_id, description, tags, suffix=''):
    """One hourly pipeline.

    `suffix` selects the driver family -- '_PRIVATE' for the authenticated twins, '' for the
    anonymous ones.
    """
    with DAG(
        dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,  # Only run one instance at a time, skips backlog
        tags=tags,
    ) as dag:

        date_from = (pd.Timestamp.now() - pd.Timedelta('2d')).strftime('%Y-%m-%d')
        print(f'Attempting update from {date_from}.')

        tasks = []

        for variable in VARIABLES:
            tasks.append(DockerOperator(
                task_id=f'retrieve_wallonie_{variable.lower()}',
                command=make_command(
                    f'HYDROMETRIE_WALLONIE_{variable}{suffix}',
                    dict(date_from=date_from, incremental=True, download_from_origin=True,
                         fetch_budget=f'{FETCH_BUDGET_MINUTES}min'),
                ),
                **common_docker_args,
            ))

        for upstream, downstream in zip(tasks, tasks[1:]):
            upstream >> downstream

    return dag


dag = build_dag(
    'tethys_hydrometrie_wallonie_pipeline',
    'Hourly incremental retrieval of Hydrometrie Wallonie discharge/level/precipitation '
    '(authenticated) via tethys-series container',
    ['tethys', 'series', 'hydrometrie wallonie', 'private'],
    suffix='_PRIVATE',
)

# Fallback for when the credential is unavailable: same parquet files, anonymous endpoint (reduced
# daily credits). Keep it paused unless the authenticated DAG above is paused.
dag_public = build_dag(
    'tethys_hydrometrie_wallonie_public_pipeline',
    'Hourly incremental retrieval of Hydrometrie Wallonie observed data, anonymous fallback -- keep '
    'paused while tethys_hydrometrie_wallonie_pipeline runs (both write the same files)',
    ['tethys', 'series', 'hydrometrie wallonie', 'public'],
)

if __name__ == "__main__":
    dag.test()
