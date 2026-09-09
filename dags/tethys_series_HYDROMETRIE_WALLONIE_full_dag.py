"""Daily full pass for the Hydrometrie Wallonie (SPW KiWIS) products, before dawn.

The companion of tethys_series_HYDROMETRIE_WALLONIE_dag.py. That one runs hourly with
incremental=True, which resumes each open month from the timestamp already stored; what it cannot
see is history arriving late outside that tail window -- a station returning from an outage with a
backlog, say. This DAG drops incremental and re-fetches each open month over its whole window once a
day, which reconciles exactly that. The hourly schedule skips 02:00 UTC so the two never merge the
same monthly parquet at once.

02:00 UTC is 04:00 CEST / 03:00 CET, comfortably before sunrise over the Walloon gauges all year
(Airflow runs on UTC here -- AIRFLOW__CORE__DEFAULT_TIMEZONE is unset).

This pass is slow by construction -- whole-month windows for every station, where the hourly DAG's
narrow window lets the coverage filter skip every gauge with no recent data. Measured 2026-08 on Q:
54 requests and ~12 min here against 17 requests and ~2 min hourly. HABS is dearer again, sampling
at one minute, so a month needs several request windows per batch instead of one. Expect this to run
well past 03:00, i.e. into the hourly DAG's schedule. That is safe only because every task here takes
a slot in the one-slot `tethys_wallonie` pool, which serializes all Wallonie work across both DAGs;
the hourly tasks simply queue until this finishes. The pool must exist on the deployment:

    airflow pools set tethys_wallonie 1 "<description>"

Two DAGs live here, authenticated and anonymous-fallback, for the reasons documented in the hourly
DAG. Only one of the pair may ever be unpaused: the *_PRIVATE drivers write the same parquet files
as the public ones.
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

schedule_interval = '0 2 * * *'  # Once per day, 02:00 UTC (before dawn in Belgium year-round)

VARIABLES = ('Q', 'H', 'P', 'HABS')  # discharge, level, precipitation, absolute level

# The forecast archive behind the API is only ~3 months deep and productions age out of it
# permanently, so QPREV's own -95d default window is left in place: productions already on disk are
# skipped, and anything missing is refetched newest-first. That also walks backwards through history
# not yet stored, hence the cap -- at ~6 s per production this bounds the task near 12 minutes
# instead of the ~50 the class default of 500 allows, so the archive drains a little each night
# rather than in one sitting behind the observed tasks. Raise it for a one-off manual catch-up.
MAX_PRODUCTIONS_PER_RUN = 120

# The driver stops issuing requests past FETCH_BUDGET and stores what arrived. Its class default is
# 25 minutes, which is sized for the hourly tail and would cut a legitimate full pass short here --
# a whole-month HABS rebuild is dearer than that on its own. Set explicitly, and kept below
# execution_timeout so the graceful stop is what normally ends a bad run rather than Airflow killing
# the container (see the hourly DAG for why that distinction matters).
#
# A truncated pass is not a lost one: whatever arrived is stored, nothing already written is
# dropped, and the stations that were skipped are picked up by the next night's run or the hourly
# tail. The point of the cap is that this DAG must not still be holding the one-slot pool at dawn.
#
# Sized per task, and the tasks are chained -- so the ceiling that matters is 4x this. A healthy
# variable costs 12-15 min (Q measured at 12; H and HABS carry more series, HABS needing several
# windows per batch at its 1-min cadence), so 40 leaves roughly triple the headroom while capping a
# fully degraded night near 2.5 h. Raise it only together with the pool contention that implies.
FETCH_BUDGET_MINUTES = 40

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
    # Above FETCH_BUDGET_MINUTES with room for the upload/Dropbox steps, so the driver's graceful
    # stop is what ends a degraded run. This is only the backstop for a hang the budget cannot see.
    'execution_timeout': timedelta(minutes=55),
}


def make_command(class_name, class_kwargs):
    return [
        class_name,
        'update',
        '--class_args', json.dumps([]),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps([]),
        # Raises when the current window has no file at all -- a total write failure, once a day
        # rather than hourly. Content-level staleness is the acquisition report DAG's job: it
        # validates from the parquet contents, which this existence check cannot do.
        '--fun_kwargs', json.dumps({'fail_if_older': True}),
    ]


def build_dag(dag_id, description, tags, suffix='', forecast=False):
    """One daily full pipeline. See the hourly DAG for what `suffix` and `forecast` select."""
    with DAG(
        dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,  # Only run one instance at a time, skips backlog
        tags=tags,
    ) as dag:

        # Far enough back to always reach the previous month while it is still open: LOOKBACK is
        # -3 days, so a month freezes on the 4th and is then skipped without even being fetched.
        date_from = (pd.Timestamp.now() - pd.Timedelta('5d')).strftime('%Y-%m-%d')
        print(f'Attempting update from {date_from}.')

        tasks = []

        for variable in VARIABLES:
            tasks.append(DockerOperator(
                task_id=f'retrieve_wallonie_{variable.lower()}',
                command=make_command(
                    f'HYDROMETRIE_WALLONIE_{variable}{suffix}',
                    # No incremental: each open month is fetched over its whole window.
                    dict(date_from=date_from, download_from_origin=True,
                         fetch_budget=f'{FETCH_BUDGET_MINUTES}min'),
                ),
                **common_docker_args,
            ))

        if forecast:
            # Last in the chain: its catch-up is the one open-ended task here, so it must not hold
            # up the observed rebuild.
            tasks.append(DockerOperator(
                task_id='retrieve_wallonie_qprev',
                command=make_command(
                    'HYDROMETRIE_WALLONIE_QPREV',
                    dict(download_from_origin=True,
                         max_productions_per_run=MAX_PRODUCTIONS_PER_RUN),
                ),
                **common_docker_args,
            ))

        for upstream, downstream in zip(tasks, tasks[1:]):
            upstream >> downstream

    return dag


dag = build_dag(
    'tethys_hydrometrie_wallonie_full_pipeline',
    'Daily pre-dawn full retrieval of Hydrometrie Wallonie discharge/level/precipitation plus the '
    'ensemble discharge forecast (authenticated) via tethys-series container',
    ['tethys', 'series', 'hydrometrie wallonie', 'private', 'full'],
    suffix='_PRIVATE',
    forecast=True,
)

# Fallback for when the credential is unavailable: same parquet files, anonymous endpoint (reduced
# daily credits), and no forecast. Keep it paused unless the authenticated DAG above is paused.
dag_public = build_dag(
    'tethys_hydrometrie_wallonie_public_full_pipeline',
    'Daily pre-dawn full retrieval of Hydrometrie Wallonie observed data, anonymous fallback -- keep '
    'paused while tethys_hydrometrie_wallonie_full_pipeline runs (both write the same files)',
    ['tethys', 'series', 'hydrometrie wallonie', 'public', 'full'],
)

if __name__ == "__main__":
    dag.test()
