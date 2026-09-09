"""Hourly acquisition for the Walloon ensemble discharge forecast (SPW KiWIS QPREV).

Split out of tethys_series_HYDROMETRIE_WALLONIE_dag.py, which keeps the observed products. QPREV led
that DAG's chain -- first in line because a production costs a single request and ages out of the API
archive permanently, so it must never wait behind the observed drivers. The cost of that position was
that the forecast's fragility took the observed products with it: when the SPW endpoint degraded on
2026-08-22, QPREV overran its execution_timeout every hour and Q/H/P/HABS, chained behind it, did not
run for six hours. In its own DAG it still goes first in wall-clock terms without being able to block
anything.

Why the forecast is the fragile one: it has no fetch budget. The observed drivers stop issuing
requests past FETCH_BUDGET and store what arrived, so a degraded source yields a partial month and
exit 0. HYDROMETRIE_WALLONIE_FORECAST_BASE never consults that deadline, so a degraded source instead
costs the full retry ladder -- ~12 min for a discovery listing that read-times-out (DISCOVERY_TIMEOUT
x MAX_RETRIES, though a discovery cache is right there to fall back on) plus up to ~2 min per failed
production until MAX_CONSECUTIVE_FAILURES stops the run: ~25-36 min for a run that fetches nothing.
Until the driver honours the budget, the only bounds are the ones set here:

* no retries -- the hourly cadence *is* the retry. PRODUCTION_LOOKBACK re-fetches the last 6 h every
  run, so a production missed now is picked up next hour at no extra cost, while a retry only doubles
  the time a degraded run holds the pool;
* an execution_timeout sized on the legitimate worst case, not the degraded one, which no timeout can
  rescue. When Airflow does win the race it abandons the log stream and cannot remove the still-
  running container -- the task dies with a "409 ... container is running" that names nothing about
  the real cause. The container itself finishes and exits 0, so `docker logs <id>` on the leftover is
  where the actual failure is written; auto_remove is deliberately left off that path.

Still in the one-slot `tethys_wallonie` pool: one SPW credential with one (unpublished) daily credit
allowance serves every Wallonie product, and the pool is also what stops this DAG merging the same
production-day parquet as the daily full pass, which runs QPREV as well. Offset to :40 so the common
case never queues behind the observed run at :10. The pool must exist on the deployment:

    airflow pools set tethys_wallonie 1 "<description>"

No anonymous twin here, unlike the observed DAGs: no forecast is published without a credential
(REQUIRE_CREDENTIAL), so there is nothing to fall back to.
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
    'retries': 0,  # See the module docstring: the hourly cadence is the retry.
    'retry_delay': timedelta(minutes=2),
}

# Every hour at 40 past, 02:40 included -- the pool, not the schedule, is what keeps this off the
# daily full pass.
schedule_interval = '40 * * * *'

with DAG(
    'tethys_hydrometrie_wallonie_qprev_pipeline',
    default_args=default_args,
    description='Hourly retrieval of the Hydrometrie Wallonie ensemble discharge forecast '
                '(authenticated) via tethys-series container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'hydrometrie wallonie', 'private', 'forecast'],
) as dag:

    # Two days back. Productions already on disk are skipped, so this bounds the run at ~48 requests
    # even after an outage -- MAX_PRODUCTIONS_PER_RUN (class default 500) never binds at this width.
    date_from = (pd.Timestamp.now() - pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    retrieve_qprev = DockerOperator(
        task_id='retrieve_wallonie_qprev',
        image='tethys-series:latest',
        api_version='auto',
        auto_remove='success',
        command=[
            'HYDROMETRIE_WALLONIE_QPREV',
            'update',
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(dict(date_from=date_from, download_from_origin=True)),
            '--fun_args', json.dumps([]),
            # No fail_if_older: the daily full DAG carries that check, so a missing product raises
            # one alarm a day instead of 24.
            '--fun_kwargs', json.dumps({}),
        ],
        mounts=container_mounts,
        environment=container_env,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        do_xcom_push=True,
        mount_tmp_dir=False,
        pool='tethys_wallonie',
        # A healthy run is ~35 s (measured 2026-08); a full two-day catch-up after an outage is ~48
        # productions at ~6 s, so ~5 min. Double that, and a fifth of the hour, so a degraded run
        # cannot sit on the pool the observed products need.
        execution_timeout=timedelta(minutes=12),
    )

if __name__ == "__main__":
    dag.test()
