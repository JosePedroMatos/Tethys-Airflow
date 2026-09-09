"""Hourly acquisition of the Roghun HPP telemetry (Sommer MDS portal) via tethys-series.

Two products, one raw tier. ``SOMMER_ROGUN_HOURLY`` and ``SOMMER_ROGUN_DAILY`` are sibling
subclasses that share the same SOURCE/LOCAL day-zips and differ only in their storage step, so --
exactly as in tethys_series_METEOSUISSE_dag.py -- the tasks are chained and only the first fetches
from origin. The second rebuilds its own parquet from the raw tier the first just wrote and issues
**zero** portal requests. It is also why they must not run in parallel: both would write the same
day-zip.

Both products are stored in **station local time** (Asia/Dushanbe, UTC+5, no DST), unconverted,
because the portal serves station local time; the daily stamp is a local calendar-day label. The
driver overrides its own ``_utcnow()`` accordingly, so nothing here needs to know about the offset --
``date_from`` below is only a floor.

Cadence: the stations report every 10 minutes with a ~5 minute lag, and the driver withholds any
step it has not seen enough samples for, so the newest stored hour is the previous one. The daily
product is refreshed on the same hourly beat rather than once a day: its partial day appears in the
late local afternoon, settles at local midnight (19:00 UTC), and re-storing it costs nothing because
the task never touches the portal.

Cost: the endpoint charges per value, not per series (measured 2026-08: 30 days = 183 kB in 0.9 s),
and a steady-state run only re-downloads the last two local days -- 4 requests, one per active
station, whatever ``date_from`` says. A wider window is therefore almost free and only does work
after an outage, which is why the 7-day window below is generous rather than minimal. The portal only
serves roughly the last 12 months, so nothing deeper is worth asking for.

Config: the four ``ROGUN_SOMMER_LINK`` / ``ROGUN_SOMMER_USERNAME`` / ``ROGUN_SOMMER_PASSWORD`` /
``ROGUN_TZ`` keys must be present in ``env/.env-series``. ``build_container_env`` forwards every
non-empty variable from that file, so there is no allow-list to update as well (the ``TETHYS_VARS``
list some older notes mention no longer exists).

Copy of the standard tethys_series_*_dag.py template; only the class names, schedule, the fetch
budget and the task chaining differ.
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

# Every hour at 30 past -- clear of the other hourly series DAGs (Wallonie :10, Romande and
# MeteoSuisse :20, Wallonie QPREV :40).
schedule_interval = '30 * * * *'

# The driver bounds its own wall clock and, past it, stops issuing requests and stores what already
# arrived. That is the graceful stop; execution_timeout is the backstop for a hang the budget cannot
# see. Keep the two ordered -- FETCH_BUDGET < execution_timeout -- because whichever fires first
# decides the outcome: when the budget wins the run stores a partial result, exits 0 and self-heals
# next hour; when Airflow wins, the DockerOperator abandons the log stream, fails to remove the
# running container, and the task dies with a "409 ... container is running" that names nothing about
# the real cause. A steady-state run is ~10 s, so this only bites on a degraded source.
FETCH_BUDGET_MINUTES = 10

with DAG(
    'tethys_sommer_rogun_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve Roghun HPP water level/discharge from the Sommer MDS portal '
                'via tethys-series container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'sommer rogun'],
) as dag:

    # A week of self-healing: only days actually missing from the raw tier are re-downloaded, so
    # widening this costs nothing while the tier is complete. Naive scheduler time is fine -- the
    # driver floors it onto its own (station-local) grid.
    date_from = (pd.Timestamp.now() - pd.Timedelta('7d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    function_ = 'update'
    fun_args = []
    # No fail_if_older here: tethys_series_acquisition_report_dag.py carries that check for both
    # products, so a stalled gauge raises one alarm a day instead of 24.
    fun_kwargs = {}

    def make_command(class_name, download_from_origin):
        class_args = []
        class_kwargs = dict(date_from=date_from, download_from_origin=download_from_origin,
                            fetch_budget=f'{FETCH_BUDGET_MINUTES}min')
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
        'execution_timeout': timedelta(minutes=20),   # > FETCH_BUDGET_MINUTES; see above
    }

    # Only the hourly task hits the portal. Both drivers share the same raw (SOURCE/LOCAL) tier,
    # keyed by local calendar day, so the daily task reuses whatever the hourly one fetched --
    # download_from_origin=False makes that explicit and keeps the two from writing the same
    # day-zip. It still produces its own parquet if the hourly task failed, from the raw already
    # on disk.
    t1 = DockerOperator(
        task_id='retrieve_sommer_rogun_hourly',
        command=make_command('SOMMER_ROGUN_HOURLY', download_from_origin=True),
        **common_docker_args,
    )

    t2 = DockerOperator(
        task_id='retrieve_sommer_rogun_daily',
        command=make_command('SOMMER_ROGUN_DAILY', download_from_origin=False),
        **common_docker_args,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.test()
