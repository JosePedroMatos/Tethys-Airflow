"""Daily acquisition of the Rogun damdata products that settle once a day, via tethys-series.

The hourly sub-daily feeds live in tethys_series_ROGUN_RESTAPI_dag.py. Four things belong here
instead:

* **The two daily reductions.** ``*_BULLETINS_DAILY`` pick the 08:00-local reading and stamp it at
  the start of that local day, which is exactly what the legacy Django config's
  ``referenceTime = 08:00:00`` meant. That value cannot change more than once a day, and -- unlike
  the Sommer products, which share a raw tier with their hourly sibling and so are free to re-run --
  these have no raw tier and would re-query damdata on every hourly beat for nothing.
* **The surveyor readings.** Manual survey of absolute water level, at most one per day and often
  far less: measured 2026-09-08, UWL last reported 2026-07-20 and DT2_DT1 2026-08-14. The driver's
  FAIL_IF_OLDER is 90 days and its STATION_STALE_PAD 60 days for that reason; an empty run is the
  normal case and exits 0.
* **The station catalogue.** ``get-all-stations`` returns only name and type -- no coordinates --
  so ROGUN_RESTAPI_CATALOG joins the discovered stations onto the committed
  ``rogun_restapi_stations.csv`` and rewrites one small parquet in place. Three requests. It is
  deliberately excluded from the acquisition report (it is a reference, not a feed), so this DAG is
  the only thing keeping it current: a station added or renamed upstream shows up here.
* **The raw archive.** ``snapshot_raw_month`` freezes damdata's own response bodies for a settled
  month as gzipped JSON under ``rogun_restapi/raw/`` and pushes them to Azure (never Dropbox). This
  is cold storage, not a tier: nothing reads it and no acquisition depends on it. It exists because
  the stored parquets cannot be reverted to what was served -- they drop the non-numeric bulletin
  fields (author, comment, createdOn, ...) and apply the hrel cm->m scale and the precipitation
  -12 h stamp before writing -- and damdata is a single self-hosted box whose Django sibling is
  being retired. A month already archived costs **no request**, so on almost every day these tasks
  do nothing; the work lands once a month, around the 15th, for the month before last. Capturing
  per run instead would store the same days ~170 times over, since the hourly DAG re-fetches a
  rolling 7-day window 24 times a day across 4 products.

Scheduled at 05:50 UTC = 10:50 in Dushanbe, comfortably after the 08:00-local bulletin reading
(03:00 UTC) has been entered. A reading entered late is not lost: the bulletin drivers carry a
LOOKBACK of -10 days and the window below is 14, so the next run picks it up.

Config and time-zone notes are the same as tethys_series_ROGUN_RESTAPI_dag.py -- see its header.
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
    'retry_delay': timedelta(minutes=5),
}

# Once a day at 05:50 UTC (10:50 Dushanbe) -- after the 08:00-local bulletin reading, and clear of
# the 05:00 Monday sweep and the hourly DAGs' :50 slot by a whole hour on the other DAGs' terms.
schedule_interval = '50 5 * * *'

DRIVERS = [
    'ROGUN_RESTAPI_GAUGE_BULLETINS_DAILY',
    'ROGUN_RESTAPI_WEATHER_BULLETINS_DAILY',
    'ROGUN_RESTAPI_GAUGE_SURVEYOR',
    'ROGUN_RESTAPI_CATALOG',
]

# One product per distinct damdata route. The archive is keyed by ROUTE, not by product, because a
# route feeds two products with byte-identical bodies (weather/bulletins serves both
# WEATHER_BULLETINS and WEATHER_BULLETINS_DAILY) -- so listing a second product of the same route
# would cost one existence check and no request. This list is therefore for clarity, not
# correctness. gauge/hydromet-auto is absent on purpose: nothing has been published on it since
# 2021-02-09, so its archive is complete after the one-off seed_raw() and a monthly check would only
# ever archive empty months.
RAW_ARCHIVE_DRIVERS = [
    'ROGUN_RESTAPI_GAUGE_BULLETINS',
    'ROGUN_RESTAPI_WEATHER_BULLETINS',
    'ROGUN_RESTAPI_GAUGE_RQ30',
    'ROGUN_RESTAPI_WEATHER_LSI',
    'ROGUN_RESTAPI_GAUGE_SURVEYOR',
]

# Same ordering rule as the hourly DAG: the driver's own budget must fire before Airflow's timeout,
# so an overrun stores a partial result and self-heals tomorrow instead of being killed mid-write.
# Roomier here because the window is wider and nothing is waiting on the hour.
FETCH_BUDGET_MINUTES = 20
# The archive tasks are a no-op on all but ~1 day a month, and that day is ~45 requests across all
# five routes, so they need far less. Partial is safe: an already written file is skipped without a
# request, so an interrupted archive resumes rather than restarting.
RAW_FETCH_BUDGET_MINUTES = 10

with DAG(
    'tethys_rogun_restapi_slow_pipeline',
    default_args=default_args,
    description='Daily Rogun damdata products (daily reductions, surveyor readings, station '
                'catalogue) via tethys-series container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'series', 'rogun', 'damdata', 'slow'],
) as dag:

    # Two weeks, against the bulletins' -10 day LOOKBACK: hand-entered readings are revised for
    # days, and a finalized month present on disk is skipped before any request, so the extra span
    # only does work after an outage. The surveyor driver carries its own -30 day LOOKBACK and
    # reaches further back on its own terms.
    date_from = (pd.Timestamp.now() - pd.Timedelta('14d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    def make_command(class_name, function_name='update'):
        class_kwargs = dict(date_from=date_from, fetch_budget=f'{FETCH_BUDGET_MINUTES}min')
        if class_name == 'ROGUN_RESTAPI_CATALOG':
            # The catalogue is a single parquet replaced in place; it takes no window and its
            # store() ignores date_from anyway. Passing the budget alone keeps make_command uniform.
            class_kwargs = dict(fetch_budget=f'{FETCH_BUDGET_MINUTES}min')
        if function_name == 'snapshot_raw_month':
            # No date_from: the method derives the months itself from RAW_SNAPSHOT_SETTLE and
            # backfills any recent month still missing, so a skipped day needs no catch-up here.
            class_kwargs = dict(fetch_budget=f'{RAW_FETCH_BUDGET_MINUTES}min')
        return [
            class_name,
            function_name,
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps([]),
            # No fail_if_older, for the same reason as the hourly DAG: the acquisition report DAG
            # scores parquet contents per station once a day. It would also be wrong here -- the
            # surveyor feed is legitimately weeks old between surveys, and the catalogue is a
            # reference with no freshness to check.
            '--fun_kwargs', json.dumps({}),
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
        'execution_timeout': timedelta(minutes=30),   # > FETCH_BUDGET_MINUTES; see above
    }

    previous = None
    for driver in DRIVERS:
        task = DockerOperator(
            task_id=f'retrieve_{driver.lower().replace("rogun_restapi_", "")}',
            command=make_command(driver),
            trigger_rule='all_done',
            **common_docker_args,
        )
        if previous is not None:
            previous >> task
        previous = task

    # The archive runs last: acquisition is what the system needs today, this is insurance for
    # later. trigger_rule='all_done' throughout, so a route having a bad day cannot skip it.
    for driver in RAW_ARCHIVE_DRIVERS:
        task = DockerOperator(
            task_id=f'archive_raw_{driver.lower().replace("rogun_restapi_", "")}',
            command=make_command(driver, 'snapshot_raw_month'),
            trigger_rule='all_done',
            **common_docker_args,
        )
        if previous is not None:
            previous >> task
        previous = task

if __name__ == "__main__":
    dag.test()
