"""Hourly acquisition of the Rogun damdata REST API (Vakhsh basin, Tajikistan) via tethys-series.

The sub-daily products only. The daily reductions, the manual surveyor readings and the station
catalogue settle at most once a day and live in tethys_series_ROGUN_RESTAPI_slow_dag.py, so the
hourly beat does not pay for them.

Source: a Salini Impregilo Spring Boot service ("REST API for access gauge and weather data in
Vahksh river basin"), HTTP Basic auth, Swagger 2.0 at /v2/api-docs. It replaces the legacy Django
Tethys server at :8080, whose per-series SIRogun_* download drivers pulled from this same API.

**No raw tier.** Unlike tethys_series_SOMMER_ROGUN_dag.py, these products cannot be chained with
``download_from_origin=False``: damdata is date-range addressable and serves its whole history on
demand, so the drivers query it directly from ``store()`` and keep no SOURCE/LOCAL files. Each task
therefore does its own fetching. That is cheap here -- a 7-day window is a handful of requests per
product -- and it means one product's bad day cannot leave another with nothing (hence
``trigger_rule='all_done'`` below, as in tethys_series_PT_MARKET_dag.py).

Tasks run **one at a time on purpose**. damdata is a single modest self-hosted box and each
container already uses MAX_WORKERS=3 internally, so running four containers in parallel would put
up to twelve concurrent requests on it.

Time zone: these products store tz-aware station-local time at a **fixed +05:00** -- not the IANA
zone Asia/Dushanbe, because damdata's archive reaches back to 1930 and IANA reports +6 before
1992-01-19 (+7 in the 1981-1991 DST summers) while the data itself carries no such shift. The
drivers own that entirely (they override ``_utcnow()``), so nothing here needs to know the offset;
``date_from`` below is only a floor. Note the legacy Django driver stored ``damdata - 2 h`` on the
weather-bulletin route, so the *old* Tethys series are 3 h off -- do not reconcile against them.

Config: ``ROGUN_RESTAPI_USER`` and ``ROGUN_RESTAPI_PASSWORD`` must be present in
``env/.env-series``. They are mandatory: unlike REN_MERCADO_API_KEY (published by its source), this
is a real service-account credential and the drivers have no default, so they raise before issuing
any request if it is unset. ``build_container_env`` forwards every non-empty variable from that
file, so there is no allow-list to update as well. ``ROGUN_RESTAPI_URL`` and
``ROGUN_RESTAPI_UTC_OFFSET`` only need setting to override the module defaults.

Copy of the standard tethys_series_*_dag.py template; only the class list, schedule, the fetch
budget and the sequencing differ.
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

# Every hour at 50 past -- the last free slot among the hourly series DAGs (Wallonie :10, Romande
# and MeteoSuisse :20, Sommer Rogun :30, Wallonie QPREV :40).
schedule_interval = '50 * * * *'

# The sub-daily products, in increasing request cost so a budget overrun costs the cheapest ones
# last. Measured 2026-09-08 on a routine window:
#   gauge/bulletins    17 bulletin gauges, 2 rows/day each -- trivial
#   weather/bulletins  13 weather stations, 8 rows/day each
#   gauge/rq30         the live station is UWL_1080 (~15-minute data)
#   weather/lsi        one station (Roghun), 10-minute data -- the heaviest, ~13 MB/year
DRIVERS = [
    'ROGUN_RESTAPI_GAUGE_BULLETINS',
    'ROGUN_RESTAPI_WEATHER_BULLETINS',
    'ROGUN_RESTAPI_GAUGE_RQ30',
    'ROGUN_RESTAPI_WEATHER_LSI',
]

# Each driver bounds its own wall clock and, past it, stops issuing requests and stores what already
# arrived. That is the graceful stop; execution_timeout is the backstop for a hang the budget cannot
# see. Keep the two ordered -- FETCH_BUDGET < execution_timeout -- because whichever fires first
# decides the outcome: when the budget wins the run stores a partial result, exits 0 and self-heals
# next hour; when Airflow wins, the DockerOperator abandons the log stream, fails to remove the
# running container, and the task dies with a "409 ... container is running" that names nothing
# about the real cause. A steady-state run is well under a minute, so this only bites on a degraded
# source. Budgeted per task, and four tasks run in series, so keep the sum inside the hour.
FETCH_BUDGET_MINUTES = 8

with DAG(
    'tethys_rogun_restapi_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve Rogun (Vakhsh basin) gauge and weather data from the Salini '
                'Impregilo damdata REST API via tethys-series container',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'rogun', 'damdata'],
) as dag:

    # A week of self-healing. Widening this is nearly free: a month already finalized (past each
    # driver's LOOKBACK) and present on disk is skipped *before* any request, so only the mutable
    # tail is actually fetched. The hand-entered bulletins are revised for days after the fact,
    # which is why their own LOOKBACK is -10 days. Naive scheduler time is fine -- each driver
    # floors it onto its own station-local grid.
    date_from = (pd.Timestamp.now() - pd.Timedelta('7d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    class_kwargs = dict(date_from=date_from, fetch_budget=f'{FETCH_BUDGET_MINUTES}min')

    def make_command(class_name):
        return [
            class_name,
            'update',
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps([]),
            # No fail_if_older: tethys_series_acquisition_report_dag.py carries that check for
            # these products, so a stalled station raises one alarm a day instead of 24. It is also
            # the better check here -- _check_cutoff only asks whether a stored FILE covers the
            # window (the monthly parquet already exists), while the report scores parquet
            # *contents* per station.
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
        'execution_timeout': timedelta(minutes=20),   # > FETCH_BUDGET_MINUTES; see above
    }

    # One product at a time so damdata only ever sees one client from here, but with
    # trigger_rule='all_done': the products are independent and share no raw tier, so one route
    # having a bad day must not skip everything queued behind it.
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

if __name__ == "__main__":
    dag.test()
