"""Operational acquisition for the SAIH Duero products (Spanish side of the Douro).

Three DAGs live in this file because the source publishes on three different cadences and paying
one cadence for all of them would either lose data or waste transfer.

    tethys_saih_duero_snapshot          every 15 min   fetch only, accumulates the raw tier
    tethys_saih_duero_pipeline          hourly         stores Q / H / Halb / Valb, uploads
    tethys_saih_duero_embalses          daily 07:40    weekly reservoir inflow/outflow bulletin

Why the fetch is split from the store
-------------------------------------
SAIH Duero has no archive endpoint for the present: ``/datos-tiempo-real/risr`` returns *one*
instantaneous reading per station, and the stored product is an hourly mean. Sampling density is
therefore set entirely by how often we poll -- at hourly polling an "hourly mean" is a single
instantaneous reading, which during a flood peak is not the same number.

So the snapshot DAG polls every 15 minutes (matching SOURCE_TIMESTEP and the ~10-minute cadence the
stations actually report at) and does nothing but append to the raw tier: one HTTP request for the
whole basin, ~175 kB. The store DAG then aggregates and publishes hourly.

Folding the two together would mean re-writing and re-uploading every open monthly parquet four
times an hour. Measured at full month: ~683 kB for Q, the same for H, ~147 kB each for Halb and
Valb, so ~1.7 MB per store cycle -- 15-minute publishing would cost ~160 MB/day of Azure traffic to
add nothing a reader can use. Hourly is ~40 MB/day for the same data.

Only the first task fetches
---------------------------
One snapshot carries every layer (gauging stations, reservoirs, rain gauges), and each product's
``_fetch_from_source`` writes all of them to the same per-day raw CSV. Running four ``update``s
would therefore fetch the same page four times *and* have them race read-modify-write on that one
file. Instead the snapshot DAG owns the only fetch, and every task in the store DAG runs with
``download_from_origin=False`` -- ``BaseSeries.retrieve`` skips ``_fetch_from_source`` entirely, so
those tasks only read the raw tier and write their own parquet.

The pool is what makes that safe
--------------------------------
Chaining orders tasks within a run but says nothing across DAGs, and here the 15-minute fetch
*writes* the same raw file the hourly store *reads*. Without serialization a store can read a
half-written CSV. Every task below takes a slot in the one-slot ``tethys_saih_duero`` pool, which
must exist on the deployment:

    airflow pools set tethys_saih_duero 1 "Serialize SAIH Duero raw-tier access"

A fetch cycle blocked behind a store chain is simply skipped (``catchup=False``,
``max_active_runs=1``) -- one lost sample out of four in that hour, not an error.

No fetch budget, and why execution_timeout is still safe
--------------------------------------------------------
Unlike the Wallonie driver this one has no FETCH_BUDGET, because there is nothing to budget: one
request per run, not a batched sweep whose cost scales with the station count. The bound is
arithmetic instead. With ``request_timeout=60`` and the driver's ``MAX_RETRIES=3`` (four attempts)
plus exponential backoff, the worst case is 4x60s + ~16s ~= 4.5 min, comfortably inside the
15-minute ``execution_timeout``. Keep that ordering if either number is changed: when Airflow's
timeout wins instead, the DockerOperator abandons the log stream, fails to remove the container, and
the task dies with a "409 ... container is running" that names nothing about the real cause.

Staleness alarms are left to tethys_series_acquisition_report_dag.py (daily, 23:00), where the new
drivers are listed. An hourly ``fail_if_older`` would raise 24 alarms a day for one problem.

Not here: the CEDEX historical backfill
---------------------------------------
``ANUARIO_AFOROS_*`` is a one-off importer of a published annual edition, not an operational feed
(it is excluded from ``BaseSeries._discover_drivers`` for that reason). Its edition lags the present
by several years, so scheduling it would re-download ~50 MB to write the same numbers. Run it by
hand when a new edition appears, e.g.:

    docker compose run --rm tethys-series ANUARIO_AFOROS_Q import_archive \\
        --class_kwargs '{"demarcacion": "DUERO"}'
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

# The four products read from the snapshot. Order is the chain order in the store DAG.
SNAPSHOT_PRODUCTS = (
    ('q', 'SAIH_DUERO_Q'),          # hourly mean discharge, 180 gauging stations
    ('h', 'SAIH_DUERO_H'),          # hourly mean gauge height, same stations
    ('halb', 'SAIH_DUERO_HALB'),    # hourly mean reservoir level, 36 reservoirs
    ('valb', 'SAIH_DUERO_VALB'),    # hourly mean reservoir volume, same reservoirs
)

# The two products read from the weekly /situacion-embalses bulletin.
EMBALSES_PRODUCTS = (
    ('qafl', 'SAIH_DUERO_QAFL'),    # weekly mean inflow, 18 CHD-operated reservoirs
    ('qefl', 'SAIH_DUERO_QEFL'),    # weekly mean total outflow, same reservoirs
)

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
    # Serializes raw-tier access across all three DAGs here -- see the module docstring.
    'pool': 'tethys_saih_duero',
    'execution_timeout': timedelta(minutes=15),
}


def make_command(class_name, class_kwargs, function_='update', fun_kwargs=None):
    return [
        class_name,
        function_,
        '--class_args', json.dumps([]),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps([]),
        '--fun_kwargs', json.dumps(fun_kwargs or {}),
    ]


# --------------------------------------------------------------------------- #
# 1. Snapshot fetch -- every 15 minutes, raw tier only
# --------------------------------------------------------------------------- #
with DAG(
    'tethys_saih_duero_snapshot',
    default_args=default_args,
    description='Every 15 min: append the SAIH Duero real-time snapshot to the raw tier '
                '(no store, no upload) so the hourly means have more than one sample',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'saih duero', 'snapshot'],
) as dag_snapshot:

    # Two days is ample: _fetch_from_source files each reading under the day it was observed and
    # ignores the event index, which only has to be non-empty and cheap to build.
    snapshot_date_from = (pd.Timestamp.now() - pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Snapshot index from {snapshot_date_from}.')

    # Any snapshot product would write the identical raw file; Q is picked as the owner of the
    # single fetch. `retrieve` deliberately stops before store/upload.
    DockerOperator(
        task_id='fetch_saih_duero_snapshot',
        command=make_command(
            'SAIH_DUERO_Q',
            dict(date_from=snapshot_date_from, download_from_origin=True, request_timeout=60),
            function_='retrieve',
        ),
        **common_docker_args,
    )


# --------------------------------------------------------------------------- #
# 2. Store and publish -- hourly
# --------------------------------------------------------------------------- #
with DAG(
    'tethys_saih_duero_pipeline',
    default_args=default_args,
    description='Hourly aggregation and upload of SAIH Duero discharge/level/reservoir products '
                'from the accumulated snapshot raw tier, via tethys-series container',
    schedule_interval='25 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'series', 'saih duero'],
) as dag_pipeline:

    # Wide enough to cover SNAPSHOT_MAX_AGE (35 days): a station that has been quiet for weeks
    # still carries its last reading on the page, and that reading belongs to an earlier month.
    # LOOKBACK (-10 days) keeps this from rewriting settled months once they are stored.
    store_date_from = (pd.Timestamp.now() - pd.Timedelta('40d')).strftime('%Y-%m-%d')
    print(f'Attempting store from {store_date_from}.')

    store_tasks = []
    for slug, class_name in SNAPSHOT_PRODUCTS:
        store_tasks.append(DockerOperator(
            task_id=f'store_saih_duero_{slug}',
            command=make_command(
                class_name,
                # download_from_origin=False is the whole point: the snapshot DAG owns the fetch,
                # so these four only read the raw tier. Four fetches would race on the same file.
                dict(date_from=store_date_from, download_from_origin=False),
            ),
            **common_docker_args,
        ))

    # Chained, not parallel: they write different parquet trees but all read the same raw CSVs, and
    # the one-slot pool would serialize them anyway -- an explicit chain makes the log order match.
    for upstream, downstream in zip(store_tasks, store_tasks[1:]):
        upstream >> downstream


# --------------------------------------------------------------------------- #
# 3. Weekly reservoir bulletin -- daily
# --------------------------------------------------------------------------- #
with DAG(
    'tethys_saih_duero_embalses',
    default_args=default_args,
    description='Daily fetch of the SAIH Duero weekly reservoir bulletin (inflow/outflow) via '
                'tethys-series container',
    schedule_interval='40 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'series', 'saih duero', 'embalses'],
) as dag_embalses:

    # The bulletin carries one value per week, stamped at the start of the week it averages, and is
    # refreshed on the site's own schedule rather than a fixed weekday -- so it is polled daily and
    # re-stored idempotently. 30 days covers a few weeks of bulletins plus the LOOKBACK window.
    embalses_date_from = (pd.Timestamp.now() - pd.Timedelta('30d')).strftime('%Y-%m-%d')
    print(f'Attempting bulletin update from {embalses_date_from}.')

    embalses_tasks = []
    for slug, class_name in EMBALSES_PRODUCTS:
        embalses_tasks.append(DockerOperator(
            task_id=f'retrieve_saih_duero_{slug}',
            command=make_command(
                class_name,
                dict(date_from=embalses_date_from, download_from_origin=True, request_timeout=60),
                # Once a day is the right frequency for a staleness alarm on a weekly product; the
                # hourly DAG deliberately carries no such check.
                fun_kwargs={'fail_if_older': True},
            ),
            **common_docker_args,
        ))

    # These two fetch the same page; chaining keeps them from doing it simultaneously.
    for upstream, downstream in zip(embalses_tasks, embalses_tasks[1:]):
        upstream >> downstream


if __name__ == "__main__":
    dag_pipeline.test()
