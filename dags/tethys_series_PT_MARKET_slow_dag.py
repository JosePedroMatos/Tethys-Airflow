from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from tethys_common import build_container_env, build_mounts, get_failure_emails

# Portuguese system services -- the products that settle late.
#
# Split from tethys_series_PT_MARKET_dag.py because these revise for days after the market day, so
# a one-day window would keep storing provisional numbers and then never revisit them (a month is
# frozen once it passes its LOOKBACK). Measured publication lags on 2026-08-31:
#   * aFRR / mFRR prices, energies and requirements   ~1-2 days, but settlement keeps revising;
#   * REN_MERCADO_AFRR_BAND_UNITS (band per physical unit)  ~7 days (latest-date read 2026-08-24).
# The drivers' LOOKBACK values match those lags (-10d, and -14d for the per-unit band), and this
# DAG's window is set wide enough to actually re-fetch inside them.
#
# Two archive-only products are deliberately absent: REN stopped publishing RR prices after
# 2025-12-30 and continuous-intraday energies after 2025-06-05. REN_MERCADO_RR_PRICE and
# REN_MERCADO_INTRADAY_CONTINUOUS_ENERGY still exist for their history and can be run by hand with
# an explicit date_from; scheduling them would only produce empty runs.

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

schedule_interval = '0 5 * * 1'  # Weekly, Monday 05:00 UTC

DRIVERS = [
    'REN_MERCADO_AFRR_PRICE',
    'REN_MERCADO_AFRR_ENERGY',
    'REN_MERCADO_AFRR_BAND_PRICE',
    'REN_MERCADO_AFRR_CAPACITY',
    'REN_MERCADO_MFRR_PRICE',
    'REN_MERCADO_MFRR_ENERGY',
    'REN_MERCADO_MFRR_NEEDS',
    'REN_MERCADO_RESERVE_ACTIVATED_ENERGY',
    'REN_MERCADO_RESERVE_ACTIVATED_PRICE',
    'REN_MERCADO_AFRR_BAND_UNITS',
    'REN_MERCADO_UNIT_CATALOG',
]

with DAG(
    'tethys_pt_market_slow_pipeline',
    default_args=default_args,
    description='Portuguese system services (aFRR/mFRR) and per-unit band via tethys-series',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'series', 'portugal', 'ancillary'],
) as dag:

    # Wide enough to re-fetch inside the drivers' own LOOKBACK windows (-10d, -14d).
    date_from = (pd.Timestamp.now() - pd.Timedelta('20d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from}.')

    class_kwargs = dict(date_from=date_from)

    def make_command(class_name):
        return [
            class_name,
            'update',
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps([]),
            '--fun_kwargs', json.dumps({'fail_if_older': True}),
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
        # A 20-day window is ~20 requests per product; keep Airflow above the drivers' 20-min
        # FETCH_BUDGET so an overrun stores a partial result instead of being killed.
        'execution_timeout': timedelta(minutes=40),
    }

    # Sequential so REN only ever sees one client from here, but trigger_rule='all_done': these
    # products are independent, so one failing must not skip everything queued behind it.
    previous = None
    for driver in DRIVERS:
        task = DockerOperator(
            task_id=f'retrieve_{driver.lower()}',
            command=make_command(driver),
            trigger_rule='all_done',
            **common_docker_args,
        )
        if previous is not None:
            previous >> task
        previous = task


if __name__ == "__main__":
    dag.test()
