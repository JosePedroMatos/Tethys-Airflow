from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from tethys_common import build_container_env, build_mounts, get_failure_emails

# Portuguese electricity market -- operational acquisition.
#
# Three public sources, no credentials (see .env.example in tethys-series if any of them ever needs
# to be pointed at a mirror or slowed down):
#   * REN SIMEE (mercadoservices.ren.pt) -- day-ahead and intraday prices/energies, and the
#     per-unit scheduling programmes;
#   * REN DataHub (datahub.ren.pt)       -- the 15-min generation mix and the daily hydro balance;
#   * OMIE (omie.es)                     -- an independent copy of the day-ahead price, kept as a
#     cross-check, plus the continuous intraday prices REN never published.
#
# The settlement-lagged products (aFRR/mFRR prices and energies, the per-unit band) are NOT here --
# they keep revising for days after the market day and live in tethys_series_PT_MARKET_slow_dag.py
# on a wider window.
#
# ---------------------------------------------------------------------------------------------
# Why the window ends in the FUTURE
# ---------------------------------------------------------------------------------------------
# The day-ahead price is a forward curve: the whole of day D+1 clears and publishes during day D,
# and the intraday auctions and the PDBF/PDVD programmes are forward too. BaseSeries defaults
# date_to to "now", which silently clips that curve at the current instant -- measured on
# 2026-09-02 10:32 UTC, the default window stored the day-ahead only to 10:30 while the source had
# already published out to 21:45. Pushing date_to two days out keeps the forward view, which for
# scheduling a reservoir is the number that actually matters.
#
# There is no cost to overshooting: a market day the source has not published yet answers with an
# empty payload, and nothing is written for it.
# ---------------------------------------------------------------------------------------------

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

# Twice a day. 06:30 closes off the previous day (and picks up any revision); 14:30 is after the
# MIBEL day-ahead session clears (12:00 CET) and publishes, so it is the run that captures
# tomorrow's prices and programmes.
schedule_interval = '30 6,14 * * *'

# Products whose sources publish about a day behind, and which are worth having promptly.
DRIVERS = [
    'REN_MERCADO_SPOT_PRICES',
    'REN_MERCADO_SPOT_ENERGY',
    'OMIE_DAY_AHEAD_PRICE',
    'OMIE_INTRADAY_CONTINUOUS_PRICE',
    'REN_DATAHUB_GENERATION',
    'REN_DATAHUB_HYDRO_BALANCE',
    'REN_MERCADO_PROGRAMME_PDBF',
    'REN_MERCADO_PROGRAMME_PDVD',
    'REN_MERCADO_PROGRAMME_PHF',
]

with DAG(
    'tethys_pt_market_pipeline',
    default_args=default_args,
    description='Portuguese day-ahead / intraday market, generation mix and unit programmes',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,  # Only run one instance at a time, skips backlog
    tags=['tethys', 'series', 'portugal', 'market'],
) as dag:

    # Five days back so a missed run heals itself and late revisions are picked up; two days
    # forward for the reasons in the header.
    now = pd.Timestamp.now()
    date_from = (now - pd.Timedelta('5d')).strftime('%Y-%m-%d')
    date_to = (now + pd.Timedelta('2d')).strftime('%Y-%m-%d')
    print(f'Attempting update from {date_from} to {date_to}.')

    class_kwargs = dict(date_from=date_from, date_to=date_to)

    def make_command(class_name):
        return [
            class_name,
            'update',
            '--class_args', json.dumps([]),
            '--class_kwargs', json.dumps(class_kwargs),
            '--fun_args', json.dumps([]),
            # A coarse alarm only: _check_cutoff tests whether a stored FILE covers the window, not
            # whether it has fresh values in it, so within a month it cannot fire (the monthly
            # parquet already exists). It does catch a product that stops producing once the month
            # rolls over. Real per-column staleness is the acquisition report DAG's job, which
            # scores parquet *contents*.
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
        # The drivers bound their own fetch at FETCH_BUDGET (20 min); keep Airflow's timeout above
        # it so an overrun stores a partial, self-healing result instead of being killed. Airflow
        # winning means the DockerOperator gives up on the log stream, fails to remove the still
        # running container, and reports only "409 ... container is running" -- and the run's work
        # is lost. Same reasoning as tethys_series_HYDROMETRIE_WALLONIE_dag.py.
        'execution_timeout': timedelta(minutes=30),
    }

    # Run one product at a time so the two REN hosts only ever see one client from here, but with
    # trigger_rule='all_done': these products are independent, so one source having a bad day must
    # not skip everything queued behind it.
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
