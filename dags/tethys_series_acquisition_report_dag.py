"""Daily acquisition diagnostic for the tethys-series drivers.

One DockerOperator run calls ``BaseSeries.acquisition_report`` inside a single tethys-series container
(a DockerOperator runs exactly one CLI invocation, so the aggregation loops the drivers *inside* that
call rather than launching one container per driver). It reports, per driver, the last successful
acquisition date and a storage-only, content-validated success rate. A downstream Python task pulls the
report from XCom and emails a summary table once per day.

Copy of the standard tethys_series_*_dag.py template; only the class/function, aggregation and the
email step differ.
"""

import ast
import html
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.email import send_email

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

schedule_interval = '0 23 * * *'  # Once per day, 23:00

# Empty list -> the container auto-discovers every concrete BaseSeries driver (abstract bases and
# one-off/backfill variants excluded). Pass an explicit list here to restrict the report.
DRIVERS: list = ['ZRA_RIVER_FLOWS',
           'ZRA_RESERVOIR',
           'HCB_BULLETINS',
           'ENGURI_RESERVOIR',
           'NUREK_RESERVOIR',
           'ROMANDE_ENERGIE',
           'METEOSUISSE_OBSERVED',
           'METEOSUISSE_FORECAST',
           ]


# Inline styles: many mail clients strip <style> blocks, so every cell/row carries its own.
_ACQ_TH = 'padding:6px 10px;border:1px solid #ccc;background:#eef2f7;text-align:left'
_ACQ_TD = 'padding:6px 10px;border:1px solid #ccc'
_ACQ_TD_NUM = _ACQ_TD + ';text-align:right'


def _location_severity(item):
    """Sort key for a (label, detail) per_location entry, worst-first: missing success_rate
    sorts ahead of any number, then ascending success_rate; ties break by oldest last_acquisition."""
    detail = item[1] if isinstance(item[1], dict) else {}
    rate = detail.get('success_rate')
    rate_key = (1, rate) if isinstance(rate, (int, float)) else (0, 0.0)
    last = detail.get('last_acquisition')
    try:
        last_key = pd.Timestamp(last).value if last else -1
    except (TypeError, ValueError):
        last_key = -1
    return rate_key + (last_key,)


def _acquisition_report_table(report: dict, *, title: str = None, show_locations: bool = True) -> str:
    """Render a combined acquisition_report dict as a self-contained HTML table.

    Kept local (rather than imported from tethys_series.BaseSeries.acquisition_report_table)
    because that package only lives inside the tethys-series container image and is not
    installed in the Airflow environment -- see tethys_series/base.py for the source this
    mirrors.
    """
    rows = []
    for i, (name, result) in enumerate(sorted(report.items())):
        bg = '#ffffff' if i % 2 == 0 else '#fafafa'
        name_h = html.escape(str(name))
        if not isinstance(result, dict) or 'error' in result:
            msg = result.get('error') if isinstance(result, dict) else result
            rows.append(
                f'<tr style="background:{bg}"><td style="{_ACQ_TD}">{name_h}</td>'
                f'<td style="{_ACQ_TD};color:#b00020" colspan="5">ERROR: {html.escape(str(msg))}</td></tr>'
            )
            continue

        rate = result.get('success_rate')
        healthy = isinstance(rate, (int, float)) and rate >= 1.0
        rate_s = f'{rate:.0%}' if isinstance(rate, (int, float)) else '-'
        start = html.escape(str(result.get('last_acquisition_start') or '-'))
        end = html.escape(str(result.get('last_acquisition') or '-'))
        rows.append(
            f'<tr style="background:{bg}">'
            f'<td style="{_ACQ_TD}">{name_h}</td>'
            f'<td style="{_ACQ_TD}">{start}</td>'
            f'<td style="{_ACQ_TD}">{end}</td>'
            f'<td style="{_ACQ_TD_NUM}{"" if healthy else ";color:#b00020"}">{rate_s}</td>'
            f'<td style="{_ACQ_TD_NUM}">{result.get("hit_cells", 0)}/{result.get("total_cells", 0)}</td>'
            f'<td style="{_ACQ_TD_NUM}">{result.get("n_locations", 0)}</td></tr>'
        )

        if show_locations and not healthy:
            inner_rows = []
            ordered_locations = sorted((result.get('per_location') or {}).items(), key=_location_severity)
            i2, n = 0, len(ordered_locations)
            while i2 < n:
                label, detail = ordered_locations[i2]
                d_rate = detail.get('success_rate') if isinstance(detail, dict) else None
                d_last_raw = (detail or {}).get('last_acquisition')
                labels = [label]
                j = i2 + 1
                while j < n:
                    next_label, next_detail = ordered_locations[j]
                    next_detail = next_detail or {}
                    if next_detail.get('success_rate') != d_rate or next_detail.get('last_acquisition') != d_last_raw:
                        break
                    labels.append(next_label)
                    j += 1
                d_rate_s = f'{d_rate:.0%}' if isinstance(d_rate, (int, float)) else 'no data'
                d_last = html.escape(str(d_last_raw or '-'))
                shown, extra = labels[:8], len(labels) - 8
                labels_s = ', '.join(html.escape(str(l)) for l in shown)
                if extra > 0:
                    labels_s += f' (+{extra} more)'
                inner_rows.append(
                    f'<tr><td style="padding:1px 8px 1px 0;white-space:nowrap">{d_last}<br>{d_rate_s}</td>'
                    f'<td style="padding:1px 0">{labels_s}</td></tr>'
                )
                i2 = j
            omitted = result.get('per_location_omitted')
            if omitted:
                inner_rows.append(
                    f'<tr><td colspan="2" style="padding:1px 0;color:#999">'
                    f'&hellip; +{omitted} more location(s) not shown</td></tr>'
                )
            if inner_rows:
                rows.append(
                    f'<tr style="background:{bg}"><td style="{_ACQ_TD}"></td>'
                    f'<td style="{_ACQ_TD};color:#555;font-size:12px" colspan="5">'
                    f'<table cellpadding="0" cellspacing="0" style="border-collapse:collapse">'
                    f'{"".join(inner_rows)}</table></td></tr>'
                )

    heading = f'<h3 style="margin-bottom:2px">{html.escape(title)}</h3>' if title else ''
    return (
        heading +
        '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-family:sans-serif;font-size:13px">'
        f'<tr><th style="{_ACQ_TH}">Driver</th><th style="{_ACQ_TH}">Last acquisition (start)</th>'
        f'<th style="{_ACQ_TH}">Last acquisition (end)</th>'
        f'<th style="{_ACQ_TH};text-align:right">Success</th><th style="{_ACQ_TH};text-align:right">Valid cells</th>'
        f'<th style="{_ACQ_TH};text-align:right">Locations</th></tr>'
        + ''.join(rows) +
        '</table>'
    )


def _parse_report(raw: str) -> dict:
    """Turn the DockerOperator XCom (the container's last stdout line) into a dict.

    main.py prints ``Result: <dict>`` as the last line, so DockerOperator pushes that line to XCom.
    We scan the captured text from the end for the ``Result:`` marker (so a stray trailing log line
    can't defeat parsing) and fall back to the whole blob. json is tried first in case the value is
    already clean JSON; ast.literal_eval then tolerates the Python repr (single quotes / None) that
    str(dict) produces. Returns {} if nothing dict-shaped can be recovered -- email_report turns that
    into a visible diagnostic rather than a silent empty table.
    """
    text = (raw or '').strip()
    if not text:
        return {}
    marker = 'Result:'
    candidates = [line.split(marker, 1)[1].strip()
                  for line in reversed(text.splitlines()) if marker in line]
    candidates.append(text)  # last resort: parse the whole captured blob
    for candidate in candidates:
        for parse in (json.loads, ast.literal_eval):
            try:
                value = parse(candidate)
            except Exception:
                continue
            if isinstance(value, dict):
                return value
    return {}


def email_report(**context):
    raw = context['ti'].xcom_pull(task_ids='acquisition_report')
    report = _parse_report(raw)
    run_date = context.get('ds') or ''

    if not failure_emails:
        # Nothing to send to; make the no-op explicit in the task log rather than failing silently.
        print('FAILURE_EMAILS is empty -- no recipients configured; skipping acquisition report email.')
        return report

    if report:
        # title is escaped once, inside _acquisition_report_table -- pass the raw date through.
        body = _acquisition_report_table(report, title=f'tethys-series acquisition report — {run_date}')
    else:
        # No driver results parsed: surface the raw container output so the failure is actionable,
        # instead of mailing an empty header-only table that looks broken.
        raw_disp = html.escape((raw or '').strip()) or '(no output captured from the container)'
        body = (
            f'<h3>tethys-series acquisition report &mdash; {html.escape(run_date)}</h3>'
            '<p style="color:#b00020">No driver results could be parsed from the '
            '<code>acquisition_report</code> container output. Raw output (last stdout line pushed to XCom):</p>'
            f'<pre style="background:#f5f5f5;border:1px solid #ddd;padding:8px;'
            f'white-space:pre-wrap;font-size:12px">{raw_disp}</pre>'
        )

    send_email(to=failure_emails, subject='tethys-series daily acquisition report', html_content=body)
    return report


with DAG(
    'tethys_series_acquisition_report',
    default_args=default_args,
    description='Daily storage-only acquisition diagnostic across the tethys-series drivers',
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'series', 'report'],
) as dag:

    command = [
        'BaseSeries', 'acquisition_report',
        '--class_args', json.dumps([]),
        '--class_kwargs', json.dumps({}),
        '--fun_args', json.dumps([]),
        # max_locations caps the per-driver drill-down; with 8 drivers aggregated into one
        # Result: line, the default of 99 locations/driver can exceed Docker's ~16KB log-line
        # limit and get silently truncated before DockerOperator pushes it to XCom.
        '--fun_kwargs', json.dumps({'drivers': DRIVERS, 'max_locations': 6}),
    ]

    report = DockerOperator(
        task_id='acquisition_report',
        image='tethys-series:latest',
        command=command,
        api_version='auto',
        auto_remove='success',
        mounts=container_mounts,
        environment=container_env,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        do_xcom_push=True,
        mount_tmp_dir=False,
    )

    notify = PythonOperator(
        task_id='email_report',
        python_callable=email_report,
    )

    report >> notify

if __name__ == "__main__":
    dag.test()
