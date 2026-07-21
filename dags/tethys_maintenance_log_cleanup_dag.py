from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import os
import time
import shutil

# Airflow's own log folder inside the containers. We read it from the same
# config Airflow uses, so this keeps working if BASE_LOG_FOLDER is ever changed.
LOG_DIR = os.environ.get('AIRFLOW__LOGGING__BASE_LOG_FOLDER', '/opt/airflow/logs')


def clean_logs(**context):
    """Delete task/scheduler log files older than `keep_days`, then prune the
    empty directories left behind. Set `dry_run=True` to preview without
    deleting. Reports how many files and bytes were removed."""
    params = context['params']
    keep_days = int(params['keep_days'])
    dry_run = bool(params['dry_run'])

    if keep_days < 1:
        # A guard so a stray 0/negative value can never wipe *all* logs.
        raise ValueError(f"keep_days must be >= 1, got {keep_days}")

    if not os.path.isdir(LOG_DIR):
        print(f"Log directory {LOG_DIR} does not exist -- nothing to do.")
        return {'deleted_files': 0, 'freed_bytes': 0}

    cutoff = time.time() - keep_days * 86400
    cutoff_str = datetime.fromtimestamp(cutoff).isoformat(timespec='seconds')
    print(f"{'DRY RUN: ' if dry_run else ''}Removing files under {LOG_DIR} "
          f"older than {keep_days} days (modified before {cutoff_str}).")

    deleted_files = 0
    freed_bytes = 0
    # topdown=False so child files are handled before we test a dir for emptiness.
    for root, dirs, files in os.walk(LOG_DIR, topdown=False):
        for name in files:
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
            except OSError:
                continue  # vanished mid-walk (a live task rotating logs) -- skip
            if st.st_mtime >= cutoff:
                continue  # still within the retention window -- keep
            freed_bytes += st.st_size
            deleted_files += 1
            if not dry_run:
                try:
                    os.remove(path)
                except OSError as exc:
                    print(f"  could not remove {path}: {exc}")
                    freed_bytes -= st.st_size
                    deleted_files -= 1

        # Prune now-empty directories (never the log root itself).
        if not dry_run and root != LOG_DIR:
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except OSError:
                pass

    freed_gb = freed_bytes / 1024 ** 3
    print(f"{'Would free' if dry_run else 'Freed'} {freed_gb:.2f} GB "
          f"across {deleted_files} files.")
    try:
        usage = shutil.disk_usage(LOG_DIR)
        print(f"Disk now: {usage.free / 1024 ** 3:.1f} GB free of "
              f"{usage.total / 1024 ** 3:.1f} GB.")
    except OSError:
        pass

    return {'deleted_files': deleted_files, 'freed_bytes': freed_bytes}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'tethys_maintenance_log_cleanup',
    default_args=default_args,
    description='Delete Airflow log files older than a configurable retention window',
    schedule_interval='0 3 * * *',  # daily at 03:00; DAG is paused until you enable it
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['tethys', 'maintenance', 'logs'],
    params={
        'keep_days': Param(
            30,
            type='integer',
            minimum=1,
            title='Retention (days)',
            description='Delete log files last modified more than this many days ago.',
        ),
        'dry_run': Param(
            False,
            type='boolean',
            title='Dry run',
            description='Preview what would be deleted without removing anything.',
        ),
    },
) as dag:

    cleanup = PythonOperator(
        task_id='clean_old_logs',
        python_callable=clean_logs,
    )

if __name__ == "__main__":
    dag.test()
