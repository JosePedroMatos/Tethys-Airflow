from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

failure_emails = [
    email.strip()
    for email in os.environ.get('FAILURE_EMAILS', '').split(',')
    if email.strip()
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': failure_emails,
    'retries': 0,
}


def fail_intentionally():
    raise Exception("Test failure — email notifications are working!")


with DAG(
    'test_email_on_failure',
    default_args=default_args,
    description='Manually triggered DAG to verify email-on-failure notifications',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'email', 'tethys'],
) as dag:

    trigger_failure = PythonOperator(
        task_id='trigger_failure',
        python_callable=fail_intentionally,
    )
