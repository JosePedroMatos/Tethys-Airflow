from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from pathlib import Path

# This path comes from your Windows environment (via the .env file)
# It tells the Docker daemon where to find the data on your host
TRANSFER_FOLDER = os.environ.get('TRANSFER_FOLDER')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def read_and_verify_parquet(**context):
    # 1. Get the relative path from the previous task's output (XCom)
    raw_output = context['ti'].xcom_pull(task_ids='retrieve_era5_data')
    
    # Extract the path directly after "Result: "
    if "Result: " in raw_output:
        # Get everything after the last "Result: " and take only the first line of that
        relative_path = raw_output.split("Result: ")[-1].splitlines()[0].strip()
    else:
        raise Exception('The file location is missing from the xcom_pull.')
    
    # 2. Construct the full path inside the Airflow container
    # Airflow container sees the data at /opt/airflow/data
    full_path = Path('/opt/airflow/data') / relative_path
    
    print(f"Attempting to read file: {full_path}")
    
    if full_path.exists():
        df = pd.read_parquet(full_path)
        print(f"Successfully read Parquet. Shape: {df.shape}")
        print(df.head())
    else:
        raise FileNotFoundError(f"Result file not found at {full_path}")

with DAG(
    'tethys_era5_pipeline',
    default_args=default_args,
    description='Pipeline to retrieve ERA5 data via tethys-tasks container',
    schedule_interval=None,
    catchup=False,
    tags=['tethys', 'era5'],
) as dag:

    class_ = 'era5'
    function_ = 'retrieve_from_source'
    class_args = []
    class_kwargs = dict(transfer_folder='/transfer')
    fun_args = []
    fun_kwargs = dict(file_path=r'era5_airflow/era5_test.parquet')

    command_1 = [
        class_,
        function_,
        '--class_args', json.dumps(class_args),
        '--class_kwargs', json.dumps(class_kwargs),
        '--fun_args', json.dumps(fun_args),
        '--fun_kwargs', json.dumps(fun_kwargs)
    ]

    print(f'Running command: {command_1}.')

    # Task 1: Run the specialized container
    # This assumes retrieve_from_source PRINTS the relative result path to stdout
    t1 = DockerOperator(
        task_id='retrieve_era5_data',
        image='tethys-tasks:latest',
        # Construct the command based on your README
        command=command_1,
        api_version='auto',
        auto_remove='success',
        # Mount the shared data folder
        mounts=[
            Mount(source=TRANSFER_FOLDER, target='/transfer', type='bind')
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        # Capture the output of the script (the relative path)
        do_xcom_push=True,
        mount_tmp_dir=False,
    )

    # Task 2: Process the result inside Airflow
    t2 = PythonOperator(
        task_id='process_result_file',
        python_callable=read_and_verify_parquet,
        provide_context=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.test()