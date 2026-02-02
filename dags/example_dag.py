from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import socket
import random

# 1. Inspecting Variables in Logs & XCom
def check_host_connection():
    # This task checks if we can resolve the host machine
    # To connect to your Host Postgres/Celery, use 'host.docker.internal' as the hostname
    try:
        host_ip = socket.gethostbyname('host.docker.internal')
        
        # Method A: LOGS
        # Anything printed to stdout appears in the Task Log tab in UI
        print(f"Successfully resolved host.docker.internal to {host_ip}")
        
        # Method B: XCOM (Return Value)
        # The return value is automatically saved to the XCom database.
        # You can see it in the "XCom" tab of the Task Instance details.
        return host_ip
    except socket.gaierror:
        print("Could not resolve host.docker.internal")
        raise

def unreliable_task():
    # Generate a random float between 0.0 and 1.0
    chance = random.random()
    print(f"Random value generated: {chance}")
    
    # Fail 60% of the time (if value is less than 0.6)
    if chance < 0.6:
        raise Exception("Random failure triggered! (60% probability)")
    
    print("Task succeeded (40% probability)")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

# 2. Complex Schedule Implementation
# Requirement: "Hourly" AND "Daily at 11:02"
# Since Airflow's cron scheduler cannot do unions easily, the best pattern
# is to define the DAG structure once in a function, and instantiate it twice.

def create_dag(dag_id, schedule_cron):
    """
    Factory function to create the DAG with a specific schedule.
    """
    with DAG(
        dag_id,
        default_args=default_args,
        description='A simple DAG to demonstrate host interaction env',
        schedule_interval=schedule_cron,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['example', 'host'],
        params={
            "series": Param("default_value", type="string", description="What to process?"),
        },
    ) as dag:
    
        t1 = BashOperator(
            task_id='print_date',
            bash_command='date',
        )
    
        # t2 = PythonOperator(
        #     task_id='check_host_network',
        #     python_callable=check_host_connection,
        # )
    
        t3 = PythonOperator(
            task_id='unreliable_step',
            python_callable=unreliable_task,
        )

        # t1 >> t2 >> t3
        t1 >> t3
    return dag

# Instance 1: Runs every hour (at minute 0)
dag_hourly = create_dag('example_host_interaction_hourly', '0 * * * *')

# Instance 2: Runs once a day at 11:02
dag_daily = create_dag('example_host_interaction_1102', '2 11 * * *')

if __name__ == "__main__":
    dag_hourly.test()