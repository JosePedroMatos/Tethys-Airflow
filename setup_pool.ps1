# Create a pool to limit concurrent tethys-tasks Docker container executions
# This limits parallel calls to tethys-tasks:latest across ALL DAGs

docker compose run --rm airflow-cli airflow pools set tethys_tasks_pool 2 "Limit concurrent tethys-tasks container calls"
