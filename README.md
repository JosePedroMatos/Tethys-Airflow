# Tethys-Airflow
Backend task management for Tethys

## Prerequisites

- Docker Desktop installed and running
- PostgreSQL database running on host (port 5432)
- Redis server running on host (port 6379)
- At least 4GB RAM and 10GB disk space available to Docker

## Setup

### 1. Environment Variables

Create a `.env` file in the project root with the following variables (see [.env template](#env-template)):

```env
# Airflow UID (use your user ID on Linux, 50000 on Windows/Mac)
AIRFLOW_UID=50000

# Airflow Admin Credentials
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# PostgreSQL Configuration
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your-secure-password
POSTGRES_DB=airflow_db

# Redis Configuration
REDIS_HOST=host.docker.internal
REDIS_PORT=6379
REDIS_DB=1
```

### 2. Prepare the Docker Image

The project uses Apache Airflow 2.10.0 as the base image. Initialize the Airflow database and create necessary directories:

```bash
docker compose up airflow-init
```

This command will:
- Check system resources (RAM, CPU, disk space)
- Create required directories (logs, dags, plugins)
- Set up the Airflow database
- Create the admin user

### 3. Running the Services

Start all Airflow services:

```bash
docker compose up -d
```

This will start:
- **Airflow Webserver** (http://localhost:7080)
- **Airflow Scheduler** - Monitors DAGs and triggers tasks
- **Airflow Worker** - Executes tasks using Celery
- **Airflow Triggerer** - Handles deferred tasks

### 4. Configure Resource Pools (Optional)

If you want to limit concurrent execution of specific tasks (e.g., Docker containers), create a pool:

```bash
docker compose exec airflow-webserver airflow pools set tethys_tasks_pool 2 "Limit concurrent tethys-tasks container calls"
```

This creates a pool with 2 concurrent slots. Adjust the number as needed for your infrastructure.

### 5. Accessing Airflow

Once the services are running, access the Airflow web interface at:

```
http://localhost:7080
```

Login credentials (default):
- Username: `airflow`
- Password: `airflow`

## Managing the Services

### Stop Services

```bash
docker compose down
```

### View Logs

```bash
# All services
docker compose logs

# Specific service
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
docker compose logs airflow-worker
```

### Restart Services

```bash
docker compose restart
```

### Access Airflow CLI

```bash
docker compose run --rm airflow-cli
```

## Configuration

The setup uses:
- **Executor**: CeleryExecutor for distributed task execution
- **Database**: PostgreSQL on host (connects via `host.docker.internal`)
- **Broker**: Redis on host (DB 1)
- **Web UI Port**: 7080 (mapped from container's 8080)

## Project Structure

```
.
├── docker-compose.yaml       # Docker services configuration
├── dags/                     # Airflow DAG definitions
│   ├── example_dag.py
│   └── tethys_tasks_dag.py
├── data/                     # Shared data storage
├── logs/                     # Airflow logs (created on init)
├── config/                   # Airflow configuration files
└── plugins/                  # Custom Airflow plugins
```

## Troubleshooting

### Services won't start
- Check that PostgreSQL and Redis are running on the host
- Verify port 7080 is not already in use
- Ensure Docker has sufficient resources allocated

### Permission issues
- On Linux, set `AIRFLOW_UID` in `.env` to your user ID: `AIRFLOW_UID=$(id -u)`
- Run `docker compose down -v` and reinitialize with `docker compose up airflow-init`

### Connection issues
- Verify `host.docker.internal` resolves correctly
- Check PostgreSQL allows connections from Docker containers
- Ensure Redis is accessible from Docker (no firewall blocking)
