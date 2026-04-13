import os

# Master list of environment variables to forward to tethys-tasks containers.
# All variables are filtered by presence (missing ones are silently skipped),
# so this single list is safe to use across all DAG types.
TETHYS_VARS = [
    # Local folders, including for Docker
    'LOCAL_FILE_FOLDER',
    'STORAGE_FILE_FOLDER',
    'LOCAL_FILE_FOLDER_DOCKER',
    'STORAGE_FILE_FOLDER_DOCKER',

    # Long term cloud storage
    'CLOUD_UPLOAD_LOCAL',
    'AZURE_STORAGE_CONNECTION_STRING',
    'CLOUD_STORAGE_FOLDER',
    'CLOUD_PARALLEL_TRANSFERS',

    # Short-term sync of stored files
    'SYNC_LATEST_STORED',
    'DROPBOX_APP_KEY',
    'DROPBOX_APP_SECRET',
    'DROPBOX_REFRESH_TOKEN',
    'DROPBOX_ACCESS_TOKEN',
    'DROPBOX_ROOT_PATH',
    'DROPBOX_PARALLEL_TRANSFERS',

    # Data from Copernicus. Can be obtained freely.
    'CDSAPI_URL',
    'CDSAPI_KEY',

    # Data from NASA (e.g., IMERG). Can be obtained freely.
    'EARTH_DATA_USER',
    'EARTH_DATA_PASSWORD',

    # CDSE S3 credentials for CLMS downloads. Can be obtained freely.
    'CDSE_S3_ACCESS_KEY',
    'CDSE_S3_SECRET_KEY',
    'CDSE_S3_ENDPOINT_URL',
    'CDSE_S3_REGION_NAME',
    'CDSE_S3_BUCKET',

    # Failure notifications
    'FAILURE_EMAILS',
]


def build_container_env() -> dict:
    """Build the environment dict to pass to a tethys-tasks DockerOperator."""
    env = {v: os.environ.get(v) for v in TETHYS_VARS if os.environ.get(v)}
    env.update({
        'LOCAL_FILE_FOLDER': os.environ.get('LOCAL_FILE_FOLDER_DOCKER'),
        'STORAGE_FILE_FOLDER': os.environ.get('STORAGE_FILE_FOLDER_DOCKER'),
        'TMPDIR': os.environ.get('STORAGE_FILE_FOLDER_DOCKER'),  # Fix for "Invalid cross-device link"
    })
    return env


def get_failure_emails() -> list:
    """Read comma-separated failure email addresses from FAILURE_EMAILS env var."""
    return [
        email.strip()
        for email in os.environ.get('FAILURE_EMAILS', '').split(',')
        if email.strip()
    ]
