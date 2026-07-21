import os
from docker.types import Mount

# Directory (inside the Airflow containers) where the per-component env files
# are mounted read-only. See the `volumes:` block in docker-compose.yaml.
ENV_DIR = os.environ.get('TETHYS_ENV_DIR', '/opt/airflow/env')

# Folder variables every component's env file must define. The host-side
# *_FOLDER paths become DockerOperator bind-mount sources; the *_DOCKER paths
# are what the container sees.
_REQUIRED_FOLDER_VARS = (
    'LOCAL_FILE_FOLDER',
    'STORAGE_FILE_FOLDER',
    'LOCAL_FILE_FOLDER_DOCKER',
    'STORAGE_FILE_FOLDER_DOCKER',
)


def _parse_env_file(path: str) -> dict:
    """Minimal .env parser (stdlib only).

    Splits each line on the FIRST '=', skips blank lines and #-comments
    (leading whitespace allowed), and keeps values verbatim -- no quote
    stripping and no ${VAR} interpolation, matching docker-compose env_file
    semantics. Opened as utf-8-sig so a Windows BOM cannot corrupt the first
    key.
    """
    values = {}
    with open(path, encoding='utf-8-sig') as fh:
        for raw in fh:
            line = raw.strip()  # also drops a trailing CR on CRLF files
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')  # split on the FIRST '=' only
            key = key.strip()
            if key:
                values[key] = value
    return values


def load_component_config(component: str = 'tasks') -> dict:
    """Read and parse `.env-<component>` from ENV_DIR.

    Raises a clear error if the file is missing so a misconfiguration fails
    fast at DAG-parse time rather than silently forwarding nothing.
    """
    path = os.path.join(ENV_DIR, f'.env-{component}')
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Tethys env file not found: {path}. Expected '.env-{component}' to "
            f"be mounted read-only at {ENV_DIR} (see docker-compose.yaml volumes)."
        )
    return _parse_env_file(path)


def _validated_config(component: str) -> dict:
    """Load a component's config and assert the required folder vars exist,
    so no None ever reaches the docker SDK for env values or mount paths."""
    cfg = load_component_config(component)
    missing = [v for v in _REQUIRED_FOLDER_VARS if not cfg.get(v)]
    if missing:
        raise ValueError(
            f".env-{component} is missing required variable(s): {', '.join(missing)}."
        )
    return cfg


def build_container_env(component: str = 'tasks') -> dict:
    """Build the environment dict for a tethys-<component> DockerOperator.

    Forwards every non-empty variable from `.env-<component>` -- the file is
    the single source of truth, so there is no hardcoded allow-list to keep in
    sync. Folder paths are then rewritten to their in-container values.
    """
    cfg = _validated_config(component)
    env = {k: v for k, v in cfg.items() if v}
    env.update({
        'LOCAL_FILE_FOLDER': cfg['LOCAL_FILE_FOLDER_DOCKER'],
        'STORAGE_FILE_FOLDER': cfg['STORAGE_FILE_FOLDER_DOCKER'],
        'TMPDIR': cfg['STORAGE_FILE_FOLDER_DOCKER'],  # Fix for "Invalid cross-device link"
    })
    return env


def build_mounts(component: str = 'tasks') -> list:
    """Build the DockerOperator bind mounts for a tethys-<component> container.

    Host paths come from the parsed `.env-<component>`; targets are the
    in-container *_DOCKER paths.
    """
    cfg = _validated_config(component)
    return [
        Mount(source=cfg['LOCAL_FILE_FOLDER'], target=cfg['LOCAL_FILE_FOLDER_DOCKER'], type='bind'),
        Mount(source=cfg['STORAGE_FILE_FOLDER'], target=cfg['STORAGE_FILE_FOLDER_DOCKER'], type='bind'),
    ]


def get_failure_emails() -> list:
    """Read comma-separated failure email addresses from the FAILURE_EMAILS env var.

    FAILURE_EMAILS is an Airflow-level concern (default_args email_on_failure)
    and stays in .env / os.environ, not in the per-component files.
    """
    return [
        email.strip()
        for email in os.environ.get('FAILURE_EMAILS', '').split(',')
        if email.strip()
    ]
