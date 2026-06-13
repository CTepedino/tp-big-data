"""Datalake paths and AstraDB settings."""

import os
from pathlib import Path


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(repo_root / ".env")


_load_dotenv()


def _default_data_root() -> str:
    env_root = os.environ.get("DATA_ROOT")
    if env_root:
        return env_root

    colab_root = Path("/content/datalake")
    if colab_root.exists():
        return str(colab_root)

    repo_datalake = Path.cwd() / "datalake"
    if repo_datalake.exists():
        return str(repo_datalake)

    return str(Path.cwd() / "datalake")


DATA_ROOT = _default_data_root()
LANDING = os.path.join(DATA_ROOT, "landing")
BRONZE = os.path.join(DATA_ROOT, "bronze")
SILVER = os.path.join(DATA_ROOT, "silver")
GOLD = os.path.join(DATA_ROOT, "gold")
QUARANTINE = os.path.join(DATA_ROOT, "quarantine")
CHECKPOINTS = os.path.join(DATA_ROOT, "checkpoints")

CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "cloud_analytics")
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN", "")
ASTRA_DB_SECURE_BUNDLE_PATH = os.environ.get("ASTRA_DB_SECURE_BUNDLE_PATH", "")

CQL_DIR = str(Path(__file__).resolve().parent.parent / "cql")

SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
SPARK_TARGET_FILES_MASTER = int(os.environ.get("SPARK_TARGET_FILES_MASTER", "1"))
SPARK_TARGET_FILES_EVENTS = int(os.environ.get("SPARK_TARGET_FILES_EVENTS", "8"))
SPARK_TARGET_FILES_GOLD = int(os.environ.get("SPARK_TARGET_FILES_GOLD", "4"))
