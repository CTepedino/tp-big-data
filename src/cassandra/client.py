"""Cliente Cassandra / AstraDB."""

from __future__ import annotations

import os
from pathlib import Path

from src.config import (
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_SECURE_BUNDLE_PATH,
    CASSANDRA_CONTACT_POINTS,
    CASSANDRA_KEYSPACE,
    CASSANDRA_PASSWORD,
    CASSANDRA_USERNAME,
    CQL_DIR,
)


class CassandraConfigError(RuntimeError):
    """Faltan variables de entorno para conectar a Astra/Cassandra."""


def is_astra_configured() -> bool:
    return bool(ASTRA_DB_APPLICATION_TOKEN and ASTRA_DB_SECURE_BUNDLE_PATH)


def is_local_cassandra_configured() -> bool:
    return bool(CASSANDRA_CONTACT_POINTS)


def is_cassandra_configured() -> bool:
    return is_astra_configured() or is_local_cassandra_configured()


def get_cassandra_session():
    """Abre sesión contra AstraDB (bundle) o Cassandra local."""
    if not is_cassandra_configured():
        raise CassandraConfigError(
            "Configurar ASTRA_DB_APPLICATION_TOKEN + ASTRA_DB_SECURE_BUNDLE_PATH "
            "o CASSANDRA_CONTACT_POINTS para cargar/consultar."
        )

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    if is_astra_configured():
        bundle_path = Path(ASTRA_DB_SECURE_BUNDLE_PATH)
        if not bundle_path.exists():
            raise CassandraConfigError(
                f"No se encontró el secure bundle: {bundle_path}"
            )
        auth = PlainTextAuthProvider("token", ASTRA_DB_APPLICATION_TOKEN)
        cluster = Cluster(
            cloud={"secure_connect_bundle": str(bundle_path)},
            auth_provider=auth,
        )
    else:
        hosts = [host.strip() for host in CASSANDRA_CONTACT_POINTS.split(",")]
        auth = None
        if CASSANDRA_USERNAME:
            auth = PlainTextAuthProvider(CASSANDRA_USERNAME, CASSANDRA_PASSWORD)
        cluster = Cluster(hosts, auth_provider=auth)

    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session, cluster


def read_cql_file(filename: str) -> str:
    path = Path(CQL_DIR) / filename
    return path.read_text(encoding="utf-8")


def execute_cql_script(session, script: str) -> None:
    """Ejecuta un script CQL statement por statement."""
    statements = []
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []

    for statement in statements:
        session.execute(statement)
