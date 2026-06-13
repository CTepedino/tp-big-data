"""AstraDB client."""

from __future__ import annotations

from pathlib import Path

from src.config import (
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_SECURE_BUNDLE_PATH,
    CASSANDRA_KEYSPACE,
    CQL_DIR,
)


class AstraConfigError(RuntimeError):
    """Missing or invalid AstraDB configuration."""


def is_astra_configured() -> bool:
    return bool(ASTRA_DB_APPLICATION_TOKEN and ASTRA_DB_SECURE_BUNDLE_PATH)


def get_cassandra_session():
    if not is_astra_configured():
        raise AstraConfigError(
            "Set ASTRA_DB_APPLICATION_TOKEN and ASTRA_DB_SECURE_BUNDLE_PATH "
            "(see .env.example)."
        )

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    bundle_path = Path(ASTRA_DB_SECURE_BUNDLE_PATH)
    if not bundle_path.exists():
        raise AstraConfigError(f"Secure connect bundle not found: {bundle_path}")

    auth = PlainTextAuthProvider("token", ASTRA_DB_APPLICATION_TOKEN)
    cluster = Cluster(
        cloud={"secure_connect_bundle": str(bundle_path)},
        auth_provider=auth,
    )
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session, cluster


def _parse_cql_statements(script: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []
    return statements


def read_cql_file(filename: str) -> str:
    return (Path(CQL_DIR) / filename).read_text(encoding="utf-8")


def read_cql_statement(filename: str) -> str:
    statements = _parse_cql_statements(read_cql_file(filename))
    if not statements:
        raise ValueError(f"No CQL statements found in {filename}")
    if len(statements) > 1:
        raise ValueError(f"Expected one statement in {filename}, found {len(statements)}")
    return statements[0]


def execute_cql_script(session, script: str) -> None:
    for statement in _parse_cql_statements(script):
        session.execute(statement)
