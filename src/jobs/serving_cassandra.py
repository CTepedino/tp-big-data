"""Carga Gold -> Cassandra/AstraDB y consultas de evidencia."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cassandra.client import (
    CassandraConfigError,
    execute_cql_script,
    get_cassandra_session,
    is_cassandra_configured,
    read_cql_file,
)
from src.config import CASSANDRA_KEYSPACE
from src.jobs.gold_batch import ORG_DAILY_USAGE_BY_SERVICE

TABLE_NAME = "org_daily_usage_by_service"
INSERT_CQL = f"""
INSERT INTO {TABLE_NAME} (
    org_id, usage_date, service,
    org_name, org_industry, org_plan_tier,
    event_count, total_daily_cost_usd, total_requests,
    total_genai_tokens, total_carbon_kg,
    anomaly_event_count, has_cost_anomaly, gold_ts
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return value


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return bool(int(value))


def setup_keyspace_and_table(session) -> None:
    """Ejecuta DDL desde scripts CQL."""
    keyspace_script = read_cql_file("01_keyspace.cql")
    execute_cql_script(session, keyspace_script)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    table_script = read_cql_file("02_org_daily_usage_by_service.cql")
    execute_cql_script(session, table_script)


def _row_to_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        _to_date(row["usage_date"]),
        row["service"],
        row.get("org_name"),
        row.get("org_industry"),
        row.get("org_plan_tier"),
        int(row["event_count"]),
        float(row["total_daily_cost_usd"]),
        float(row["total_requests"]),
        int(row["total_genai_tokens"]),
        float(row["total_carbon_kg"]),
        int(row["anomaly_event_count"]),
        _to_bool(row["has_cost_anomaly"]),
        row.get("gold_ts"),
    )


def load_org_daily_usage_by_service(
    spark: SparkSession,
    session,
    *,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Carga idempotente (upsert por PK) desde Gold Parquet."""
    gold_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    gold_count = gold_df.count()

    count_before = session.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").one()[0]

    prepared = session.prepare(INSERT_CQL)
    rows = [row.asDict() for row in gold_df.collect()]

    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i : i + batch_size]
        for row in batch_rows:
            session.execute(prepared, _row_to_params(row))
            inserted += 1

    count_after = session.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").one()[0]

    return {
        "table_name": TABLE_NAME,
        "gold_path": ORG_DAILY_USAGE_BY_SERVICE,
        "gold_row_count": gold_count,
        "rows_upserted": inserted,
        "count_before": count_before,
        "count_after": count_after,
        "idempotent_ok": count_after == gold_count,
    }


def run_query_daily_costs_and_requests(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_date: str = "2025-07-01",
    end_date: str = "2025-08-31",
) -> list[dict[str, Any]]:
    """Consulta #1: costos y requests diarios por org y servicio."""
    cql = f"""
        SELECT usage_date, service, total_daily_cost_usd, total_requests,
               total_genai_tokens, total_carbon_kg
        FROM {TABLE_NAME}
        WHERE org_id = %s
          AND usage_date >= %s
          AND usage_date <= %s
    """
    rows = session.execute(
        cql,
        (org_id, date.fromisoformat(start_date), date.fromisoformat(end_date)),
    )
    return [dict(row._asdict()) for row in rows]


def run_query_top_services_by_cost(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_date: str = "2025-08-17",
    end_date: str = "2025-08-31",
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Consulta #2: top-N servicios por costo acumulado (agregación en driver)."""
    cql = f"""
        SELECT service, usage_date, total_daily_cost_usd
        FROM {TABLE_NAME}
        WHERE org_id = %s
          AND usage_date >= %s
          AND usage_date <= %s
    """
    rows = session.execute(
        cql,
        (org_id, date.fromisoformat(start_date), date.fromisoformat(end_date)),
    )

    totals: dict[str, float] = {}
    for row in rows:
        totals[row.service] = totals.get(row.service, 0.0) + float(
            row.total_daily_cost_usd
        )

    ranked = sorted(totals.items(), key=lambda item: item[1], reverse=True)[:top_n]
    return [
        {"org_id": org_id, "service": service, "accumulated_cost_usd": cost}
        for service, cost in ranked
    ]


def run_serving(
    spark: SparkSession,
    *,
    setup_ddl: bool = True,
    org_id: str = "org_rixa11dp",
) -> dict[str, Any]:
    """Pipeline completo: DDL + carga + consultas #1 y #2."""
    session, cluster = get_cassandra_session()
    try:
        if setup_ddl:
            setup_keyspace_and_table(session)

        load_result = load_org_daily_usage_by_service(spark, session)
        query1 = run_query_daily_costs_and_requests(session, org_id=org_id)
        query2 = run_query_top_services_by_cost(session, org_id=org_id)

        return {
            "load": load_result,
            "query1_rows": len(query1),
            "query1_sample": query1[:5],
            "query2_top": query2,
        }
    finally:
        cluster.shutdown()


def dry_run_preview(spark: SparkSession) -> dict[str, Any]:
    """Preview sin conexión: valida Gold listo para carga."""
    gold_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    sample = gold_df.limit(3).collect()
    top_org = (
        gold_df.groupBy("org_id")
        .agg(F.sum("total_daily_cost_usd").alias("cost"))
        .orderBy(F.desc("cost"))
        .first()
    )
    return {
        "gold_row_count": gold_df.count(),
        "sample_rows": [row.asDict() for row in sample],
        "suggested_org_id_for_queries": top_org["org_id"] if top_org else "org_rixa11dp",
        "cassandra_configured": is_cassandra_configured(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cargar Gold en Cassandra/AstraDB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validar Gold sin conectar a Cassandra",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("serving-cassandra")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if args.dry_run:
        preview = dry_run_preview(spark)
        print("DRY RUN — Gold listo para carga:")
        for key, value in preview.items():
            if key != "sample_rows":
                print(f"  {key}: {value}")
        spark.stop()
        raise SystemExit(0)

    if not is_cassandra_configured():
        preview = dry_run_preview(spark)
        print("Gold listo para carga, pero faltan credenciales Cassandra/Astra:")
        print(f"  gold_row_count: {preview['gold_row_count']}")
        print(
            "\nConfigurar ASTRA_DB_APPLICATION_TOKEN y "
            "ASTRA_DB_SECURE_BUNDLE_PATH (ver .env.example)."
        )
        spark.stop()
        raise SystemExit(1)

    try:
        result = run_serving(spark)
        print("LOAD:", result["load"])
        print(f"QUERY #1 rows: {result['query1_rows']}")
        print("QUERY #1 sample:", result["query1_sample"])
        print("QUERY #2 top:", result["query2_top"])
    except CassandraConfigError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1) from exc
    finally:
        spark.stop()
