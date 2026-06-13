"""Load Gold into AstraDB and run demo queries."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cassandra.client import (
    AstraConfigError,
    execute_cql_script,
    get_cassandra_session,
    is_astra_configured,
    read_cql_file,
    read_cql_statement,
)
from src.config import CASSANDRA_KEYSPACE
from src.jobs.gold_batch import ORG_DAILY_USAGE_BY_SERVICE

TABLE_NAME = "org_daily_usage_by_service"
CQL_CREATE_TABLES = "00_create_tables.cql"
CQL_QUERY_DAILY_COSTS = "01_daily_costs_and_requests.cql"
CQL_QUERY_TOP_SERVICES = "02_top_services_by_cost.cql"

DEFAULT_CONCURRENCY = int(os.environ.get("CASSANDRA_LOAD_CONCURRENCY", "50"))
DEFAULT_PROGRESS_EVERY = int(os.environ.get("CASSANDRA_PROGRESS_EVERY", "500"))

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


def setup_table(session) -> None:
    session.set_keyspace(CASSANDRA_KEYSPACE)
    execute_cql_script(session, read_cql_file(CQL_CREATE_TABLES))


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
    concurrency: int = DEFAULT_CONCURRENCY,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    skip_if_loaded: bool = True,
) -> dict[str, Any]:
    from cassandra.concurrent import execute_concurrent_with_args

    gold_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    gold_count = gold_df.count()

    rows = [row.asDict() for row in gold_df.collect()]
    params = [_row_to_params(row) for row in rows]

    count_before = None
    if skip_if_loaded and rows:
        sample_org = rows[0]["org_id"]
        count_before = session.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE org_id = %s",
            (sample_org,),
        ).one()[0]
        if count_before > 0:
            print(
                f"  Sample org_id={sample_org}: {count_before} rows. "
                "Continuing idempotent upsert..."
            )

    prepared = session.prepare(INSERT_CQL)
    print(f"  Loading {gold_count} rows (concurrency={concurrency})...")

    inserted = 0
    errors = 0
    for start in range(0, len(params), progress_every):
        chunk = params[start : start + progress_every]
        chunk_results = execute_concurrent_with_args(
            session,
            prepared,
            chunk,
            concurrency=concurrency,
            raise_on_first_error=False,
        )
        for success, result in chunk_results:
            if success:
                inserted += 1
            else:
                errors += 1
                if errors <= 3:
                    print(f"  INSERT error: {result}", file=sys.stderr)
        done = min(start + progress_every, len(params))
        print(f"  Progress: {done}/{len(params)} rows sent")

    return {
        "table_name": TABLE_NAME,
        "gold_path": ORG_DAILY_USAGE_BY_SERVICE,
        "gold_row_count": gold_count,
        "rows_upserted": inserted,
        "insert_errors": errors,
        "sample_org_count_before": count_before,
        "idempotent_ok": errors == 0 and inserted == gold_count,
    }


def run_query_daily_costs_and_requests(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_date: str = "2025-07-01",
    end_date: str = "2025-08-31",
) -> list[dict[str, Any]]:
    cql = read_cql_statement(CQL_QUERY_DAILY_COSTS)
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
    cql = read_cql_statement(CQL_QUERY_TOP_SERVICES)
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
    skip_load: bool = False,
    org_id: str = "org_rixa11dp",
) -> dict[str, Any]:
    session, cluster = get_cassandra_session()
    try:
        if setup_ddl:
            setup_table(session)

        load_result = None
        if not skip_load:
            load_result = load_org_daily_usage_by_service(spark, session)
        else:
            print("  Load skipped (--skip-load).")

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
        "astra_configured": is_astra_configured(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Gold into AstraDB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate Gold without connecting to AstraDB",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Run DDL and queries only; skip Gold load",
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
        print("DRY RUN — Gold ready to load:")
        for key, value in preview.items():
            if key != "sample_rows":
                print(f"  {key}: {value}")
        spark.stop()
        raise SystemExit(0)

    if not is_astra_configured():
        preview = dry_run_preview(spark)
        print("Gold ready to load, but AstraDB credentials are missing:")
        print(f"  gold_row_count: {preview['gold_row_count']}")
        print(
            "\nSet ASTRA_DB_APPLICATION_TOKEN and "
            "ASTRA_DB_SECURE_BUNDLE_PATH (see .env.example)."
        )
        spark.stop()
        raise SystemExit(1)

    try:
        result = run_serving(spark, skip_load=args.skip_load)
        print("LOAD:", result["load"])
        print(f"QUERY #1 rows: {result['query1_rows']}")
        print("QUERY #1 sample:", result["query1_sample"])
        print("QUERY #2 top:", result["query2_top"])
    except AstraConfigError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("\nCancelled by user.", file=sys.stderr)
        raise SystemExit(130)
    finally:
        try:
            spark.stop()
        except Exception:
            pass
