"""Load Gold into AstraDB via foreachBatch and run demo queries."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.cassandra.client import (
    AstraConfigError,
    execute_cql_script,
    get_cassandra_session,
    is_astra_configured,
    read_cql_file,
)
from src.cassandra.foreach_batch_loader import (
    load_dataframe_via_foreach_batch,
    load_parquet_via_foreach_batch,
)
from src.spark.performance import configure_spark_performance
from src.cassandra.queries import (
    INSERT_GENAI,
    INSERT_ORG_DAILY,
    INSERT_REVENUE,
    INSERT_TICKETS,
    INSERT_TOP_SERVICES,
    QUERY_CRITICAL_TICKETS,
    QUERY_DAILY_COSTS,
    QUERY_GENAI_TOKENS_DAILY,
    QUERY_MONTHLY_REVENUE,
    QUERY_TOP_SERVICES,
    TABLE_GENAI,
    TABLE_ORG_DAILY,
    TABLE_REVENUE,
    TABLE_TICKETS,
    TABLE_TOP_SERVICES,
)
from src.cassandra.queries.constants import DEFAULT_PERIOD_END, DEFAULT_PERIOD_START
from src.config import CASSANDRA_KEYSPACE
from src.jobs.gold import (
    GENAI_TOKENS_BY_ORG_DATE,
    ORG_DAILY_USAGE_BY_SERVICE,
    REVENUE_BY_ORG_MONTH,
    TICKETS_BY_ORG_DATE,
)

CQL_CREATE_TABLES = "00_create_tables.cql"
DEFAULT_TOP_N = 5


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


def setup_tables(session) -> None:
    session.set_keyspace(CASSANDRA_KEYSPACE)
    execute_cql_script(session, read_cql_file(CQL_CREATE_TABLES))


def _org_daily_params(row: dict[str, Any]) -> tuple:
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


def load_org_daily_usage_by_service(spark: SparkSession, session) -> dict[str, Any]:
    return load_parquet_via_foreach_batch(
        spark,
        ORG_DAILY_USAGE_BY_SERVICE,
        TABLE_ORG_DAILY,
        insert_cql=INSERT_ORG_DAILY,
        row_to_params=_org_daily_params,
        session=session,
    )


def build_top_services_dataframe(
    spark: SparkSession,
    *,
    period_start: str = DEFAULT_PERIOD_START,
    period_end: str = DEFAULT_PERIOD_END,
    top_n: int = DEFAULT_TOP_N,
) -> DataFrame:
    start = date.fromisoformat(period_start)
    end = date.fromisoformat(period_end)

    ranked = (
        spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
        .filter(
            (F.col("usage_date") >= F.lit(start))
            & (F.col("usage_date") <= F.lit(end))
        )
        .groupBy("org_id", "service")
        .agg(F.sum("total_daily_cost_usd").alias("accumulated_cost_usd"))
    )
    window = Window.partitionBy("org_id").orderBy(F.desc("accumulated_cost_usd"))
    return (
        ranked.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
        .withColumn("period_end", F.lit(end))
        .withColumn("period_start", F.lit(start))
        .select(
            "org_id",
            "period_end",
            "rank",
            "service",
            "period_start",
            "accumulated_cost_usd",
        )
    )


def _top_services_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        _to_date(row["period_end"]),
        int(row["rank"]),
        row["service"],
        _to_date(row["period_start"]),
        float(row["accumulated_cost_usd"]),
    )


def load_org_top_services_by_cost(
    spark: SparkSession,
    session,
    *,
    period_start: str = DEFAULT_PERIOD_START,
    period_end: str = DEFAULT_PERIOD_END,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    top_df = build_top_services_dataframe(
        spark,
        period_start=period_start,
        period_end=period_end,
        top_n=top_n,
    )
    result = load_dataframe_via_foreach_batch(
        spark,
        top_df,
        TABLE_TOP_SERVICES,
        insert_cql=INSERT_TOP_SERVICES,
        row_to_params=_top_services_params,
        session=session,
    )
    result["gold_path"] = ORG_DAILY_USAGE_BY_SERVICE
    return result


def _tickets_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        row["severity"],
        _to_date(row["ticket_date"]),
        row.get("org_name"),
        int(row["ticket_count"]),
        int(row["sla_breach_count"]),
        float(row["sla_breach_rate"]),
        float(row["avg_csat"]) if row.get("avg_csat") is not None else None,
    )


def load_tickets_by_org_date(spark: SparkSession, session) -> dict[str, Any]:
    return load_parquet_via_foreach_batch(
        spark,
        TICKETS_BY_ORG_DATE,
        TABLE_TICKETS,
        insert_cql=INSERT_TICKETS,
        row_to_params=_tickets_params,
        session=session,
    )


def _revenue_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        _to_date(row["billing_month"]),
        row.get("org_name"),
        float(row["subtotal_usd"]),
        float(row["credits_usd"]),
        float(row["taxes_usd"]),
        float(row["revenue_usd"]),
        int(row["invoice_count"]),
    )


def load_revenue_by_org_month(spark: SparkSession, session) -> dict[str, Any]:
    return load_parquet_via_foreach_batch(
        spark,
        REVENUE_BY_ORG_MONTH,
        TABLE_REVENUE,
        insert_cql=INSERT_REVENUE,
        row_to_params=_revenue_params,
        session=session,
    )


def _genai_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        _to_date(row["usage_date"]),
        row.get("org_name"),
        int(row["total_genai_tokens"]),
        float(row["estimated_cost_usd"]),
        int(row["event_count"]),
    )


def load_genai_tokens_by_org_date(spark: SparkSession, session) -> dict[str, Any]:
    return load_parquet_via_foreach_batch(
        spark,
        GENAI_TOKENS_BY_ORG_DATE,
        TABLE_GENAI,
        insert_cql=INSERT_GENAI,
        row_to_params=_genai_params,
        session=session,
    )


def load_all_gold_marts(spark: SparkSession, session) -> list[dict[str, Any]]:
    loaders = [
        load_org_daily_usage_by_service,
        load_org_top_services_by_cost,
        load_tickets_by_org_date,
        load_revenue_by_org_month,
        load_genai_tokens_by_org_date,
    ]
    return [loader(spark, session) for loader in loaders]


def run_query_daily_costs_and_requests(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_date: str = "2025-07-01",
    end_date: str = "2025-08-31",
) -> list[dict[str, Any]]:
    rows = session.execute(
        QUERY_DAILY_COSTS,
        (org_id, date.fromisoformat(start_date), date.fromisoformat(end_date)),
    )
    return [dict(row._asdict()) for row in rows]


def run_query_top_services_by_cost(
    session,
    *,
    org_id: str = "org_rixa11dp",
    period_end: str = DEFAULT_PERIOD_END,
    top_n: int = DEFAULT_TOP_N,
) -> list[dict[str, Any]]:
    rows = session.execute(
        QUERY_TOP_SERVICES,
        (org_id, date.fromisoformat(period_end), top_n),
    )
    return [dict(row._asdict()) for row in rows]


def run_query_critical_tickets_sla(
    session,
    *,
    org_id: str = "org_rixa11dp",
    severity: str = "high",
    start_date: str = "2025-08-01",
    end_date: str = "2025-08-31",
) -> list[dict[str, Any]]:
    rows = session.execute(
        QUERY_CRITICAL_TICKETS,
        (
            org_id,
            severity,
            date.fromisoformat(start_date),
            date.fromisoformat(end_date),
        ),
    )
    return [dict(row._asdict()) for row in rows]


def run_query_monthly_revenue(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_month: str = "2025-06-01",
    end_month: str = "2025-08-01",
) -> list[dict[str, Any]]:
    rows = session.execute(
        QUERY_MONTHLY_REVENUE,
        (org_id, date.fromisoformat(start_month), date.fromisoformat(end_month)),
    )
    return [dict(row._asdict()) for row in rows]


def run_query_genai_tokens_daily(
    session,
    *,
    org_id: str = "org_rixa11dp",
    start_date: str = "2025-08-01",
    end_date: str = "2025-08-31",
) -> list[dict[str, Any]]:
    rows = session.execute(
        QUERY_GENAI_TOKENS_DAILY,
        (org_id, date.fromisoformat(start_date), date.fromisoformat(end_date)),
    )
    return [dict(row._asdict()) for row in rows]


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
            setup_tables(session)

        load_results: list[dict[str, Any]] | None = None
        if not skip_load:
            print("  Load mode: Structured Streaming foreachBatch → Cassandra")
            load_results = load_all_gold_marts(spark, session)
        else:
            print("  Load skipped (--skip-load).")

        query1 = run_query_daily_costs_and_requests(session, org_id=org_id)
        query3 = run_query_critical_tickets_sla(session, org_id=org_id)
        query4 = run_query_monthly_revenue(session, org_id=org_id)
        query5 = run_query_genai_tokens_daily(session, org_id=org_id)

        return {
            "load": load_results,
            "query1_rows": len(query1),
            "query1_sample": query1[:5],
            "query2_top": run_query_top_services_by_cost(session, org_id=org_id),
            "query3_rows": len(query3),
            "query3_sample": query3[:5],
            "query4_rows": len(query4),
            "query4_sample": query4,
            "query5_rows": len(query5),
            "query5_sample": query5[:5],
        }
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Gold into AstraDB via foreachBatch"
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
    configure_spark_performance(spark)
    spark.sparkContext.setLogLevel("WARN")

    if not is_astra_configured():
        print(
            "AstraDB credentials missing. Set ASTRA_DB_APPLICATION_TOKEN and "
            "ASTRA_DB_SECURE_BUNDLE_PATH (see .env.example)."
        )
        spark.stop()
        raise SystemExit(1)

    try:
        result = run_serving(spark, skip_load=args.skip_load)
        if result["load"]:
            for load_result in result["load"]:
                print(f"LOAD {load_result['table_name']}: {load_result}")
        print(f"QUERY #1 rows: {result['query1_rows']}")
        print("QUERY #1 sample:", result["query1_sample"])
        print("QUERY #2 top:", result["query2_top"])
        print(f"QUERY #3 rows: {result['query3_rows']}")
        print("QUERY #3 sample:", result["query3_sample"])
        print(f"QUERY #4 rows: {result['query4_rows']}")
        print("QUERY #4 sample:", result["query4_sample"])
        print(f"QUERY #5 rows: {result['query5_rows']}")
        print("QUERY #5 sample:", result["query5_sample"])
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
