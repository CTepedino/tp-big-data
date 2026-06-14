"""Load Gold Parquet into AstraDB via foreachBatch."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

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
from src.cassandra.inserts import (
    INSERT_GENAI,
    INSERT_ORG_DAILY,
    INSERT_REVENUE,
    INSERT_TICKETS,
    INSERT_TOP_SERVICES,
)
from src.cassandra.schema import (
    TABLE_GENAI,
    TABLE_ORG_DAILY,
    TABLE_REVENUE,
    TABLE_TICKETS,
    TABLE_TOP_SERVICES,
    TOP_SERVICES_LOOKBACK_DAYS,
)
from src.config import CASSANDRA_KEYSPACE
from src.jobs.gold import (
    GENAI_TOKENS_BY_ORG_DATE,
    ORG_DAILY_USAGE_BY_SERVICE,
    ORG_TOP_SERVICES_BY_COST,
    REVENUE_BY_ORG_MONTH,
    TICKETS_BY_ORG_DATE,
)
from src.spark.performance import configure_spark_performance

CQL_CREATE_TABLES = "00_create_tables.cql"


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


def _clear_top_services_partitions(session, top_df: DataFrame) -> int:
    """Delete Cassandra partitions before reload to avoid stale ranks."""
    pairs = top_df.select("org_id", "period_end").distinct().collect()
    for row in pairs:
        session.execute(
            f"DELETE FROM {TABLE_TOP_SERVICES} "
            "WHERE org_id = %s AND period_end = %s",
            (row["org_id"], _to_date(row["period_end"])),
        )
    return len(pairs)


def _top_services_params(row: dict[str, Any]) -> tuple:
    return (
        row["org_id"],
        _to_date(row["period_end"]),
        int(row["rank"]),
        row["service"],
        _to_date(row["period_start"]),
        float(row["accumulated_cost_usd"]),
    )


def load_org_top_services_by_cost(spark: SparkSession, session) -> dict[str, Any]:
    top_df = spark.read.parquet(ORG_TOP_SERVICES_BY_COST)
    deleted_partitions = _clear_top_services_partitions(session, top_df)
    result = load_dataframe_via_foreach_batch(
        spark,
        top_df,
        TABLE_TOP_SERVICES,
        insert_cql=INSERT_TOP_SERVICES,
        row_to_params=_top_services_params,
        session=session,
    )
    result["gold_path"] = ORG_TOP_SERVICES_BY_COST
    result["deleted_partitions"] = deleted_partitions
    result["lookback_days"] = TOP_SERVICES_LOOKBACK_DAYS
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


def run_serving(
    spark: SparkSession,
    *,
    setup_ddl: bool = True,
    skip_load: bool = False,
) -> dict[str, Any]:
    """DDL + Gold load only. Demo queries live in src.cassandra.demo (notebook section 7)."""
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

        return {"load": load_results}
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Gold into AstraDB via foreachBatch"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Run DDL only; skip Gold load",
    )
    parser.add_argument(
        "--demo-queries",
        action="store_true",
        help="After load, run demo CQL selects (see src.cassandra.demo)",
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

        if args.demo_queries:
            from src.cassandra.demo import close_demo_session, open_demo_session, run_cql_select
            from src.cassandra.selects import (
                critical_tickets_sla,
                daily_costs_and_requests,
                genai_tokens_daily,
                monthly_revenue,
                top_services_by_cost,
            )

            demo = open_demo_session(spark)
            if demo is None:
                print("Demo queries skipped (Astra not configured).")
            else:
                p = demo.params
                print(f"DEMO org_id={p.org_id}")
                for label, query in [
                    ("#1", daily_costs_and_requests(p.org_id, p.q1_start, p.q1_end)),
                    ("#2", top_services_by_cost(p.org_id, p.period_end, p.top_n)),
                    ("#3", critical_tickets_sla(p.org_id, p.q3_severity, p.q3_start, p.q3_end)),
                    ("#4", monthly_revenue(p.org_id, p.q4_start, p.q4_end)),
                    ("#5", genai_tokens_daily(p.org_id, p.q5_start, p.q5_end)),
                ]:
                    rows = run_cql_select(demo.session, query)
                    print(f"QUERY {label}: {len(rows)} rows")
                close_demo_session(demo)
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
