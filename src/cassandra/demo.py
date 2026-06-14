"""Demo helpers: resolve parameters and run CQL selects for notebook / CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cassandra.client import get_cassandra_session, is_astra_configured
from src.cassandra.schema import DEFAULT_TOP_N, TOP_SERVICES_LOOKBACK_DAYS
from src.cassandra.selects import CqlSelect
from src.config import CASSANDRA_KEYSPACE
from src.jobs.gold import ORG_DAILY_USAGE_BY_SERVICE, ORG_TOP_SERVICES_BY_COST


@dataclass
class DemoQueryParams:
    org_id: str
    top_n: int
    period_end: str
    period_start: str
    q1_start: str = "2025-07-01"
    q1_end: str = "2025-08-31"
    q3_start: str = "2025-08-01"
    q3_end: str = "2025-08-31"
    q3_severity: str = "high"
    q4_start: str = "2025-06-01"
    q4_end: str = "2025-08-01"
    q5_start: str = "2025-08-01"
    q5_end: str = "2025-08-31"


@dataclass
class DemoSession:
    session: Any
    cluster: Any
    params: DemoQueryParams


def resolve_demo_org_id(spark: SparkSession) -> str:
    top_org = (
        spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
        .groupBy("org_id")
        .agg(F.sum("total_daily_cost_usd").alias("cost"))
        .orderBy(F.desc("cost"))
        .first()
    )
    return top_org["org_id"] if top_org else "org_rixa11dp"


def resolve_top_services_period(
    spark: SparkSession,
    org_id: str,
) -> tuple[str, str]:
    top_meta = (
        spark.read.parquet(ORG_TOP_SERVICES_BY_COST)
        .filter(F.col("org_id") == org_id)
        .agg(
            F.max("period_end").alias("period_end"),
            F.max("period_start").alias("period_start"),
        )
        .first()
    )
    if top_meta and top_meta["period_end"] is not None:
        return str(top_meta["period_start"]), str(top_meta["period_end"])

    max_usage = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE).agg(
        F.max("usage_date")
    ).collect()[0][0]
    period_end = str(max_usage)
    period_start = str(
        spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
        .agg(F.date_sub(F.max("usage_date"), TOP_SERVICES_LOOKBACK_DAYS - 1))
        .collect()[0][0]
    )
    return period_start, period_end


def build_demo_params(spark: SparkSession, org_id: str | None = None) -> DemoQueryParams:
    resolved_org = org_id or resolve_demo_org_id(spark)
    period_start, period_end = resolve_top_services_period(spark, resolved_org)
    return DemoQueryParams(
        org_id=resolved_org,
        top_n=DEFAULT_TOP_N,
        period_start=period_start,
        period_end=period_end,
    )


def open_demo_session(spark: SparkSession, org_id: str | None = None) -> DemoSession | None:
    if not is_astra_configured():
        return None
    session, cluster = get_cassandra_session()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return DemoSession(
        session=session,
        cluster=cluster,
        params=build_demo_params(spark, org_id=org_id),
    )


def close_demo_session(demo: DemoSession | None) -> None:
    if demo is not None:
        demo.cluster.shutdown()


def run_cql_select(session, query: CqlSelect) -> list[dict[str, Any]]:
    print(query.literal)
    print()
    return query.execute(session)


def display_cql_select(session, query: CqlSelect) -> Any:
    """Print literal CQL, execute, return a pandas DataFrame (notebook helper)."""
    import pandas as pd

    rows = run_cql_select(session, query)
    df = pd.DataFrame(rows)
    try:
        from IPython.display import display

        display(df)
    except ImportError:
        print(df)
    print(f"({len(df)} filas)")
    return df


def run_all_demo_queries(demo: DemoSession) -> None:
    """Run consultas #1–#5 (notebook §7)."""
    from src.cassandra.selects import (
        critical_tickets_sla,
        daily_costs_and_requests,
        genai_tokens_daily,
        monthly_revenue,
        top_services_by_cost,
    )

    p = demo.params
    queries = [
        ("#1 — costos y requests diarios", daily_costs_and_requests(p.org_id, p.q1_start, p.q1_end)),
        (
            "#2 — top servicios (rolling 14d)",
            top_services_by_cost(p.org_id, p.period_end, p.top_n),
        ),
        (
            "#3 — tickets críticos + SLA",
            critical_tickets_sla(p.org_id, p.q3_severity, p.q3_start, p.q3_end),
        ),
        ("#4 — revenue mensual", monthly_revenue(p.org_id, p.q4_start, p.q4_end)),
        ("#5 — tokens GenAI", genai_tokens_daily(p.org_id, p.q5_start, p.q5_end)),
    ]
    for title, query in queries:
        print(f"=== Consulta {title} ===")
        display_cql_select(demo.session, query)
        print()
