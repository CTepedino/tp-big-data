"""Gold job: Silver -> business marts."""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import GOLD, SILVER, SPARK_TARGET_FILES_GOLD
from src.spark.performance import configure_spark_performance, write_partitioned_parquet

TOP_SERVICES_LOOKBACK_DAYS = 14
TOP_SERVICES_TOP_N = 5

USAGE_EVENTS_SILVER = os.path.join(SILVER, "usage_events")
ORG_SERVICE_DAILY_SILVER = os.path.join(SILVER, "org_service_daily")
BILLING_MONTHLY_SILVER = os.path.join(SILVER, "billing_monthly")
CUSTOMERS_ORGS_SILVER = os.path.join(SILVER, "customers_orgs")
SUPPORT_TICKETS_SILVER = os.path.join(SILVER, "support_tickets")
MARKETING_TOUCHES_SILVER = os.path.join(SILVER, "marketing_touches")
NPS_SURVEYS_SILVER = os.path.join(SILVER, "nps_surveys")

ORG_DAILY_USAGE_BY_SERVICE = os.path.join(GOLD, "org_daily_usage_by_service")
ORG_TOP_SERVICES_BY_COST = os.path.join(GOLD, "org_top_services_by_cost")
REVENUE_BY_ORG_MONTH = os.path.join(GOLD, "revenue_by_org_month")
COST_ANOMALY_MART = os.path.join(GOLD, "cost_anomaly_mart")
TICKETS_BY_ORG_DATE = os.path.join(GOLD, "tickets_by_org_date")
GENAI_TOKENS_BY_ORG_DATE = os.path.join(GOLD, "genai_tokens_by_org_date")
NPS_BY_ORG_DATE = os.path.join(GOLD, "nps_by_org_date")
MARKETING_TOUCHES_BY_ORG_CHANNEL = os.path.join(
    GOLD, "marketing_touches_by_org_channel"
)


def _write_gold_mart(df: DataFrame, gold_path: str, partition_col: str) -> DataFrame:
    write_partitioned_parquet(
        df,
        gold_path,
        partition_cols=[partition_col],
        repartition_cols=[partition_col],
        num_files=SPARK_TARGET_FILES_GOLD,
    )
    return df.sparkSession.read.parquet(gold_path)


def _mart_result(
    mart_name: str,
    gold_path: str,
    gold_df: DataFrame,
    written_df: DataFrame,
    grain_columns: list[str],
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gold_count = gold_df.count()
    distinct_grain = gold_df.select(*grain_columns).distinct().count()
    result = {
        "mart_name": mart_name,
        "gold_path": gold_path,
        "gold_row_count": gold_count,
        "written_count": written_df.count(),
        "distinct_grain": distinct_grain,
        "grain_unique": gold_count == distinct_grain,
    }
    if extra:
        result.update(extra)
    return result


def build_org_daily_usage_by_service(daily_df: DataFrame) -> DataFrame:
    return (
        daily_df.select(
            "org_id",
            "usage_date",
            "service",
            "org_name",
            "org_industry",
            "org_plan_tier",
            "event_count",
            F.col("daily_cost_usd").alias("total_daily_cost_usd"),
            F.col("requests").alias("total_requests"),
            F.col("cpu_hours").alias("total_cpu_hours"),
            F.col("storage_gb_hours").alias("total_storage_gb_hours"),
            F.col("genai_tokens").alias("total_genai_tokens"),
            F.col("carbon_kg").alias("total_carbon_kg"),
            "anomaly_event_count",
            "has_cost_anomaly",
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_revenue_by_org_month(
    billing_df: DataFrame, customers_df: DataFrame
) -> DataFrame:
    customers = customers_df.select(
        "org_id",
        F.col("org_name").alias("org_name"),
        F.col("plan_tier").alias("org_plan_tier"),
    )

    invoices = (
        billing_df.join(customers, on="org_id", how="left")
        .withColumn(
            "subtotal_usd",
            F.col("subtotal") * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)),
        )
        .withColumn(
            "credits_usd",
            F.coalesce(F.col("credits"), F.lit(0.0))
            * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)),
        )
        .withColumn(
            "taxes_usd",
            F.coalesce(F.col("taxes"), F.lit(0.0))
            * F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)),
        )
        .withColumn(
            "revenue_usd",
            F.col("subtotal_usd") - F.col("credits_usd") + F.col("taxes_usd"),
        )
        .withColumn("billing_month", F.col("month"))
    )

    return (
        invoices.groupBy("org_id", "billing_month")
        .agg(
            F.first("org_name").alias("org_name"),
            F.first("org_plan_tier").alias("org_plan_tier"),
            F.sum("subtotal_usd").alias("subtotal_usd"),
            F.sum("credits_usd").alias("credits_usd"),
            F.sum("taxes_usd").alias("taxes_usd"),
            F.sum("revenue_usd").alias("revenue_usd"),
            F.count("*").alias("invoice_count"),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_cost_anomaly_mart(events_df: DataFrame) -> DataFrame:
    return (
        events_df.groupBy("org_id", "usage_date", "service")
        .agg(
            F.first("org_name").alias("org_name"),
            F.count("*").alias("event_count"),
            F.sum("cost_usd_increment").alias("total_daily_cost_usd"),
            F.sum(F.when(F.col("is_cost_anomaly"), 1).otherwise(0)).alias(
                "anomaly_event_count"
            ),
            F.sum(
                F.when(F.col("is_cost_anomaly"), F.col("cost_usd_increment")).otherwise(
                    F.lit(0.0)
                )
            ).alias("anomaly_cost_usd"),
            F.sum(F.when(F.col("is_cost_anomaly_zscore"), 1).otherwise(0)).alias(
                "zscore_anomaly_count"
            ),
            F.sum(F.when(F.col("is_cost_anomaly_mad"), 1).otherwise(0)).alias(
                "mad_anomaly_count"
            ),
            F.sum(
                F.when(F.col("is_cost_anomaly_percentile"), 1).otherwise(0)
            ).alias("percentile_anomaly_count"),
        )
        .withColumn(
            "anomaly_score",
            F.when(
                F.col("event_count") > 0,
                F.col("anomaly_event_count") / F.col("event_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "has_cost_anomaly",
            F.col("anomaly_event_count") > 0,
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_tickets_by_org_date(
    tickets_df: DataFrame, customers_df: DataFrame
) -> DataFrame:
    customers = customers_df.select("org_id", F.col("org_name").alias("org_name"))

    return (
        tickets_df.join(customers, on="org_id", how="left")
        .withColumn("ticket_date", F.col("created_at"))
        .groupBy("org_id", "ticket_date", "severity")
        .agg(
            F.first("org_name").alias("org_name"),
            F.count("*").alias("ticket_count"),
            F.sum(F.when(F.col("sla_breached"), 1).otherwise(0)).alias(
                "sla_breach_count"
            ),
            F.avg("csat").alias("avg_csat"),
        )
        .withColumn(
            "sla_breach_rate",
            F.when(
                F.col("ticket_count") > 0,
                F.col("sla_breach_count") / F.col("ticket_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_genai_tokens_by_org_date(events_df: DataFrame) -> DataFrame:
    genai_events = events_df.filter(
        (F.col("service") == "genai") | (F.col("genai_tokens") > 0)
    )

    return (
        genai_events.groupBy("org_id", "usage_date")
        .agg(
            F.first("org_name").alias("org_name"),
            F.sum("genai_tokens").alias("total_genai_tokens"),
            F.sum("cost_usd_increment").alias("estimated_cost_usd"),
            F.count("*").alias("event_count"),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_nps_by_org_date(
    nps_df: DataFrame, customers_df: DataFrame
) -> DataFrame:
    customers = customers_df.select("org_id", F.col("org_name").alias("org_name"))

    return (
        nps_df.join(customers, on="org_id", how="left")
        .groupBy("org_id", "survey_date")
        .agg(
            F.first("org_name").alias("org_name"),
            F.avg("nps_score").alias("avg_nps_score"),
            F.count("*").alias("survey_count"),
            F.sum(F.when(F.col("nps_score").isNotNull(), 1).otherwise(0)).alias(
                "scored_count"
            ),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_marketing_touches_by_org_channel(
    touches_df: DataFrame, customers_df: DataFrame
) -> DataFrame:
    customers = customers_df.select("org_id", F.col("org_name").alias("org_name"))

    return (
        touches_df.withColumn("touch_date", F.to_date("timestamp"))
        .join(customers, on="org_id", how="left")
        .groupBy("org_id", "touch_date", "channel")
        .agg(
            F.first("org_name").alias("org_name"),
            F.count("*").alias("touch_count"),
            F.sum(F.when(F.col("clicked"), 1).otherwise(0)).alias("click_count"),
            F.sum(F.when(F.col("converted"), 1).otherwise(0)).alias(
                "conversion_count"
            ),
        )
        .withColumn(
            "click_rate",
            F.when(
                F.col("touch_count") > 0,
                F.col("click_count") / F.col("touch_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "conversion_rate",
            F.when(
                F.col("touch_count") > 0,
                F.col("conversion_count") / F.col("touch_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def build_org_top_services_by_cost(
    org_daily_df: DataFrame,
    *,
    lookback_days: int = TOP_SERVICES_LOOKBACK_DAYS,
    top_n: int = TOP_SERVICES_TOP_N,
) -> DataFrame:
    """Top-N services by cost over the last N calendar days ending at max(usage_date)."""
    period_end = org_daily_df.agg(F.max("usage_date")).collect()[0][0]
    if period_end is None:
        return org_daily_df.limit(0)

    if isinstance(period_end, date):
        period_start = period_end - timedelta(days=lookback_days - 1)
    else:
        period_start = (
            org_daily_df.agg(
                F.date_sub(F.max("usage_date"), lookback_days - 1)
            ).collect()[0][0]
        )

    in_window = org_daily_df.filter(
        (F.col("usage_date") >= F.lit(period_start))
        & (F.col("usage_date") <= F.lit(period_end))
    )
    ranked = (
        in_window.groupBy("org_id", "service")
        .agg(F.sum("total_daily_cost_usd").alias("accumulated_cost_usd"))
    )
    rank_window = Window.partitionBy("org_id").orderBy(F.desc("accumulated_cost_usd"))
    return (
        ranked.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= top_n)
        .withColumn("period_end", F.lit(period_end))
        .withColumn("period_start", F.lit(period_start))
        .withColumn("gold_ts", F.current_timestamp())
    )


def process_org_daily_usage_by_service(spark: SparkSession) -> dict[str, Any]:
    daily_df = spark.read.parquet(ORG_SERVICE_DAILY_SILVER)
    silver_count = daily_df.count()

    gold_df = build_org_daily_usage_by_service(daily_df)
    written_df = _write_gold_mart(gold_df, ORG_DAILY_USAGE_BY_SERVICE, "usage_date")

    silver_cost = daily_df.agg(F.sum("daily_cost_usd")).collect()[0][0]
    gold_cost = written_df.agg(F.sum("total_daily_cost_usd")).collect()[0][0]

    return _mart_result(
        "org_daily_usage_by_service",
        ORG_DAILY_USAGE_BY_SERVICE,
        gold_df,
        written_df,
        ["org_id", "usage_date", "service"],
        extra={
            "silver_daily_row_count": silver_count,
            "silver_total_cost_usd": float(silver_cost or 0),
            "gold_total_cost_usd": float(gold_cost or 0),
            "cost_balance_ok": abs((silver_cost or 0) - (gold_cost or 0)) < 0.01,
        },
    )


def process_org_top_services_by_cost(spark: SparkSession) -> dict[str, Any]:
    org_daily_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    gold_df = build_org_top_services_by_cost(org_daily_df)
    written_df = _write_gold_mart(gold_df, ORG_TOP_SERVICES_BY_COST, "period_end")

    period_meta = gold_df.agg(
        F.min("period_start").alias("period_start"),
        F.max("period_end").alias("period_end"),
    ).collect()[0]

    return _mart_result(
        "org_top_services_by_cost",
        ORG_TOP_SERVICES_BY_COST,
        gold_df,
        written_df,
        ["org_id", "period_end", "rank", "service"],
        extra={
            "lookback_days": TOP_SERVICES_LOOKBACK_DAYS,
            "top_n": TOP_SERVICES_TOP_N,
            "period_start": str(period_meta["period_start"]),
            "period_end": str(period_meta["period_end"]),
        },
    )


def process_revenue_by_org_month(spark: SparkSession) -> dict[str, Any]:
    billing_df = spark.read.parquet(BILLING_MONTHLY_SILVER)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)
    silver_invoice_count = billing_df.count()

    gold_df = build_revenue_by_org_month(billing_df, customers_df)
    written_df = _write_gold_mart(gold_df, REVENUE_BY_ORG_MONTH, "billing_month")

    gold_revenue = gold_df.agg(F.sum("revenue_usd")).collect()[0][0]
    gold_invoice_count = (
        gold_df.agg(F.sum("invoice_count")).collect()[0][0] or 0
    )

    return _mart_result(
        "revenue_by_org_month",
        REVENUE_BY_ORG_MONTH,
        gold_df,
        written_df,
        ["org_id", "billing_month"],
        extra={
            "silver_invoice_count": silver_invoice_count,
            "gold_invoice_count": int(gold_invoice_count),
            "gold_total_revenue_usd": float(gold_revenue or 0),
            "invoice_balance_ok": int(gold_invoice_count) == silver_invoice_count,
        },
    )


def process_cost_anomaly_mart(spark: SparkSession) -> dict[str, Any]:
    events_df = spark.read.parquet(USAGE_EVENTS_SILVER)
    gold_df = build_cost_anomaly_mart(events_df)
    written_df = _write_gold_mart(gold_df, COST_ANOMALY_MART, "usage_date")

    flagged_rows = gold_df.filter(F.col("has_cost_anomaly")).count()
    return _mart_result(
        "cost_anomaly_mart",
        COST_ANOMALY_MART,
        gold_df,
        written_df,
        ["org_id", "usage_date", "service"],
        extra={"anomaly_rows": flagged_rows},
    )


def process_tickets_by_org_date(spark: SparkSession) -> dict[str, Any]:
    tickets_df = spark.read.parquet(SUPPORT_TICKETS_SILVER)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)
    silver_count = tickets_df.count()

    gold_df = build_tickets_by_org_date(tickets_df, customers_df)
    written_df = _write_gold_mart(gold_df, TICKETS_BY_ORG_DATE, "ticket_date")

    gold_ticket_count = gold_df.agg(F.sum("ticket_count")).collect()[0][0]
    return _mart_result(
        "tickets_by_org_date",
        TICKETS_BY_ORG_DATE,
        gold_df,
        written_df,
        ["org_id", "ticket_date", "severity"],
        extra={
            "silver_ticket_count": silver_count,
            "gold_ticket_count": int(gold_ticket_count or 0),
            "ticket_balance_ok": silver_count == gold_ticket_count,
        },
    )


def process_genai_tokens_by_org_date(spark: SparkSession) -> dict[str, Any]:
    events_df = spark.read.parquet(USAGE_EVENTS_SILVER)
    gold_df = build_genai_tokens_by_org_date(events_df)
    written_df = _write_gold_mart(gold_df, GENAI_TOKENS_BY_ORG_DATE, "usage_date")

    silver_genai_tokens = (
        events_df.filter((F.col("service") == "genai") | (F.col("genai_tokens") > 0))
        .agg(F.sum("genai_tokens"))
        .collect()[0][0]
    )
    gold_genai_tokens = written_df.agg(F.sum("total_genai_tokens")).collect()[0][0]

    return _mart_result(
        "genai_tokens_by_org_date",
        GENAI_TOKENS_BY_ORG_DATE,
        gold_df,
        written_df,
        ["org_id", "usage_date"],
        extra={
            "silver_genai_tokens": int(silver_genai_tokens or 0),
            "gold_genai_tokens": int(gold_genai_tokens or 0),
            "token_balance_ok": silver_genai_tokens == gold_genai_tokens,
        },
    )


def process_nps_by_org_date(spark: SparkSession) -> dict[str, Any]:
    nps_df = spark.read.parquet(NPS_SURVEYS_SILVER)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)
    silver_count = nps_df.count()

    gold_df = build_nps_by_org_date(nps_df, customers_df)
    written_df = _write_gold_mart(gold_df, NPS_BY_ORG_DATE, "survey_date")

    gold_survey_count = written_df.agg(F.sum("survey_count")).collect()[0][0] or 0

    return _mart_result(
        "nps_by_org_date",
        NPS_BY_ORG_DATE,
        gold_df,
        written_df,
        ["org_id", "survey_date"],
        extra={
            "silver_survey_count": silver_count,
            "gold_survey_count": int(gold_survey_count),
            "survey_balance_ok": int(gold_survey_count) == silver_count,
        },
    )


def process_marketing_touches_by_org_channel(spark: SparkSession) -> dict[str, Any]:
    touches_df = spark.read.parquet(MARKETING_TOUCHES_SILVER)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)
    silver_count = touches_df.count()

    gold_df = build_marketing_touches_by_org_channel(touches_df, customers_df)
    written_df = _write_gold_mart(
        gold_df, MARKETING_TOUCHES_BY_ORG_CHANNEL, "touch_date"
    )

    gold_touch_count = written_df.agg(F.sum("touch_count")).collect()[0][0] or 0

    return _mart_result(
        "marketing_touches_by_org_channel",
        MARKETING_TOUCHES_BY_ORG_CHANNEL,
        gold_df,
        written_df,
        ["org_id", "touch_date", "channel"],
        extra={
            "silver_touch_count": silver_count,
            "gold_touch_count": int(gold_touch_count),
            "touch_balance_ok": int(gold_touch_count) == silver_count,
        },
    )


GOLD_MART_PROCESSORS: list[Callable[[SparkSession], dict[str, Any]]] = [
    process_org_daily_usage_by_service,
    process_org_top_services_by_cost,
    process_revenue_by_org_month,
    process_cost_anomaly_mart,
    process_tickets_by_org_date,
    process_genai_tokens_by_org_date,
    process_nps_by_org_date,
    process_marketing_touches_by_org_channel,
]


def run_gold(spark: SparkSession) -> list[dict[str, Any]]:
    return [processor(spark) for processor in GOLD_MART_PROCESSORS]


def validate_gold(spark: SparkSession) -> dict[str, Any]:
    org_service_daily_df = spark.read.parquet(ORG_SERVICE_DAILY_SILVER)
    silver_cost = (
        org_service_daily_df.agg(F.sum("daily_cost_usd")).collect()[0][0] or 0
    )

    org_daily_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    org_daily_rows = org_daily_df.count()
    org_daily_grain = (
        org_daily_df.select("org_id", "usage_date", "service").distinct().count()
    )
    org_daily_cost = (
        org_daily_df.agg(F.sum("total_daily_cost_usd")).collect()[0][0] or 0
    )

    top_services_df = spark.read.parquet(ORG_TOP_SERVICES_BY_COST)
    top_services_rows = top_services_df.count()
    top_services_grain = (
        top_services_df.select("org_id", "period_end", "rank", "service").distinct().count()
    )

    revenue_df = spark.read.parquet(REVENUE_BY_ORG_MONTH)
    revenue_rows = revenue_df.count()
    revenue_grain = (
        revenue_df.select("org_id", "billing_month").distinct().count()
    )

    anomaly_df = spark.read.parquet(COST_ANOMALY_MART)
    anomaly_rows = anomaly_df.count()
    anomaly_grain = (
        anomaly_df.select("org_id", "usage_date", "service").distinct().count()
    )

    tickets_df = spark.read.parquet(TICKETS_BY_ORG_DATE)
    tickets_rows = tickets_df.count()
    tickets_grain = (
        tickets_df.select("org_id", "ticket_date", "severity").distinct().count()
    )
    silver_tickets = spark.read.parquet(SUPPORT_TICKETS_SILVER).count()
    gold_tickets = tickets_df.agg(F.sum("ticket_count")).collect()[0][0] or 0

    genai_df = spark.read.parquet(GENAI_TOKENS_BY_ORG_DATE)
    genai_rows = genai_df.count()
    genai_grain = genai_df.select("org_id", "usage_date").distinct().count()

    nps_df = spark.read.parquet(NPS_BY_ORG_DATE)
    nps_rows = nps_df.count()
    nps_grain = nps_df.select("org_id", "survey_date").distinct().count()
    silver_nps = spark.read.parquet(NPS_SURVEYS_SILVER).count()
    gold_nps = nps_df.agg(F.sum("survey_count")).collect()[0][0] or 0

    marketing_df = spark.read.parquet(MARKETING_TOUCHES_BY_ORG_CHANNEL)
    marketing_rows = marketing_df.count()
    marketing_grain = (
        marketing_df.select("org_id", "touch_date", "channel").distinct().count()
    )
    silver_touches = spark.read.parquet(MARKETING_TOUCHES_SILVER).count()
    gold_touches = marketing_df.agg(F.sum("touch_count")).collect()[0][0] or 0

    required_org_daily_metrics = {
        "total_daily_cost_usd",
        "total_requests",
        "total_cpu_hours",
        "total_storage_gb_hours",
        "total_genai_tokens",
        "total_carbon_kg",
        "event_count",
    }
    missing_org_daily = sorted(
        required_org_daily_metrics - set(org_daily_df.columns)
    )

    return {
        "org_daily_usage_by_service": {
            "gold_row_count": org_daily_rows,
            "distinct_grain": org_daily_grain,
            "grain_unique": org_daily_rows == org_daily_grain,
            "missing_metrics": missing_org_daily,
            "metrics_ok": len(missing_org_daily) == 0,
            "silver_total_cost_usd": float(silver_cost),
            "gold_total_cost_usd": float(org_daily_cost),
            "cost_balance_ok": abs(silver_cost - org_daily_cost) < 0.01,
        },
        "org_top_services_by_cost": {
            "gold_row_count": top_services_rows,
            "distinct_grain": top_services_grain,
            "grain_unique": top_services_rows == top_services_grain,
        },
        "revenue_by_org_month": {
            "gold_row_count": revenue_rows,
            "distinct_grain": revenue_grain,
            "grain_unique": revenue_rows == revenue_grain,
        },
        "cost_anomaly_mart": {
            "gold_row_count": anomaly_rows,
            "distinct_grain": anomaly_grain,
            "grain_unique": anomaly_rows == anomaly_grain,
        },
        "tickets_by_org_date": {
            "gold_row_count": tickets_rows,
            "distinct_grain": tickets_grain,
            "grain_unique": tickets_rows == tickets_grain,
            "ticket_balance_ok": silver_tickets == gold_tickets,
        },
        "genai_tokens_by_org_date": {
            "gold_row_count": genai_rows,
            "distinct_grain": genai_grain,
            "grain_unique": genai_rows == genai_grain,
        },
        "nps_by_org_date": {
            "gold_row_count": nps_rows,
            "distinct_grain": nps_grain,
            "grain_unique": nps_rows == nps_grain,
            "survey_balance_ok": int(gold_nps) == silver_nps,
        },
        "marketing_touches_by_org_channel": {
            "gold_row_count": marketing_rows,
            "distinct_grain": marketing_grain,
            "grain_unique": marketing_rows == marketing_grain,
            "touch_balance_ok": int(gold_touches) == silver_touches,
        },
    }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("gold")
        .master("local[*]")
        .getOrCreate()
    )
    configure_spark_performance(spark)
    spark.sparkContext.setLogLevel("WARN")

    for result in run_gold(spark):
        print(f"\n=== {result['mart_name']} ===")
        for key, value in result.items():
            print(f"  {key}: {value}")

    validation = validate_gold(spark)
    all_ok = all(
        mart.get("grain_unique", False)
        and mart.get("metrics_ok", True)
        and mart.get("cost_balance_ok", True)
        and mart.get("ticket_balance_ok", True)
        for mart in validation.values()
    )
    status = "OK" if all_ok else "FAIL"
    print(f"\nvalidation [{status}]: {validation}")

    spark.stop()
