"""Gold job: Silver -> business marts."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import GOLD, SILVER

USAGE_EVENTS_SILVER = os.path.join(SILVER, "usage_events")
ORG_DAILY_USAGE_BY_SERVICE = os.path.join(GOLD, "org_daily_usage_by_service")


def build_org_daily_usage_by_service(events_df: DataFrame) -> DataFrame:
    return (
        events_df.groupBy("org_id", "usage_date", "service")
        .agg(
            F.first("org_name").alias("org_name"),
            F.first("org_industry").alias("org_industry"),
            F.first("org_plan_tier").alias("org_plan_tier"),
            F.count("*").alias("event_count"),
            F.sum("daily_cost_usd").alias("total_daily_cost_usd"),
            F.sum("requests").alias("total_requests"),
            F.sum("genai_tokens").alias("total_genai_tokens"),
            F.sum("carbon_kg").alias("total_carbon_kg"),
            F.sum(F.when(F.col("is_cost_anomaly"), 1).otherwise(0)).alias(
                "anomaly_event_count"
            ),
            F.max(F.col("is_cost_anomaly").cast("int")).alias("has_cost_anomaly"),
        )
        .withColumn("gold_ts", F.current_timestamp())
    )


def process_org_daily_usage_by_service(spark: SparkSession) -> dict[str, Any]:
    events_df = spark.read.parquet(USAGE_EVENTS_SILVER)
    silver_count = events_df.count()

    gold_df = build_org_daily_usage_by_service(events_df)
    gold_count = gold_df.count()
    distinct_grain = gold_df.select("org_id", "usage_date", "service").distinct().count()

    os.makedirs(ORG_DAILY_USAGE_BY_SERVICE, exist_ok=True)
    (
        gold_df.write.mode("overwrite")
        .partitionBy("usage_date")
        .parquet(ORG_DAILY_USAGE_BY_SERVICE)
    )

    written_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    silver_cost = events_df.agg(F.sum("daily_cost_usd")).collect()[0][0]
    gold_cost = written_df.agg(F.sum("total_daily_cost_usd")).collect()[0][0]

    return {
        "mart_name": "org_daily_usage_by_service",
        "silver_path": USAGE_EVENTS_SILVER,
        "gold_path": ORG_DAILY_USAGE_BY_SERVICE,
        "silver_event_count": silver_count,
        "gold_row_count": gold_count,
        "written_count": written_df.count(),
        "distinct_grain": distinct_grain,
        "grain_unique": gold_count == distinct_grain,
        "silver_total_cost_usd": float(silver_cost or 0),
        "gold_total_cost_usd": float(gold_cost or 0),
        "cost_balance_ok": abs((silver_cost or 0) - (gold_cost or 0)) < 0.01,
    }


def run_gold(spark: SparkSession) -> list[dict[str, Any]]:
    return [process_org_daily_usage_by_service(spark)]


def validate_gold(spark: SparkSession) -> dict[str, Any]:
    gold_df = spark.read.parquet(ORG_DAILY_USAGE_BY_SERVICE)
    events_df = spark.read.parquet(USAGE_EVENTS_SILVER)

    total_rows = gold_df.count()
    distinct_grain = gold_df.select("org_id", "usage_date", "service").distinct().count()

    required_metrics = {
        "total_daily_cost_usd",
        "total_requests",
        "total_genai_tokens",
        "total_carbon_kg",
        "event_count",
    }
    missing_metrics = sorted(required_metrics - set(gold_df.columns))

    silver_cost = events_df.agg(F.sum("daily_cost_usd")).collect()[0][0] or 0
    gold_cost = gold_df.agg(F.sum("total_daily_cost_usd")).collect()[0][0] or 0

    return {
        "gold_row_count": total_rows,
        "distinct_grain": distinct_grain,
        "grain_unique": total_rows == distinct_grain,
        "missing_metrics": missing_metrics,
        "metrics_ok": len(missing_metrics) == 0,
        "silver_total_cost_usd": float(silver_cost),
        "gold_total_cost_usd": float(gold_cost),
        "cost_balance_ok": abs(silver_cost - gold_cost) < 0.01,
    }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("gold")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for result in run_gold(spark):
        print(f"\n=== {result['mart_name']} ===")
        for key, value in result.items():
            print(f"  {key}: {value}")

    validation = validate_gold(spark)
    status = (
        "OK"
        if validation["grain_unique"]
        and validation["metrics_ok"]
        and validation["cost_balance_ok"]
        else "FAIL"
    )
    print(f"\nvalidation [{status}]: {validation}")

    spark.stop()
