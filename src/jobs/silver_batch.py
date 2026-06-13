"""Job Silver: Bronze -> Silver + Quarantine."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import BRONZE, QUARANTINE, SILVER
from src.schemas.silver import COST_ANOMALY_THRESHOLD, QUARANTINE_REASONS

CUSTOMERS_ORGS_BRONZE = os.path.join(BRONZE, "customers_orgs")
USAGE_EVENTS_BRONZE = os.path.join(BRONZE, "usage_events")

CUSTOMERS_ORGS_SILVER = os.path.join(SILVER, "customers_orgs")
USAGE_EVENTS_SILVER = os.path.join(SILVER, "usage_events")
USAGE_EVENTS_QUARANTINE = os.path.join(QUARANTINE, "silver", "usage_events")


def _add_quarantine_metadata(df: DataFrame, error_reason: str) -> DataFrame:
    return df.withColumn("error_reason", F.lit(error_reason)).withColumn(
        "quarantine_ts", F.current_timestamp()
    )


def process_customers_orgs_silver(spark: SparkSession) -> dict[str, Any]:
    """Conformance ligero del maestro customers_orgs."""
    df = spark.read.parquet(CUSTOMERS_ORGS_BRONZE)

    silver_df = (
        df.withColumn("org_name", F.trim(F.col("org_name")))
        .withColumn("industry", F.trim(F.col("industry")))
        .withColumn("hq_region", F.trim(F.col("hq_region")))
        .withColumn("plan_tier", F.trim(F.col("plan_tier")))
        .withColumn("lifecycle_stage", F.trim(F.col("lifecycle_stage")))
        .withColumn("marketing_source", F.trim(F.col("marketing_source")))
        .withColumn("silver_ts", F.current_timestamp())
    )

    raw_count = df.count()
    silver_count = silver_df.count()

    os.makedirs(CUSTOMERS_ORGS_SILVER, exist_ok=True)
    (
        silver_df.write.mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(CUSTOMERS_ORGS_SILVER)
    )

    written_count = spark.read.parquet(CUSTOMERS_ORGS_SILVER).count()

    return {
        "dataset_name": "customers_orgs",
        "bronze_path": CUSTOMERS_ORGS_BRONZE,
        "silver_path": CUSTOMERS_ORGS_SILVER,
        "raw_count": raw_count,
        "silver_count": silver_count,
        "written_count": written_count,
    }


def _enrich_usage_events(events_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Join de enriquecimiento y features de negocio."""
    customers = customers_df.select(
        "org_id",
        F.col("org_name").alias("org_name"),
        F.col("industry").alias("org_industry"),
        F.col("plan_tier").alias("org_plan_tier"),
        F.col("lifecycle_stage").alias("org_lifecycle_stage"),
        F.col("hq_region").alias("org_hq_region"),
    )

    enriched = events_df.join(customers, on="org_id", how="left")

    return (
        enriched.withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("daily_cost_usd", F.col("cost_usd_increment"))
        .withColumn(
            "requests",
            F.when(F.col("metric") == "requests", F.coalesce(F.col("value"), F.lit(0.0)))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg"), F.lit(0.0)))
        .withColumn(
            "is_cost_anomaly",
            F.col("cost_usd_increment") < F.lit(COST_ANOMALY_THRESHOLD),
        )
        .withColumn("silver_ts", F.current_timestamp())
    )


def _split_valid_and_quarantine(enriched_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Aplica las 3 reglas activas y separa válidos vs quarantine."""
    quarantine_parts: list[DataFrame] = []

    null_event_id = enriched_df.filter(F.col("event_id").isNull())
    if null_event_id.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                null_event_id, QUARANTINE_REASONS["null_event_id"]
            )
        )

    window = Window.partitionBy("event_id").orderBy(F.desc("event_ts"))
    duplicates = (
        enriched_df.filter(F.col("event_id").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") > 1)
        .drop("_rn")
    )
    if duplicates.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                duplicates, QUARANTINE_REASONS["duplicate_event_id"]
            )
        )

    valid_ids = enriched_df.filter(F.col("event_id").isNotNull()).withColumn(
        "_rn", F.row_number().over(window)
    )
    deduped = valid_ids.filter(F.col("_rn") == 1).drop("_rn")

    missing_unit = deduped.filter(
        F.col("value").isNotNull() & F.col("unit").isNull()
    )
    if missing_unit.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                missing_unit, QUARANTINE_REASONS["missing_unit_with_value"]
            )
        )

    orphans = deduped.filter(
        F.col("org_name").isNull() & ~(
            F.col("value").isNotNull() & F.col("unit").isNull()
        )
    )
    if orphans.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(orphans, QUARANTINE_REASONS["orphan_org_id"])
        )

    valid_df = deduped.filter(
        F.col("org_name").isNotNull()
        & ~(F.col("value").isNotNull() & F.col("unit").isNull())
    )

    if quarantine_parts:
        quarantine_df = quarantine_parts[0]
        for part in quarantine_parts[1:]:
            quarantine_df = quarantine_df.unionByName(part, allowMissingColumns=True)
    else:
        quarantine_df = enriched_df.limit(0)

    return valid_df, quarantine_df


def process_usage_events_silver(spark: SparkSession) -> dict[str, Any]:
    """Limpieza, enriquecimiento, features y quarantine de eventos."""
    events_df = spark.read.parquet(USAGE_EVENTS_BRONZE)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)

    raw_count = events_df.count()
    enriched_df = _enrich_usage_events(events_df, customers_df)
    valid_df, quarantine_df = _split_valid_and_quarantine(enriched_df)

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    cost_anomalies = valid_df.filter(F.col("is_cost_anomaly")).count()

    os.makedirs(USAGE_EVENTS_SILVER, exist_ok=True)
    (
        valid_df.write.mode("overwrite")
        .partitionBy("usage_date")
        .parquet(USAGE_EVENTS_SILVER)
    )

    os.makedirs(USAGE_EVENTS_QUARANTINE, exist_ok=True)
    if quarantine_count > 0:
        (
            quarantine_df.write.mode("overwrite")
            .partitionBy("ingest_date")
            .parquet(USAGE_EVENTS_QUARANTINE)
        )

    written_valid = spark.read.parquet(USAGE_EVENTS_SILVER).count()
    written_quarantine = (
        spark.read.parquet(USAGE_EVENTS_QUARANTINE).count()
        if quarantine_count > 0
        else 0
    )

    quarantine_sample = (
        quarantine_df.select(
            "event_id", "org_id", "metric", "value", "unit", "error_reason"
        )
        .limit(5)
        .collect()
        if quarantine_count > 0
        else []
    )

    return {
        "dataset_name": "usage_events",
        "bronze_path": USAGE_EVENTS_BRONZE,
        "silver_path": USAGE_EVENTS_SILVER,
        "quarantine_path": USAGE_EVENTS_QUARANTINE,
        "raw_count": raw_count,
        "valid_count": valid_count,
        "quarantine_count": quarantine_count,
        "written_valid_count": written_valid,
        "written_quarantine_count": written_quarantine,
        "cost_anomalies_flagged": cost_anomalies,
        "quarantine_sample": [row.asDict() for row in quarantine_sample],
    }


def run_silver(spark: SparkSession) -> list[dict[str, Any]]:
    """Ejecuta Silver para maestro + eventos en orden de dependencia."""
    customers_result = process_customers_orgs_silver(spark)
    events_result = process_usage_events_silver(spark)
    return [customers_result, events_result]


def validate_silver(spark: SparkSession) -> dict[str, Any]:
    """Valida features, unicidad y balance raw = valid + quarantine."""
    events_silver = spark.read.parquet(USAGE_EVENTS_SILVER)
    events_bronze = spark.read.parquet(USAGE_EVENTS_BRONZE)

    total_silver = events_silver.count()
    distinct_ids = events_silver.select("event_id").distinct().count()
    required_features = {"daily_cost_usd", "requests", "genai_tokens", "carbon_kg"}
    missing_features = sorted(required_features - set(events_silver.columns))

    quarantine_count = 0
    if os.path.isdir(USAGE_EVENTS_QUARANTINE):
        quarantine_count = spark.read.parquet(USAGE_EVENTS_QUARANTINE).count()

    bronze_count = events_bronze.count()

    return {
        "bronze_count": bronze_count,
        "silver_valid_count": total_silver,
        "quarantine_count": quarantine_count,
        "balance_ok": bronze_count == total_silver + quarantine_count,
        "event_id_unique": total_silver == distinct_ids,
        "missing_features": missing_features,
        "features_ok": len(missing_features) == 0,
    }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("silver")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    results = run_silver(spark)
    for result in results:
        print(f"\n=== {result['dataset_name']} ===")
        for key, value in result.items():
            if key != "quarantine_sample":
                print(f"  {key}: {value}")
        if result.get("quarantine_sample"):
            print("  quarantine_sample:")
            for row in result["quarantine_sample"]:
                print(f"    {row}")

    validation = validate_silver(spark)
    status = (
        "OK"
        if validation["balance_ok"]
        and validation["event_id_unique"]
        and validation["features_ok"]
        else "FAIL"
    )
    print(f"\nvalidation [{status}]: {validation}")

    spark.stop()
