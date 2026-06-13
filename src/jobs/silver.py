"""Silver job: Bronze -> Silver + Quarantine."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

from src.config import (
    BRONZE,
    QUARANTINE,
    SILVER,
    SPARK_SHUFFLE_PARTITIONS,
    SPARK_TARGET_FILES_EVENTS,
    SPARK_TARGET_FILES_MASTER,
)
from src.spark.performance import configure_spark_performance, write_partitioned_parquet
from src.schemas.silver import (
    FUTURE_EVENT_TOLERANCE_SEC,
    MAD_MIN_FLOOR,
    MAD_THRESHOLD,
    PERCENTILE_HIGH,
    PERCENTILE_LOW,
    QUARANTINE_REASONS,
    ZSCORE_THRESHOLD,
)
from src.schemas.bronze_streaming import LATE_CATCHUP_MAX_SEC

CUSTOMERS_ORGS_BRONZE = os.path.join(BRONZE, "customers_orgs")
USAGE_EVENTS_BRONZE = os.path.join(BRONZE, "usage_events")

CUSTOMERS_ORGS_SILVER = os.path.join(SILVER, "customers_orgs")
USAGE_EVENTS_SILVER = os.path.join(SILVER, "usage_events")
ORG_SERVICE_DAILY_SILVER = os.path.join(SILVER, "org_service_daily")
USAGE_EVENTS_QUARANTINE = os.path.join(QUARANTINE, "silver", "usage_events")

MASTER_SILVER_DATASETS: list[dict[str, Any]] = [
    {
        "dataset_name": "customers_orgs",
        "bronze_path": CUSTOMERS_ORGS_BRONZE,
        "silver_path": os.path.join(SILVER, "customers_orgs"),
        "primary_keys": ["org_id"],
        "trim_columns": [
            "org_name",
            "industry",
            "hq_region",
            "plan_tier",
            "lifecycle_stage",
            "marketing_source",
            "sales_rep",
        ],
        "check_org": False,
    },
    {
        "dataset_name": "users",
        "bronze_path": os.path.join(BRONZE, "users"),
        "silver_path": os.path.join(SILVER, "users"),
        "primary_keys": ["user_id"],
        "trim_columns": ["email", "role"],
        "check_org": True,
    },
    {
        "dataset_name": "billing_monthly",
        "bronze_path": os.path.join(BRONZE, "billing_monthly"),
        "silver_path": os.path.join(SILVER, "billing_monthly"),
        "primary_keys": ["invoice_id"],
        "trim_columns": ["currency"],
        "check_org": True,
    },
    {
        "dataset_name": "resources",
        "bronze_path": os.path.join(BRONZE, "resources"),
        "silver_path": os.path.join(SILVER, "resources"),
        "primary_keys": ["resource_id"],
        "trim_columns": ["service", "region", "state", "tags_json"],
        "check_org": True,
    },
    {
        "dataset_name": "support_tickets",
        "bronze_path": os.path.join(BRONZE, "support_tickets"),
        "silver_path": os.path.join(SILVER, "support_tickets"),
        "primary_keys": ["ticket_id"],
        "trim_columns": ["category", "severity"],
        "check_org": True,
    },
    {
        "dataset_name": "marketing_touches",
        "bronze_path": os.path.join(BRONZE, "marketing_touches"),
        "silver_path": os.path.join(SILVER, "marketing_touches"),
        "primary_keys": ["touch_id"],
        "trim_columns": ["campaign", "channel"],
        "check_org": True,
    },
    {
        "dataset_name": "nps_surveys",
        "bronze_path": os.path.join(BRONZE, "nps_surveys"),
        "silver_path": os.path.join(SILVER, "nps_surveys"),
        "primary_keys": ["org_id", "survey_date"],
        "trim_columns": ["comment"],
        "check_org": True,
    },
]


def _add_quarantine_metadata(df: DataFrame, error_reason: str) -> DataFrame:
    return df.withColumn("error_reason", F.lit(error_reason)).withColumn(
        "quarantine_ts", F.current_timestamp()
    )


def _trim_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, F.trim(F.col(column)))
    return df


def _null_primary_key_condition(primary_keys: list[str]) -> Any:
    condition = F.col(primary_keys[0]).isNull()
    for key in primary_keys[1:]:
        condition = condition | F.col(key).isNull()
    return condition


def _write_silver_master(df: DataFrame, silver_path: str) -> None:
    write_partitioned_parquet(
        df,
        silver_path,
        partition_cols=["ingest_date"],
        num_files=SPARK_TARGET_FILES_MASTER,
    )


def _write_quarantine(df: DataFrame, quarantine_path: str) -> None:
    if df.head(1):
        write_partitioned_parquet(
            df,
            quarantine_path,
            partition_cols=["ingest_date"],
            num_files=SPARK_TARGET_FILES_MASTER,
        )


def process_master_silver(
    spark: SparkSession,
    dataset_name: str,
    bronze_path: str,
    silver_path: str,
    primary_keys: list[str],
    trim_columns: list[str],
    check_org: bool,
    customers_silver_path: str | None = None,
) -> dict[str, Any]:
    df = spark.read.parquet(bronze_path)
    silver_df = _trim_columns(df, trim_columns).withColumn(
        "silver_ts", F.current_timestamp()
    )

    quarantine_parts: list[DataFrame] = []
    null_pk = silver_df.filter(_null_primary_key_condition(primary_keys))
    if null_pk.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(null_pk, QUARANTINE_REASONS["null_primary_key"])
        )

    working_df = silver_df.filter(~_null_primary_key_condition(primary_keys))

    if check_org and customers_silver_path:
        customers = spark.read.parquet(customers_silver_path).select("org_id")
        orphans = working_df.join(customers, on="org_id", how="left_anti")
        if orphans.head(1):
            quarantine_parts.append(
                _add_quarantine_metadata(orphans, QUARANTINE_REASONS["orphan_org_id"])
            )
        working_df = working_df.join(customers, on="org_id", how="inner")

    if quarantine_parts:
        quarantine_df = quarantine_parts[0]
        for part in quarantine_parts[1:]:
            quarantine_df = quarantine_df.unionByName(part, allowMissingColumns=True)
    else:
        quarantine_df = silver_df.limit(0)

    quarantine_path = os.path.join(QUARANTINE, "silver", dataset_name)
    raw_count = df.count()
    silver_count = working_df.count()
    quarantine_count = quarantine_df.count()

    _write_silver_master(working_df, silver_path)
    _write_quarantine(quarantine_df, quarantine_path)

    return {
        "dataset_name": dataset_name,
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "quarantine_path": quarantine_path,
        "raw_count": raw_count,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "written_count": spark.read.parquet(silver_path).count(),
        "balance_ok": raw_count == silver_count + quarantine_count,
    }


def _metric_feature(metric_name: str, feature_name: str) -> Any:
    return F.when(F.col("metric") == metric_name, F.coalesce(F.col("value"), F.lit(0.0))).otherwise(
        F.lit(0.0)
    ).alias(feature_name)


def _build_org_user_stats(users_df: DataFrame) -> DataFrame:
    return users_df.groupBy("org_id").agg(
        F.count("*").alias("org_user_count"),
        F.sum(F.when(F.col("active"), 1).otherwise(0)).alias("org_active_user_count"),
    )


def _add_cost_anomaly_flags(df: DataFrame) -> DataFrame:
    stats = df.groupBy("org_id", "service").agg(
        F.avg("cost_usd_increment").alias("_mean_cost"),
        F.stddev_pop("cost_usd_increment").alias("_std_cost"),
        F.expr(f"percentile_approx(cost_usd_increment, {PERCENTILE_LOW})").alias("_p01"),
        F.expr(f"percentile_approx(cost_usd_increment, {PERCENTILE_HIGH})").alias("_p99"),
        F.expr("percentile_approx(cost_usd_increment, 0.5)").alias("_median_cost"),
    )

    with_stats = df.join(stats, on=["org_id", "service"], how="left")
    with_stats = with_stats.withColumn(
        "_abs_dev",
        F.abs(F.col("cost_usd_increment") - F.col("_median_cost")),
    )

    mad_stats = with_stats.groupBy("org_id", "service").agg(
        F.expr("percentile_approx(_abs_dev, 0.5)").alias("_mad_cost")
    )
    with_stats = with_stats.join(mad_stats, on=["org_id", "service"], how="left")

    with_stats = with_stats.withColumn(
        "_zscore",
        F.when(
            F.col("_std_cost").isNull() | (F.col("_std_cost") == 0),
            F.lit(0.0),
        ).otherwise(
            (F.col("cost_usd_increment") - F.col("_mean_cost")) / F.col("_std_cost")
        ),
    ).withColumn(
        "_modified_z",
        F.when(
            F.col("_mad_cost").isNull() | (F.col("_mad_cost") == 0),
            F.lit(0.0),
        ).otherwise(
            0.6745
            * (F.col("cost_usd_increment") - F.col("_median_cost"))
            / F.col("_mad_cost")
        ),
    )

    return (
        with_stats.withColumn(
            "is_cost_anomaly_zscore",
            F.abs(F.col("_zscore")) > F.lit(ZSCORE_THRESHOLD),
        )
        .withColumn(
            "is_cost_anomaly_mad",
            (F.col("_mad_cost") >= F.lit(MAD_MIN_FLOOR))
            & (F.abs(F.col("_modified_z")) > F.lit(MAD_THRESHOLD)),
        )
        .withColumn(
            "is_cost_anomaly_percentile",
            (F.col("cost_usd_increment") < F.col("_p01"))
            | (F.col("cost_usd_increment") > F.col("_p99")),
        )
        .withColumn(
            "is_cost_anomaly",
            F.col("is_cost_anomaly_zscore")
            | F.col("is_cost_anomaly_mad")
            | F.col("is_cost_anomaly_percentile"),
        )
        .drop(
            "_mean_cost",
            "_std_cost",
            "_p01",
            "_p99",
            "_median_cost",
            "_abs_dev",
            "_mad_cost",
            "_zscore",
            "_modified_z",
        )
    )


def _enrich_usage_events(
    events_df: DataFrame,
    customers_df: DataFrame,
    resources_df: DataFrame,
    org_user_stats_df: DataFrame,
) -> DataFrame:
    customers = customers_df.select(
        "org_id",
        F.col("org_name").alias("org_name"),
        F.col("industry").alias("org_industry"),
        F.col("plan_tier").alias("org_plan_tier"),
        F.col("lifecycle_stage").alias("org_lifecycle_stage"),
        F.col("hq_region").alias("org_hq_region"),
    )

    resources = resources_df.select(
        "resource_id",
        F.col("service").alias("resource_service"),
        F.col("region").alias("resource_region"),
        F.col("state").alias("resource_state"),
    )

    enriched = (
        events_df.repartition(SPARK_SHUFFLE_PARTITIONS, "org_id")
        .join(broadcast(customers), on="org_id", how="left")
        .join(broadcast(resources), on="resource_id", how="left")
        .join(broadcast(org_user_stats_df), on="org_id", how="left")
    )

    enriched = (
        enriched.withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("requests", _metric_feature("requests", "requests"))
        .withColumn("cpu_hours", _metric_feature("cpu_hours", "cpu_hours"))
        .withColumn(
            "storage_gb_hours",
            _metric_feature("storage_gb_hours", "storage_gb_hours"),
        )
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg"), F.lit(0.0)))
        .withColumn(
            "org_user_count", F.coalesce(F.col("org_user_count"), F.lit(0))
        )
        .withColumn(
            "org_active_user_count",
            F.coalesce(F.col("org_active_user_count"), F.lit(0)),
        )
        .withColumn("silver_ts", F.current_timestamp())
    )

    return _add_cost_anomaly_flags(enriched)


def _build_org_service_daily(valid_events_df: DataFrame) -> DataFrame:
    """Silver feature grain: (org_id, usage_date, service) per consigna completa."""
    return (
        valid_events_df.groupBy("org_id", "usage_date", "service")
        .agg(
            F.first("org_name").alias("org_name"),
            F.first("org_industry").alias("org_industry"),
            F.first("org_plan_tier").alias("org_plan_tier"),
            F.sum("cost_usd_increment").alias("daily_cost_usd"),
            F.sum("requests").alias("requests"),
            F.sum("cpu_hours").alias("cpu_hours"),
            F.sum("storage_gb_hours").alias("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.sum("carbon_kg").alias("carbon_kg"),
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("is_cost_anomaly"), 1).otherwise(0)).alias(
                "anomaly_event_count"
            ),
            F.max(F.col("is_cost_anomaly").cast("int")).alias("has_cost_anomaly"),
        )
        .withColumn("silver_ts", F.current_timestamp())
    )


def _future_event_ts_condition() -> Any:
    return F.col("event_ts").isNotNull() & (
        F.col("event_ts")
        > F.current_timestamp()
        + F.expr(f"INTERVAL {FUTURE_EVENT_TOLERANCE_SEC} SECONDS")
    )


def _split_valid_and_quarantine(enriched_df: DataFrame) -> tuple[DataFrame, DataFrame]:
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

    orphans_org = deduped.filter(
        F.col("org_name").isNull()
        & ~(F.col("value").isNotNull() & F.col("unit").isNull())
    )
    if orphans_org.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(orphans_org, QUARANTINE_REASONS["orphan_org_id"])
        )

    orphans_resource = deduped.filter(
        F.col("resource_id").isNotNull()
        & F.col("resource_service").isNull()
        & F.col("org_name").isNotNull()
        & ~(F.col("value").isNotNull() & F.col("unit").isNull())
    )
    if orphans_resource.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                orphans_resource, QUARANTINE_REASONS["orphan_resource_id"]
            )
        )

    late_exceeds_catchup = deduped.filter(
        F.col("event_latency_sec") > F.lit(LATE_CATCHUP_MAX_SEC)
    )
    if late_exceeds_catchup.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                late_exceeds_catchup,
                QUARANTINE_REASONS["late_arrival_exceeds_catchup"],
            )
        )

    future_events = deduped.filter(_future_event_ts_condition())
    if future_events.head(1):
        quarantine_parts.append(
            _add_quarantine_metadata(
                future_events,
                QUARANTINE_REASONS["future_event_ts"],
            )
        )

    valid_df = deduped.filter(
        F.col("org_name").isNotNull()
        & (
            F.col("resource_id").isNull()
            | F.col("resource_service").isNotNull()
        )
        & ~(F.col("value").isNotNull() & F.col("unit").isNull())
        & (
            F.col("event_latency_sec").isNull()
            | (F.col("event_latency_sec") <= F.lit(LATE_CATCHUP_MAX_SEC))
        )
        & ~_future_event_ts_condition()
    )

    if quarantine_parts:
        quarantine_df = quarantine_parts[0]
        for part in quarantine_parts[1:]:
            quarantine_df = quarantine_df.unionByName(part, allowMissingColumns=True)
    else:
        quarantine_df = enriched_df.limit(0)

    return valid_df, quarantine_df


def process_usage_events_silver(spark: SparkSession) -> dict[str, Any]:
    events_df = spark.read.parquet(USAGE_EVENTS_BRONZE)
    customers_df = spark.read.parquet(CUSTOMERS_ORGS_SILVER)
    resources_df = spark.read.parquet(os.path.join(SILVER, "resources"))
    users_df = spark.read.parquet(os.path.join(SILVER, "users"))
    org_user_stats_df = _build_org_user_stats(users_df)

    raw_count = events_df.count()
    enriched_df = _enrich_usage_events(
        events_df, customers_df, resources_df, org_user_stats_df
    )
    valid_df, quarantine_df = _split_valid_and_quarantine(enriched_df)

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    write_partitioned_parquet(
        valid_df,
        USAGE_EVENTS_SILVER,
        partition_cols=["usage_date"],
        repartition_cols=["usage_date"],
        num_files=SPARK_TARGET_FILES_EVENTS,
    )

    org_service_daily_df = _build_org_service_daily(valid_df)
    org_service_daily_count = org_service_daily_df.count()
    write_partitioned_parquet(
        org_service_daily_df,
        ORG_SERVICE_DAILY_SILVER,
        partition_cols=["usage_date"],
        repartition_cols=["usage_date"],
        num_files=SPARK_TARGET_FILES_EVENTS,
    )
    _write_quarantine(quarantine_df, USAGE_EVENTS_QUARANTINE)

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
        "org_service_daily_path": ORG_SERVICE_DAILY_SILVER,
        "quarantine_path": USAGE_EVENTS_QUARANTINE,
        "raw_count": raw_count,
        "valid_count": valid_count,
        "org_service_daily_count": org_service_daily_count,
        "quarantine_count": quarantine_count,
        "written_valid_count": spark.read.parquet(USAGE_EVENTS_SILVER).count(),
        "written_quarantine_count": (
            spark.read.parquet(USAGE_EVENTS_QUARANTINE).count()
            if quarantine_count > 0
            else 0
        ),
        "balance_ok": raw_count == valid_count + quarantine_count,
        "cost_anomalies_flagged": valid_df.filter(F.col("is_cost_anomaly")).count(),
        "cost_anomalies_zscore": valid_df.filter(
            F.col("is_cost_anomaly_zscore")
        ).count(),
        "cost_anomalies_mad": valid_df.filter(F.col("is_cost_anomaly_mad")).count(),
        "cost_anomalies_percentile": valid_df.filter(
            F.col("is_cost_anomaly_percentile")
        ).count(),
        "late_arrivals_flagged": valid_df.filter(F.col("is_late_arrival")).count(),
        "late_arrivals_quarantined": (
            quarantine_df.filter(
                F.col("error_reason")
                == QUARANTINE_REASONS["late_arrival_exceeds_catchup"]
            ).count()
            if quarantine_count > 0
            else 0
        ),
        "future_event_ts_quarantined": (
            quarantine_df.filter(
                F.col("error_reason") == QUARANTINE_REASONS["future_event_ts"]
            ).count()
            if quarantine_count > 0
            else 0
        ),
        "quarantine_sample": [row.asDict() for row in quarantine_sample],
    }


def run_silver(spark: SparkSession) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for dataset in MASTER_SILVER_DATASETS:
        customers_path = CUSTOMERS_ORGS_SILVER if dataset["check_org"] else None
        results.append(
            process_master_silver(
                spark=spark,
                dataset_name=dataset["dataset_name"],
                bronze_path=dataset["bronze_path"],
                silver_path=dataset["silver_path"],
                primary_keys=dataset["primary_keys"],
                trim_columns=dataset["trim_columns"],
                check_org=dataset["check_org"],
                customers_silver_path=customers_path,
            )
        )

    results.append(process_usage_events_silver(spark))
    return results


def _quarantine_count(spark: SparkSession, quarantine_path: str) -> int:
    if not os.path.isdir(quarantine_path):
        return 0
    has_parquet = any(
        name.endswith(".parquet")
        for root, _, files in os.walk(quarantine_path)
        for name in files
    )
    if not has_parquet:
        return 0
    return spark.read.parquet(quarantine_path).count()


def validate_silver(spark: SparkSession) -> dict[str, Any]:
    master_validations: list[dict[str, Any]] = []
    for dataset in MASTER_SILVER_DATASETS:
        silver_df = spark.read.parquet(dataset["silver_path"])
        bronze_df = spark.read.parquet(dataset["bronze_path"])
        quarantine_path = os.path.join(QUARANTINE, "silver", dataset["dataset_name"])
        quarantine_count = _quarantine_count(spark, quarantine_path)
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        master_validations.append(
            {
                "dataset_name": dataset["dataset_name"],
                "bronze_count": bronze_count,
                "silver_count": silver_count,
                "quarantine_count": quarantine_count,
                "balance_ok": bronze_count == silver_count + quarantine_count,
            }
        )

    events_silver = spark.read.parquet(USAGE_EVENTS_SILVER)
    events_bronze = spark.read.parquet(USAGE_EVENTS_BRONZE)

    total_silver = events_silver.count()
    distinct_ids = events_silver.select("event_id").distinct().count()
    required_features = {
        "cost_usd_increment",
        "requests",
        "cpu_hours",
        "storage_gb_hours",
        "genai_tokens",
        "carbon_kg",
        "org_user_count",
        "org_active_user_count",
        "resource_service",
        "resource_region",
        "is_cost_anomaly",
        "is_cost_anomaly_zscore",
        "is_cost_anomaly_mad",
        "is_cost_anomaly_percentile",
    }
    missing_features = sorted(required_features - set(events_silver.columns))

    org_service_daily = spark.read.parquet(ORG_SERVICE_DAILY_SILVER)
    org_service_daily_count = org_service_daily.count()
    org_service_daily_grain = (
        org_service_daily.select("org_id", "usage_date", "service").distinct().count()
    )
    org_service_daily_features = {
        "daily_cost_usd",
        "requests",
        "cpu_hours",
        "storage_gb_hours",
        "genai_tokens",
        "carbon_kg",
    }
    missing_org_service_daily = sorted(
        org_service_daily_features - set(org_service_daily.columns)
    )

    quarantine_count = _quarantine_count(spark, USAGE_EVENTS_QUARANTINE)

    bronze_count = events_bronze.count()

    return {
        "masters": master_validations,
        "usage_events": {
            "bronze_count": bronze_count,
            "silver_valid_count": total_silver,
            "quarantine_count": quarantine_count,
            "balance_ok": bronze_count == total_silver + quarantine_count,
            "event_id_unique": total_silver == distinct_ids,
            "missing_features": missing_features,
            "features_ok": len(missing_features) == 0,
        },
        "org_service_daily": {
            "silver_row_count": org_service_daily_count,
            "distinct_grain": org_service_daily_grain,
            "grain_unique": org_service_daily_count == org_service_daily_grain,
            "missing_features": missing_org_service_daily,
            "features_ok": len(missing_org_service_daily) == 0,
            "cost_balance_ok": abs(
                (events_silver.agg(F.sum("cost_usd_increment")).collect()[0][0] or 0)
                - (
                    org_service_daily.agg(F.sum("daily_cost_usd")).collect()[0][0]
                    or 0
                )
            )
            < 0.01,
        },
    }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("silver")
        .master("local[*]")
        .getOrCreate()
    )
    configure_spark_performance(spark)
    spark.sparkContext.setLogLevel("WARN")

    for result in run_silver(spark):
        print(f"\n=== {result['dataset_name']} ===")
        for key, value in result.items():
            if key != "quarantine_sample":
                print(f"  {key}: {value}")

    validation = validate_silver(spark)
    masters_ok = all(item["balance_ok"] for item in validation["masters"])
    events = validation["usage_events"]
    status = (
        "OK"
        if masters_ok
        and events["balance_ok"]
        and events["event_id_unique"]
        and events["features_ok"]
        else "FAIL"
    )
    print(f"\nvalidation [{status}]: {validation}")

    spark.stop()
