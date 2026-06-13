"""Batch job: Landing CSV -> Bronze Parquet."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from src.config import BRONZE, LANDING
from src.schemas.bronze_batch import (
    BILLING_MONTHLY_SCHEMA,
    CUSTOMERS_ORGS_SCHEMA,
    USERS_SCHEMA,
)


def _parse_boolean(column_name: str) -> Column:
    normalized = F.lower(F.trim(F.col(column_name)))
    return F.when(normalized == "true", F.lit(True)).otherwise(F.lit(False))


def _parse_date(column_name: str) -> Column:
    return F.to_date(F.col(column_name))


def _parse_double(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed.cast("double"))


def _landing_relative_source_file(landing_glob: str) -> Column:
    relative_path = os.path.join("landing", os.path.basename(landing_glob))
    return F.lit(relative_path.replace("\\", "/"))


def _apply_customers_orgs_casts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("is_enterprise", _parse_boolean("is_enterprise"))
        .withColumn("signup_date", _parse_date("signup_date"))
        .withColumn("nps_score", _parse_double("nps_score"))
    )


def _apply_users_casts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("active", _parse_boolean("active"))
        .withColumn("created_at", _parse_date("created_at"))
        .withColumn("last_login", _parse_date("last_login"))
    )


def _apply_billing_monthly_casts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("month", _parse_date("month"))
        .withColumn("subtotal", _parse_double("subtotal"))
        .withColumn("credits", _parse_double("credits"))
        .withColumn("taxes", _parse_double("taxes"))
        .withColumn("exchange_rate_to_usd", _parse_double("exchange_rate_to_usd"))
    )


BATCH_BRONZE_DATASETS: list[dict[str, Any]] = [
    {
        "dataset_name": "customers_orgs",
        "landing_glob": os.path.join(LANDING, "customers_orgs.csv"),
        "schema": CUSTOMERS_ORGS_SCHEMA,
        "dedup_keys": ["org_id"],
        "bronze_path": os.path.join(BRONZE, "customers_orgs"),
        "apply_casts": _apply_customers_orgs_casts,
        "natural_key": "org_id",
    },
    {
        "dataset_name": "users",
        "landing_glob": os.path.join(LANDING, "users.csv"),
        "schema": USERS_SCHEMA,
        "dedup_keys": ["user_id"],
        "bronze_path": os.path.join(BRONZE, "users"),
        "apply_casts": _apply_users_casts,
        "natural_key": "user_id",
    },
    {
        "dataset_name": "billing_monthly",
        "landing_glob": os.path.join(LANDING, "billing_monthly.csv"),
        "schema": BILLING_MONTHLY_SCHEMA,
        "dedup_keys": ["invoice_id"],
        "bronze_path": os.path.join(BRONZE, "billing_monthly"),
        "apply_casts": _apply_billing_monthly_casts,
        "natural_key": "invoice_id",
    },
]


def ingest_master_to_bronze(
    spark: SparkSession,
    dataset_name: str,
    landing_glob: str,
    schema: StructType,
    dedup_keys: list[str],
    bronze_path: str,
    apply_casts,
) -> dict[str, Any]:
    df = (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(landing_glob)
    )

    df = apply_casts(df)
    df = (
        df.withColumn("source_file", _landing_relative_source_file(landing_glob))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )

    raw_count = df.count()

    window = Window.partitionBy(*dedup_keys).orderBy(F.desc("ingest_ts"))
    df_deduped = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    deduped_count = df_deduped.count()

    os.makedirs(bronze_path, exist_ok=True)
    (
        df_deduped.write.mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(bronze_path)
    )

    written_count = spark.read.parquet(bronze_path).count()

    return {
        "dataset_name": dataset_name,
        "landing_glob": landing_glob,
        "bronze_path": bronze_path,
        "raw_count": raw_count,
        "deduped_count": deduped_count,
        "removed_duplicates": raw_count - deduped_count,
        "written_count": written_count,
        "dedup_keys": dedup_keys,
    }


def run_batch_bronze(spark: SparkSession) -> list[dict[str, Any]]:
    return [
        ingest_master_to_bronze(
            spark=spark,
            dataset_name=dataset["dataset_name"],
            landing_glob=dataset["landing_glob"],
            schema=dataset["schema"],
            dedup_keys=dataset["dedup_keys"],
            bronze_path=dataset["bronze_path"],
            apply_casts=dataset["apply_casts"],
        )
        for dataset in BATCH_BRONZE_DATASETS
    ]


def validate_bronze_uniqueness(spark: SparkSession) -> list[dict[str, Any]]:
    validations = []
    for dataset in BATCH_BRONZE_DATASETS:
        natural_key = dataset["natural_key"]
        bronze_path = dataset["bronze_path"]
        df = spark.read.parquet(bronze_path)
        total = df.count()
        distinct = df.select(natural_key).distinct().count()
        validations.append(
            {
                "dataset_name": dataset["dataset_name"],
                "natural_key": natural_key,
                "total_rows": total,
                "distinct_keys": distinct,
                "is_unique": total == distinct,
            }
        )
    return validations


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("batch-bronze")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    results = run_batch_bronze(spark)
    for result in results:
        print(
            f"{result['dataset_name']}: raw={result['raw_count']} "
            f"deduped={result['deduped_count']} written={result['written_count']}"
        )

    for validation in validate_bronze_uniqueness(spark):
        status = "OK" if validation["is_unique"] else "FAIL"
        print(
            f"{validation['dataset_name']} uniqueness [{status}]: "
            f"{validation['distinct_keys']}/{validation['total_rows']}"
        )

    spark.stop()
