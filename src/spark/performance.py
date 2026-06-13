"""Spark write and shuffle tuning for lake layers."""

from __future__ import annotations

import os
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession

from src.config import (
    SPARK_SHUFFLE_PARTITIONS,
    SPARK_TARGET_FILES_EVENTS,
    SPARK_TARGET_FILES_GOLD,
    SPARK_TARGET_FILES_MASTER,
)


def configure_spark_performance(spark: SparkSession) -> None:
    """Apply shuffle and adaptive execution defaults for batch jobs."""
    spark.conf.set("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


def count_parquet_files(path: str) -> int:
    return sum(
        1
        for root, _, files in os.walk(path)
        for name in files
        if name.endswith(".parquet")
    )


def write_partitioned_parquet(
    df: DataFrame,
    path: str,
    partition_cols: Iterable[str] | None = None,
    *,
    repartition_cols: list[str] | None = None,
    num_files: int | None = None,
    mode: str = "overwrite",
) -> None:
    """Write Parquet with optional repartition/coalesce before persist."""
    os.makedirs(path, exist_ok=True)
    output = df

    if repartition_cols and num_files:
        output = output.repartition(num_files, *repartition_cols)
    elif num_files:
        output = output.coalesce(max(1, num_files))

    writer = output.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*list(partition_cols))
    writer.parquet(path)
