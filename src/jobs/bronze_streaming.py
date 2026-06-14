"""Streaming job: Landing JSONL -> Bronze Parquet."""

from __future__ import annotations

import os
import shutil
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.config import (
    BRONZE,
    CHECKPOINTS,
    LANDING,
    SPARK_TARGET_FILES_EVENTS,
)
from src.spark.performance import (
    configure_spark_performance,
    count_parquet_files,
    write_partitioned_parquet,
)
from src.schemas.bronze_streaming import (
    LATE_CATCHUP_MAX_SEC,
    LATE_DATA_THRESHOLD_SEC,
    USAGE_EVENTS_SCHEMA,
    WATERMARK_DELAY,
    WATERMARK_DELAY_PRODUCTION,
)
from src.schemas.normalization import (
    normalized_region,
    normalized_service,
    normalized_string,
)

USAGE_EVENTS_LANDING_GLOB = os.path.join(LANDING, "usage_events_stream", "*.jsonl")
USAGE_EVENTS_BRONZE_PATH = os.path.join(BRONZE, "usage_events")
USAGE_EVENTS_CHECKPOINT_PATH = os.path.join(CHECKPOINTS, "usage_events_bronze")


def _streaming_num_input_rows(query: StreamingQuery) -> int:
    total = 0
    for progress in query.recentProgress or []:
        if progress.get("numInputRows") is not None:
            total += int(progress["numInputRows"])
    return total


def _is_reparquet_layout(bronze_path: str) -> bool:
    if not os.path.isdir(bronze_path):
        return False
    return any(
        name.startswith("usage_date=") for name in os.listdir(bronze_path)
    )


def _clean_stale_stream_partitions(bronze_path: str) -> int:
    """Drop empty ingest_date dirs left by streaming reruns on reparquet layout."""
    if not _is_reparquet_layout(bronze_path):
        return 0

    removed = 0
    for name in os.listdir(bronze_path):
        if not name.startswith("ingest_date="):
            continue
        partition_dir = os.path.join(bronze_path, name)
        if count_parquet_files(partition_dir) == 0:
            shutil.rmtree(partition_dir)
            removed += 1
    return removed


def _prepare_bronze_for_read(bronze_path: str) -> int:
    """Remove empty hive partitions that break spark.read.parquet schema inference."""
    removed = _clean_stale_stream_partitions(bronze_path)
    if not os.path.isdir(bronze_path):
        return removed

    for root, dirs, files in os.walk(bronze_path, topdown=False):
        if root == bronze_path:
            continue
        if not files and not dirs:
            os.rmdir(root)
            removed += 1
    return removed


def read_bronze_usage_events_parquet(
    spark: SparkSession,
    bronze_path: str = USAGE_EVENTS_BRONZE_PATH,
) -> DataFrame:
    _prepare_bronze_for_read(bronze_path)
    if count_parquet_files(bronze_path) == 0:
        raise FileNotFoundError(
            f"No Parquet files under {bronze_path}. "
            "Run streaming Bronze with reset_state=True first."
        )
    return spark.read.option("mergeSchema", "true").parquet(bronze_path)


def _streaming_relative_source_file() -> Column:
    filename = F.regexp_extract(F.input_file_name(), r"([^/]+\.jsonl)$", 1)
    return F.concat(F.lit("landing/usage_events_stream/"), filename)


def _parse_event_value() -> Column:
    as_string = F.trim(F.col("value").cast("string"))
    return (
        F.when(as_string.isNull() | (as_string == ""), F.lit(None))
        .otherwise(as_string.cast("double"))
    )


def _is_replay_watermark() -> bool:
    return WATERMARK_DELAY != WATERMARK_DELAY_PRODUCTION


def _ingest_ts_column() -> Column:
    """Replay: ingest_ts aligned to event time; production: wall-clock ingest."""
    if _is_replay_watermark():
        return F.col("event_ts")
    return F.current_timestamp()


def transform_usage_events_bronze(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("service", normalized_service(F.col("service")))
        .withColumn("region", normalized_region(F.col("region")))
        .withColumn("metric", normalized_string(F.col("metric")))
        .withColumn("unit", normalized_string(F.col("unit")))
        .withColumn("value_numeric", _parse_event_value())
        .drop("value")
        .withColumnRenamed("value_numeric", "value")
        .withColumn("source_file", _streaming_relative_source_file())
        .withColumn("ingest_ts", _ingest_ts_column())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn(
            "event_latency_sec",
            F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts"),
        )
        .withColumn(
            "is_late_arrival",
            F.col("event_latency_sec") > F.lit(LATE_DATA_THRESHOLD_SEC),
        )
    )

    return df.withWatermark("event_ts", WATERMARK_DELAY).dropDuplicates(["event_id"])


def build_usage_events_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.schema(USAGE_EVENTS_SCHEMA)
        .option("maxFilesPerTrigger", 20)
        .json(USAGE_EVENTS_LANDING_GLOB)
    )


def start_usage_events_bronze_query(
    spark: SparkSession,
    *,
    trigger: str = "availableNow",
    processing_interval: str = "10 seconds",
) -> StreamingQuery:
    raw_stream = build_usage_events_stream(spark)
    bronze_stream = transform_usage_events_bronze(raw_stream)

    writer = (
        bronze_stream.writeStream.outputMode("append")
        .option("checkpointLocation", USAGE_EVENTS_CHECKPOINT_PATH)
        .partitionBy("ingest_date")
        .format("parquet")
    )

    if trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=processing_interval)

    return writer.start(USAGE_EVENTS_BRONZE_PATH)


def reparquet_bronze_usage_events(
    spark: SparkSession,
    bronze_path: str = USAGE_EVENTS_BRONZE_PATH,
) -> dict[str, Any]:
    """Re-layout Bronze events by usage_date + service (reparquet) for range scans."""
    files_before = count_parquet_files(bronze_path)
    if files_before == 0:
        return {
            "bronze_path": bronze_path,
            "skipped": True,
            "reason": "no_parquet_files",
            "row_count": 0,
            "parquet_files_before": 0,
            "parquet_files_after": 0,
            "partition_cols": ["usage_date", "service"],
        }

    bronze_df = read_bronze_usage_events_parquet(spark, bronze_path)
    row_count = bronze_df.count()

    optimized = (
        bronze_df.withColumn("usage_date", F.to_date("event_ts"))
        .repartition(SPARK_TARGET_FILES_EVENTS, "usage_date", "service")
    )
    write_partitioned_parquet(
        optimized,
        bronze_path,
        partition_cols=["usage_date", "service"],
        mode="overwrite",
    )

    files_after = count_parquet_files(bronze_path)
    return {
        "bronze_path": bronze_path,
        "skipped": False,
        "row_count": row_count,
        "parquet_files_before": files_before,
        "parquet_files_after": files_after,
        "partition_cols": ["usage_date", "service"],
    }


def reset_streaming_state(
    *,
    bronze_path: str = USAGE_EVENTS_BRONZE_PATH,
    checkpoint_path: str = USAGE_EVENTS_CHECKPOINT_PATH,
) -> None:
    for path in (bronze_path, checkpoint_path):
        if os.path.isdir(path):
            shutil.rmtree(path)


def run_streaming_bronze(
    spark: SparkSession,
    *,
    reset_state: bool = False,
    trigger: str = "availableNow",
) -> dict[str, Any]:
    if reset_state:
        reset_streaming_state()

    os.makedirs(os.path.dirname(USAGE_EVENTS_CHECKPOINT_PATH), exist_ok=True)

    query = start_usage_events_bronze_query(spark, trigger=trigger)
    query.awaitTermination()

    streaming_input_rows = _streaming_num_input_rows(query)
    _prepare_bronze_for_read(USAGE_EVENTS_BRONZE_PATH)

    parquet_files = count_parquet_files(USAGE_EVENTS_BRONZE_PATH)
    should_reparquet = (
        parquet_files > 0
        and (
            reset_state
            or streaming_input_rows > 0
            or not _is_reparquet_layout(USAGE_EVENTS_BRONZE_PATH)
        )
    )

    if should_reparquet:
        reparquet_stats = reparquet_bronze_usage_events(spark)
    else:
        reparquet_stats = {
            "bronze_path": USAGE_EVENTS_BRONZE_PATH,
            "skipped": True,
            "reason": "no_new_stream_rows",
            "streaming_input_rows": streaming_input_rows,
            "parquet_files_before": parquet_files,
            "parquet_files_after": parquet_files,
            "partition_cols": ["usage_date", "service"],
        }

    bronze_df = read_bronze_usage_events_parquet(spark)
    total_rows = bronze_df.count()
    distinct_event_ids = bronze_df.select("event_id").distinct().count()

    return {
        "landing_glob": USAGE_EVENTS_LANDING_GLOB,
        "bronze_path": USAGE_EVENTS_BRONZE_PATH,
        "checkpoint_path": USAGE_EVENTS_CHECKPOINT_PATH,
        "watermark_delay": WATERMARK_DELAY,
        "watermark_delay_production": WATERMARK_DELAY_PRODUCTION,
        "late_data_threshold_sec": LATE_DATA_THRESHOLD_SEC,
        "late_catchup_max_sec": LATE_CATCHUP_MAX_SEC,
        "ingest_ts_mode": "event_ts" if _is_replay_watermark() else "current_timestamp",
        "written_count": total_rows,
        "distinct_event_ids": distinct_event_ids,
        "is_unique": total_rows == distinct_event_ids,
        "late_arrivals": bronze_df.filter(F.col("is_late_arrival")).count(),
        "schema_v2_rows": bronze_df.filter(F.col("schema_version") == 2).count(),
        "streaming_input_rows": streaming_input_rows,
        "reparquet": reparquet_stats,
    }


def validate_bronze_streaming(spark: SparkSession) -> dict[str, Any]:
    bronze_df = read_bronze_usage_events_parquet(spark)
    total = bronze_df.count()
    distinct = bronze_df.select("event_id").distinct().count()

    required_columns = {
        "event_id",
        "event_ts",
        "ingest_ts",
        "ingest_date",
        "source_file",
        "is_late_arrival",
        "schema_version",
        "carbon_kg",
        "genai_tokens",
    }
    missing_columns = sorted(required_columns - set(bronze_df.columns))

    return {
        "total_rows": total,
        "distinct_event_ids": distinct,
        "is_unique": total == distinct,
        "missing_columns": missing_columns,
        "columns_ok": len(missing_columns) == 0,
    }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("streaming-bronze")
        .master("local[*]")
        .getOrCreate()
    )
    configure_spark_performance(spark)
    spark.sparkContext.setLogLevel("WARN")

    result = run_streaming_bronze(spark, reset_state=True)
    print(
        f"written={result['written_count']} "
        f"distinct_event_ids={result['distinct_event_ids']} "
        f"unique={result['is_unique']}"
    )

    validation = validate_bronze_streaming(spark)
    status = "OK" if validation["is_unique"] and validation["columns_ok"] else "FAIL"
    print(f"validation [{status}]: {validation}")

    spark.stop()
