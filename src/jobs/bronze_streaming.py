"""Job streaming: Landing JSONL -> Bronze Parquet."""

from __future__ import annotations

import os
import shutil
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.config import BRONZE, CHECKPOINTS, LANDING
from src.schemas.bronze_streaming import (
    LATE_DATA_THRESHOLD_SEC,
    USAGE_EVENTS_SCHEMA,
    WATERMARK_DELAY,
    WATERMARK_DELAY_PRODUCTION,
)

USAGE_EVENTS_LANDING_GLOB = os.path.join(LANDING, "usage_events_stream", "*.jsonl")
USAGE_EVENTS_BRONZE_PATH = os.path.join(BRONZE, "usage_events")
USAGE_EVENTS_CHECKPOINT_PATH = os.path.join(CHECKPOINTS, "usage_events_bronze")


def _streaming_relative_source_file() -> Column:
    """Ruta lógica relativa, p. ej. landing/usage_events_stream/events_part_0001.jsonl."""
    filename = F.regexp_extract(F.input_file_name(), r"([^/]+\.jsonl)$", 1)
    return F.concat(F.lit("landing/usage_events_stream/"), filename)


def _parse_event_value() -> Column:
    """Normaliza value mixto (numérico/string/null) a double nullable."""
    as_string = F.trim(F.col("value").cast("string"))
    return (
        F.when(as_string.isNull() | (as_string == ""), F.lit(None))
        .otherwise(as_string.cast("double"))
    )


def transform_usage_events_bronze(df: DataFrame) -> DataFrame:
    """Tipifica, audita, aplica watermark, deduplica y marca late arrivals."""
    df = (
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("value_numeric", _parse_event_value())
        .drop("value")
        .withColumnRenamed("value_numeric", "value")
        .withColumn("source_file", _streaming_relative_source_file())
        .withColumn("ingest_ts", F.current_timestamp())
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

    df = df.withWatermark("event_ts", WATERMARK_DELAY)
    return df.dropDuplicates(["event_id"])


def build_usage_events_stream(spark: SparkSession) -> DataFrame:
    """Construye el stream de lectura desde landing."""
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
    """Inicia la query de escritura Bronze con checkpoint habilitado."""
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


def reset_streaming_state(
    *,
    bronze_path: str = USAGE_EVENTS_BRONZE_PATH,
    checkpoint_path: str = USAGE_EVENTS_CHECKPOINT_PATH,
) -> None:
    """Limpia salida y checkpoint para una corrida limpia en desarrollo."""
    for path in (bronze_path, checkpoint_path):
        if os.path.isdir(path):
            shutil.rmtree(path)


def run_streaming_bronze(
    spark: SparkSession,
    *,
    reset_state: bool = False,
    trigger: str = "availableNow",
) -> dict[str, Any]:
    """Ejecuta streaming Bronze hasta completar y devuelve métricas."""
    if reset_state:
        reset_streaming_state()

    os.makedirs(os.path.dirname(USAGE_EVENTS_CHECKPOINT_PATH), exist_ok=True)

    query = start_usage_events_bronze_query(spark, trigger=trigger)
    query.awaitTermination()

    bronze_df = spark.read.parquet(USAGE_EVENTS_BRONZE_PATH)
    total_rows = bronze_df.count()
    distinct_event_ids = bronze_df.select("event_id").distinct().count()
    late_arrivals = bronze_df.filter(F.col("is_late_arrival")).count()
    schema_v2_rows = bronze_df.filter(F.col("schema_version") == 2).count()

    return {
        "landing_glob": USAGE_EVENTS_LANDING_GLOB,
        "bronze_path": USAGE_EVENTS_BRONZE_PATH,
        "checkpoint_path": USAGE_EVENTS_CHECKPOINT_PATH,
        "watermark_delay": WATERMARK_DELAY,
        "watermark_delay_production": WATERMARK_DELAY_PRODUCTION,
        "late_data_threshold_sec": LATE_DATA_THRESHOLD_SEC,
        "written_count": total_rows,
        "distinct_event_ids": distinct_event_ids,
        "is_unique": total_rows == distinct_event_ids,
        "late_arrivals": late_arrivals,
        "schema_v2_rows": schema_v2_rows,
    }


def validate_bronze_streaming(spark: SparkSession) -> dict[str, Any]:
    """Valida unicidad de event_id y presencia de columnas técnicas en Bronze."""
    bronze_df = spark.read.parquet(USAGE_EVENTS_BRONZE_PATH)
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
    spark.sparkContext.setLogLevel("WARN")

    print(f"LANDING glob:   {USAGE_EVENTS_LANDING_GLOB}")
    print(f"BRONZE path:    {USAGE_EVENTS_BRONZE_PATH}")
    print(f"CHECKPOINT:     {USAGE_EVENTS_CHECKPOINT_PATH}")
    print(f"WATERMARK:      {WATERMARK_DELAY}")

    result = run_streaming_bronze(spark, reset_state=True)
    print(
        f"written={result['written_count']} "
        f"distinct_event_ids={result['distinct_event_ids']} "
        f"unique={result['is_unique']}"
    )
    print(f"late_arrivals={result['late_arrivals']} schema_v2_rows={result['schema_v2_rows']}")

    validation = validate_bronze_streaming(spark)
    status = "OK" if validation["is_unique"] and validation["columns_ok"] else "FAIL"
    print(f"validation [{status}]: {validation}")

    spark.stop()
