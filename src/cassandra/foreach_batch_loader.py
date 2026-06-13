"""Load Gold Parquet into Cassandra via Structured Streaming foreachBatch."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.config import CHECKPOINTS

DEFAULT_CONCURRENCY = int(os.environ.get("CASSANDRA_LOAD_CONCURRENCY", "50"))


def _write_batch_with_driver(
    batch_df: DataFrame,
    batch_id: int,
    *,
    insert_cql: str,
    row_to_params: Callable[[dict[str, Any]], tuple],
    session,
    table_name: str,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> int:
    from cassandra.concurrent import execute_concurrent_with_args

    if batch_df.rdd.isEmpty():
        return 0

    params = [row_to_params(row.asDict()) for row in batch_df.collect()]
    prepared = session.prepare(insert_cql)
    inserted = 0
    errors = 0

    for success, result in execute_concurrent_with_args(
        session,
        prepared,
        params,
        concurrency=concurrency,
        raise_on_first_error=False,
    ):
        if success:
            inserted += 1
        else:
            errors += 1
            if errors <= 3:
                print(
                    f"  foreachBatch INSERT error ({table_name}, batch={batch_id}): "
                    f"{result}",
                    file=sys.stderr,
                )

    if errors:
        raise RuntimeError(
            f"foreachBatch load failed for {table_name} "
            f"(batch={batch_id}): {errors} insert errors"
        )
    return inserted


def load_parquet_via_foreach_batch(
    spark: SparkSession,
    gold_path: str,
    table_name: str,
    *,
    insert_cql: str,
    row_to_params: Callable[[dict[str, Any]], tuple],
    session,
    transform: Callable[[DataFrame], DataFrame] | None = None,
    checkpoint_subdir: str | None = None,
) -> dict[str, Any]:
    """Read Gold Parquet as a stream and upsert each micro-batch to Cassandra."""
    checkpoint = os.path.join(
        CHECKPOINTS,
        checkpoint_subdir or f"serving_foreach_{table_name}",
    )
    schema = spark.read.parquet(gold_path).schema
    total_rows = spark.read.parquet(gold_path).count()
    inserted_total = 0

    def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
        nonlocal inserted_total
        working_df = transform(batch_df) if transform else batch_df
        inserted_total += _write_batch_with_driver(
            working_df,
            batch_id,
            insert_cql=insert_cql,
            row_to_params=row_to_params,
            session=session,
            table_name=table_name,
        )

    print(
        f"  foreachBatch load {table_name}: {total_rows} rows "
        f"from {gold_path} (checkpoint={checkpoint})"
    )

    query = (
        spark.readStream.schema(schema)
        .format("parquet")
        .load(gold_path)
        .writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    return {
        "table_name": table_name,
        "gold_path": gold_path,
        "gold_row_count": total_rows,
        "rows_upserted": inserted_total,
        "load_mode": "structured_streaming_foreachBatch",
        "idempotent_ok": inserted_total == total_rows,
    }


def load_dataframe_via_foreach_batch(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    *,
    insert_cql: str,
    row_to_params: Callable[[dict[str, Any]], tuple],
    session,
    checkpoint_subdir: str | None = None,
) -> dict[str, Any]:
    """Stage a DataFrame to Parquet and load it with foreachBatch."""
    staging_path = os.path.join(CHECKPOINTS, "staging", table_name)
    df.write.mode("overwrite").parquet(staging_path)
    return load_parquet_via_foreach_batch(
        spark,
        staging_path,
        table_name,
        insert_cql=insert_cql,
        row_to_params=row_to_params,
        session=session,
        checkpoint_subdir=checkpoint_subdir,
    )
