"""Optional Spark Cassandra Connector helpers (Spark 3.5.x)."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import (
    ASTRA_DB_APPLICATION_TOKEN,
    ASTRA_DB_SECURE_BUNDLE_PATH,
    CASSANDRA_KEYSPACE,
)

SPARK_CASSANDRA_CONNECTOR_PACKAGE = os.environ.get(
    "SPARK_CASSANDRA_CONNECTOR_PACKAGE",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
)


def apply_connector_spark_conf(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Attach Astra + Cassandra connector settings to a Spark session builder."""
    bundle = Path(ASTRA_DB_SECURE_BUNDLE_PATH).resolve()
    return (
        builder.config("spark.jars.packages", SPARK_CASSANDRA_CONNECTOR_PACKAGE)
        .config("spark.cassandra.connection.config.cloud.path", str(bundle))
        .config("spark.cassandra.auth.username", "token")
        .config("spark.cassandra.auth.password", ASTRA_DB_APPLICATION_TOKEN)
        .config(
            "spark.sql.extensions",
            "com.datastax.spark.connector.CassandraSparkExtensions",
        )
    )


def write_dataframe_with_connector(
    df,
    table_name: str,
    *,
    keyspace: str = CASSANDRA_KEYSPACE,
    mode: str = "append",
) -> None:
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .mode(mode)
        .options(keyspace=keyspace, table=table_name)
        .save()
    )
