"""Bronze streaming schema and streaming parameters."""

import os

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

USAGE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=True),
        StructField("org_id", StringType(), nullable=True),
        StructField("resource_id", StringType(), nullable=True),
        StructField("service", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("metric", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("unit", StringType(), nullable=True),
        StructField("cost_usd_increment", DoubleType(), nullable=True),
        StructField("schema_version", IntegerType(), nullable=True),
        StructField("carbon_kg", DoubleType(), nullable=True),
        StructField("genai_tokens", LongType(), nullable=True),
    ]
)

WATERMARK_DELAY_PRODUCTION = "10 minutes"
WATERMARK_DELAY = os.environ.get("STREAMING_WATERMARK", "60 days")
LATE_DATA_THRESHOLD_SEC = 600
