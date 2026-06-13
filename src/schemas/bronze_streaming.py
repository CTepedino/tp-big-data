"""Schema explícito para ingesta streaming Bronze desde JSONL."""

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

# Producción near-real-time (diseño entrega 1)
WATERMARK_DELAY_PRODUCTION = "10 minutes"

# Replay del landing estático (~60 días): watermark amplio para no descartar histórico
# en un único availableNow. En vivo, usar WATERMARK_DELAY_PRODUCTION.
WATERMARK_DELAY = os.environ.get("STREAMING_WATERMARK", "60 days")

# Umbral de negocio para marcar late arrivals (independiente del watermark técnico)
LATE_DATA_THRESHOLD_SEC = 600
