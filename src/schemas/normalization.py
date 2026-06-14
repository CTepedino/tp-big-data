"""String, service, region and date normalization for Silver conformance."""

from pyspark.sql import Column
from pyspark.sql import functions as F

ALLOWED_CLOUD_SERVICES = frozenset(
    {
        "analytics",
        "compute",
        "database",
        "genai",
        "networking",
        "storage",
    }
)

ALLOWED_CLOUD_REGIONS = frozenset(
    {
        "ap-northeast",
        "ap-south",
        "eu-central",
        "eu-west",
        "sa-east",
        "us-east",
        "us-west",
    }
)

SERVICE_ALIASES: dict[str, str] = {
    "ai": "genai",
    "computing": "compute",
    "databases": "database",
    "network": "networking",
    "object-storage": "storage",
}

REGION_COLUMN_NAMES = frozenset({"hq_region", "region", "resource_region"})
SERVICE_COLUMN_NAMES = frozenset({"service", "resource_service"})


def normalized_string(column: Column) -> Column:
    trimmed = F.trim(column.cast("string"))
    lowered = F.lower(trimmed)
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(None)).otherwise(lowered)


def normalized_region(column: Column) -> Column:
    base = normalized_string(column)
    return F.when(base.isNull(), F.lit(None)).otherwise(F.regexp_replace(base, "_", "-"))


def normalized_service(column: Column) -> Column:
    base = normalized_string(column)
    mapped = base
    for alias, canonical in SERVICE_ALIASES.items():
        mapped = F.when(base == F.lit(alias), F.lit(canonical)).otherwise(mapped)
    return mapped


def normalize_column_expr(column_name: str) -> Column:
    column = F.col(column_name)
    if column_name in REGION_COLUMN_NAMES:
        return normalized_region(column)
    if column_name in SERVICE_COLUMN_NAMES:
        return normalized_service(column)
    return normalized_string(column)


def invalid_service_condition() -> Column:
    services = list(ALLOWED_CLOUD_SERVICES)
    return F.col("service").isNotNull() & ~F.col("service").isin(services)


def invalid_region_condition() -> Column:
    regions = list(ALLOWED_CLOUD_REGIONS)
    return F.col("region").isNotNull() & ~F.col("region").isin(regions)


def normalized_event_ts(column: Column) -> Column:
    """Parse event time; invalid strings become null."""
    return F.to_timestamp(column)
