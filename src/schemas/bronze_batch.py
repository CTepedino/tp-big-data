"""Explicit CSV schemas for batch Bronze ingestion."""

from pyspark.sql.types import StringType, StructField, StructType

CUSTOMERS_ORGS_SCHEMA = StructType(
    [
        StructField("org_id", StringType(), nullable=False),
        StructField("org_name", StringType(), nullable=True),
        StructField("industry", StringType(), nullable=True),
        StructField("hq_region", StringType(), nullable=True),
        StructField("plan_tier", StringType(), nullable=True),
        StructField("is_enterprise", StringType(), nullable=True),
        StructField("signup_date", StringType(), nullable=True),
        StructField("sales_rep", StringType(), nullable=True),
        StructField("lifecycle_stage", StringType(), nullable=True),
        StructField("marketing_source", StringType(), nullable=True),
        StructField("nps_score", StringType(), nullable=True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("org_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("active", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("last_login", StringType(), nullable=True),
    ]
)

BILLING_MONTHLY_SCHEMA = StructType(
    [
        StructField("invoice_id", StringType(), nullable=False),
        StructField("org_id", StringType(), nullable=True),
        StructField("month", StringType(), nullable=True),
        StructField("subtotal", StringType(), nullable=True),
        StructField("credits", StringType(), nullable=True),
        StructField("taxes", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("exchange_rate_to_usd", StringType(), nullable=True),
    ]
)
