"""Cassandra table names and serving defaults."""

TABLE_ORG_DAILY = "org_daily_usage_by_service"
TABLE_TOP_SERVICES = "org_top_services_by_cost"
TABLE_TICKETS = "tickets_by_org_date"
TABLE_REVENUE = "revenue_by_org_month"
TABLE_GENAI = "genai_tokens_by_org_date"

# Backward-compatible alias used by legacy code.
TABLE_NAME = TABLE_ORG_DAILY

DEFAULT_TOP_N = 5
TOP_SERVICES_LOOKBACK_DAYS = 14
TICKETS_CRITICAL_LOOKBACK_DAYS = 30
