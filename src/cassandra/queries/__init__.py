from src.cassandra.queries.constants import (
    TABLE_GENAI,
    TABLE_ORG_DAILY,
    TABLE_REVENUE,
    TABLE_TICKETS,
    TABLE_TOP_SERVICES,
)
from src.cassandra.queries.critical_tickets_sla import QUERY_CRITICAL_TICKETS
from src.cassandra.queries.daily_costs_and_requests import QUERY_DAILY_COSTS
from src.cassandra.queries.genai_tokens_daily import QUERY_GENAI_TOKENS_DAILY
from src.cassandra.queries.insert_genai_tokens_by_org_date import (
    INSERT_CQL as INSERT_GENAI,
)
from src.cassandra.queries.insert_org_daily_usage import INSERT_CQL as INSERT_ORG_DAILY
from src.cassandra.queries.insert_revenue_by_org_month import (
    INSERT_CQL as INSERT_REVENUE,
)
from src.cassandra.queries.insert_tickets_by_org_date import INSERT_CQL as INSERT_TICKETS
from src.cassandra.queries.monthly_revenue import QUERY_MONTHLY_REVENUE
from src.cassandra.queries.top_services_by_cost import (
    INSERT_CQL as INSERT_TOP_SERVICES,
    QUERY_TOP_SERVICES,
)

# Backward-compatible aliases.
TABLE_NAME = TABLE_ORG_DAILY
INSERT_CQL = INSERT_ORG_DAILY

__all__ = [
    "TABLE_NAME",
    "TABLE_ORG_DAILY",
    "TABLE_TOP_SERVICES",
    "TABLE_TICKETS",
    "TABLE_REVENUE",
    "TABLE_GENAI",
    "INSERT_CQL",
    "INSERT_ORG_DAILY",
    "INSERT_TOP_SERVICES",
    "INSERT_TICKETS",
    "INSERT_REVENUE",
    "INSERT_GENAI",
    "QUERY_DAILY_COSTS",
    "QUERY_TOP_SERVICES",
    "QUERY_CRITICAL_TICKETS",
    "QUERY_MONTHLY_REVENUE",
    "QUERY_GENAI_TOKENS_DAILY",
]
