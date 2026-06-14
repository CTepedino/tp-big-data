"""Prepared INSERT statements for Gold → Astra load."""

from src.cassandra.schema import (
    TABLE_GENAI,
    TABLE_ORG_DAILY,
    TABLE_REVENUE,
    TABLE_TICKETS,
    TABLE_TOP_SERVICES,
)

INSERT_ORG_DAILY = f"""
INSERT INTO {TABLE_ORG_DAILY} (
    org_id, usage_date, service,
    org_name, org_industry, org_plan_tier,
    event_count, total_daily_cost_usd, total_requests,
    total_genai_tokens, total_carbon_kg,
    anomaly_event_count, has_cost_anomaly, gold_ts
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""

INSERT_TOP_SERVICES = f"""
INSERT INTO {TABLE_TOP_SERVICES} (
    org_id, period_end, rank, service,
    period_start, accumulated_cost_usd
) VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_TICKETS = f"""
INSERT INTO {TABLE_TICKETS} (
    org_id, severity, ticket_date,
    org_name, ticket_count, sla_breach_count,
    sla_breach_rate, avg_csat
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_REVENUE = f"""
INSERT INTO {TABLE_REVENUE} (
    org_id, billing_month, org_name,
    subtotal_usd, credits_usd, taxes_usd,
    revenue_usd, invoice_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_GENAI = f"""
INSERT INTO {TABLE_GENAI} (
    org_id, usage_date, org_name,
    total_genai_tokens, estimated_cost_usd, event_count
) VALUES (?, ?, ?, ?, ?, ?)
"""

# Backward-compatible alias.
INSERT_CQL = INSERT_ORG_DAILY
