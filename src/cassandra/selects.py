"""Parameterized SELECT statements (mirrors cql/01–05)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from src.cassandra.schema import (
    DEFAULT_TOP_N,
    TABLE_GENAI,
    TABLE_ORG_DAILY,
    TABLE_REVENUE,
    TABLE_TICKETS,
    TABLE_TOP_SERVICES,
)


@dataclass(frozen=True)
class CqlSelect:
    """Prepared SELECT plus literal CQL for notebook / demo display."""

    prepared: str
    params: tuple[Any, ...]
    literal: str

    def execute(self, session) -> list[dict[str, Any]]:
        rows = session.execute(self.prepared, self.params)
        return [dict(row._asdict()) for row in rows]


def _as_date(value: str | date) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(value)


def daily_costs_and_requests(
    org_id: str,
    start_date: str | date,
    end_date: str | date,
) -> CqlSelect:
    start = _as_date(start_date)
    end = _as_date(end_date)
    prepared = f"""
SELECT usage_date, service, total_daily_cost_usd, total_requests
FROM {TABLE_ORG_DAILY}
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s
"""
    literal = f"""
SELECT
    usage_date,
    service,
    total_daily_cost_usd,
    total_requests
FROM {TABLE_ORG_DAILY}
WHERE org_id = '{org_id}'
  AND usage_date >= '{start.isoformat()}'
  AND usage_date <= '{end.isoformat()}';
"""
    return CqlSelect(prepared, (org_id, start, end), literal.strip())


def top_services_by_cost(
    org_id: str,
    period_end: str | date,
    top_n: int = DEFAULT_TOP_N,
) -> CqlSelect:
    period_end_date = _as_date(period_end)
    prepared = f"""
SELECT service, accumulated_cost_usd, period_start, period_end, rank
FROM {TABLE_TOP_SERVICES}
WHERE org_id = %s
  AND period_end = %s
ORDER BY rank ASC
LIMIT %s
"""
    literal = f"""
SELECT
    service,
    accumulated_cost_usd,
    period_start,
    period_end,
    rank
FROM {TABLE_TOP_SERVICES}
WHERE org_id = '{org_id}'
  AND period_end = '{period_end_date.isoformat()}'
ORDER BY rank ASC
LIMIT {top_n};
"""
    return CqlSelect(
        prepared,
        (org_id, period_end_date, top_n),
        literal.strip(),
    )


def critical_tickets_sla(
    org_id: str,
    severity: str,
    start_date: str | date,
    end_date: str | date,
) -> CqlSelect:
    start = _as_date(start_date)
    end = _as_date(end_date)
    prepared = f"""
SELECT
    ticket_date,
    ticket_count,
    sla_breach_count,
    sla_breach_rate,
    avg_csat
FROM {TABLE_TICKETS}
WHERE org_id = %s
  AND severity = %s
  AND ticket_date >= %s
  AND ticket_date <= %s
"""
    literal = f"""
SELECT
    ticket_date,
    ticket_count,
    sla_breach_count,
    sla_breach_rate,
    avg_csat
FROM {TABLE_TICKETS}
WHERE org_id = '{org_id}'
  AND severity = '{severity}'
  AND ticket_date >= '{start.isoformat()}'
  AND ticket_date <= '{end.isoformat()}';
"""
    return CqlSelect(prepared, (org_id, severity, start, end), literal.strip())


def monthly_revenue(
    org_id: str,
    start_month: str | date,
    end_month: str | date,
) -> CqlSelect:
    start = _as_date(start_month)
    end = _as_date(end_month)
    prepared = f"""
SELECT
    billing_month,
    subtotal_usd,
    credits_usd,
    taxes_usd,
    revenue_usd,
    invoice_count
FROM {TABLE_REVENUE}
WHERE org_id = %s
  AND billing_month >= %s
  AND billing_month <= %s
"""
    literal = f"""
SELECT
    billing_month,
    subtotal_usd,
    credits_usd,
    taxes_usd,
    revenue_usd,
    invoice_count
FROM {TABLE_REVENUE}
WHERE org_id = '{org_id}'
  AND billing_month >= '{start.isoformat()}'
  AND billing_month <= '{end.isoformat()}';
"""
    return CqlSelect(prepared, (org_id, start, end), literal.strip())


def genai_tokens_daily(
    org_id: str,
    start_date: str | date,
    end_date: str | date,
) -> CqlSelect:
    start = _as_date(start_date)
    end = _as_date(end_date)
    prepared = f"""
SELECT
    usage_date,
    total_genai_tokens,
    estimated_cost_usd
FROM {TABLE_GENAI}
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s
"""
    literal = f"""
SELECT
    usage_date,
    total_genai_tokens,
    estimated_cost_usd
FROM {TABLE_GENAI}
WHERE org_id = '{org_id}'
  AND usage_date >= '{start.isoformat()}'
  AND usage_date <= '{end.isoformat()}';
"""
    return CqlSelect(prepared, (org_id, start, end), literal.strip())
