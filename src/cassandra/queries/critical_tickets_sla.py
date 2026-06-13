from src.cassandra.queries.constants import TABLE_TICKETS

QUERY_CRITICAL_TICKETS = f"""
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
