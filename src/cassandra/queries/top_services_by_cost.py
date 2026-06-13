from src.cassandra.queries.constants import TABLE_TOP_SERVICES

INSERT_CQL = f"""
INSERT INTO {TABLE_TOP_SERVICES} (
    org_id, period_end, rank, service,
    period_start, accumulated_cost_usd
) VALUES (?, ?, ?, ?, ?, ?)
"""

QUERY_TOP_SERVICES = f"""
SELECT service, accumulated_cost_usd
FROM {TABLE_TOP_SERVICES}
WHERE org_id = %s
  AND period_end = %s
ORDER BY rank ASC
LIMIT %s
"""
