from src.cassandra.queries.constants import TABLE_NAME

QUERY_DAILY_COSTS = f"""
SELECT usage_date, service, total_daily_cost_usd, total_requests
FROM {TABLE_NAME}
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s
"""
