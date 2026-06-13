from src.cassandra.queries.constants import TABLE_NAME

QUERY_TOP_SERVICES = f"""
SELECT service, total_daily_cost_usd
FROM {TABLE_NAME}
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s
"""
