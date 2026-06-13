-- Query #1: daily costs and requests by org and service (date range).

USE cloud_analytics;

SELECT
    usage_date,
    service,
    total_daily_cost_usd,
    total_requests
FROM org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';
