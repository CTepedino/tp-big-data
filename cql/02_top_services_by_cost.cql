-- Query #2: top-N services by accumulated cost (last 14 days) for one org.
-- Step 1 (CQL): fetch daily costs in the window.
-- Step 2 (app): SUM(total_daily_cost_usd) GROUP BY service, ORDER BY cost DESC, LIMIT N.

USE cloud_analytics;

SELECT
    service,
    total_daily_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-08-18'
  AND usage_date <= '2025-08-31';
