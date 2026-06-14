-- Query #2: top-N services by accumulated cost (pre-aggregated, pure CQL).
-- period_end = latest usage_date in Gold (rolling 14-day window inclusive).

USE cloud_analytics;

SELECT service, accumulated_cost_usd, period_start, period_end, rank
FROM org_top_services_by_cost
WHERE org_id = 'org_rixa11dp'
  AND period_end = '2025-08-31'
ORDER BY rank ASC
LIMIT 4;
