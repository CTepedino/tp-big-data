-- Query #2: rows for top-N services by accumulated cost (aggregate in app).
-- Astra Console example:
--   org_id = 'org_rixa11dp', start = '2025-08-17', end = '2025-08-31'

SELECT
    service,
    usage_date,
    total_daily_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s;
