-- Query #1: daily costs and requests by org and service (date range).
-- Astra Console example:
--   org_id = 'org_rixa11dp', start = '2025-07-01', end = '2025-08-31'

SELECT
    usage_date,
    service,
    total_daily_cost_usd,
    total_requests,
    total_genai_tokens,
    total_carbon_kg
FROM org_daily_usage_by_service
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s;
