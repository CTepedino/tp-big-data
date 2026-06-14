-- Query #2: top-N services by accumulated cost (pre-aggregated, pure CQL).
-- period_end = último usage_date en Gold (ventana rolling de 14 días inclusive).

USE cloud_analytics;

SELECT service, accumulated_cost_usd, period_start, period_end, rank
FROM org_top_services_by_cost
WHERE org_id = 'org_rixa11dp'
  AND period_end = '2025-08-31'  -- reemplazar con max(period_end) del mart Gold / Astra
ORDER BY rank ASC
LIMIT 5;
