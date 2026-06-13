-- Consultas mínimas de demostración (consigna #1 y #2)
-- Reemplazar :org_id y fechas según el dataset.

USE cloud_analytics;

-- #1 Costos y requests diarios por organización y servicio en un rango de fechas
SELECT
    usage_date,
    service,
    total_daily_cost_usd,
    total_requests,
    total_genai_tokens,
    total_carbon_kg
FROM org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';

-- #2 Top servicios por costo acumulado en los últimos 14 días para una organización
-- Cassandra devuelve filas por (org_id, usage_date, service); el top-N se calcula
-- agregando por service en el notebook/cliente (ver serving_cassandra.py).
SELECT
    service,
    usage_date,
    total_daily_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-08-17'
  AND usage_date <= '2025-08-31';
