-- Tabla query-first para el mart Gold org_daily_usage_by_service
-- PK: ((org_id), usage_date DESC, service ASC)
-- Consulta #1: costos/requests diarios por org+servicio en rango de fechas
-- Consulta #2: top-N servicios por costo acumulado para una org (agregar en cliente)

USE cloud_analytics;

CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    org_name text,
    org_industry text,
    org_plan_tier text,
    event_count int,
    total_daily_cost_usd double,
    total_requests double,
    total_genai_tokens bigint,
    total_carbon_kg double,
    anomaly_event_count int,
    has_cost_anomaly boolean,
    gold_ts timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
