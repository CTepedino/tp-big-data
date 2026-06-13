-- Keyspace must exist in Astra console before running this script.

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
