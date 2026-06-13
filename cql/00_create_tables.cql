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

CREATE TABLE IF NOT EXISTS org_top_services_by_cost (
    org_id text,
    period_end date,
    rank int,
    service text,
    period_start date,
    accumulated_cost_usd double,
    PRIMARY KEY ((org_id, period_end), rank, service)
) WITH CLUSTERING ORDER BY (rank ASC, service ASC);

DROP TABLE IF EXISTS tickets_by_org_date;

CREATE TABLE IF NOT EXISTS tickets_by_org_date (
    org_id text,
    severity text,
    ticket_date date,
    org_name text,
    ticket_count int,
    sla_breach_count int,
    sla_breach_rate double,
    avg_csat double,
    PRIMARY KEY ((org_id, severity), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id text,
    billing_month date,
    org_name text,
    subtotal_usd double,
    credits_usd double,
    taxes_usd double,
    revenue_usd double,
    invoice_count int,
    PRIMARY KEY ((org_id), billing_month)
) WITH CLUSTERING ORDER BY (billing_month DESC);

CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id text,
    usage_date date,
    org_name text,
    total_genai_tokens bigint,
    estimated_cost_usd double,
    event_count int,
    PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
