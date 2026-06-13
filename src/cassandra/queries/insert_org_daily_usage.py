from src.cassandra.queries.constants import TABLE_NAME

INSERT_CQL = f"""
INSERT INTO {TABLE_NAME} (
    org_id, usage_date, service,
    org_name, org_industry, org_plan_tier,
    event_count, total_daily_cost_usd, total_requests,
    total_genai_tokens, total_carbon_kg,
    anomaly_event_count, has_cost_anomaly, gold_ts
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""
