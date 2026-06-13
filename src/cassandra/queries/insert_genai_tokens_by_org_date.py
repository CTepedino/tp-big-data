from src.cassandra.queries.constants import TABLE_GENAI

INSERT_CQL = f"""
INSERT INTO {TABLE_GENAI} (
    org_id, usage_date, org_name,
    total_genai_tokens, estimated_cost_usd, event_count
) VALUES (?, ?, ?, ?, ?, ?)
"""

QUERY_GENAI_TOKENS_DAILY = f"""
SELECT
    usage_date,
    total_genai_tokens,
    estimated_cost_usd,
    event_count
FROM {TABLE_GENAI}
WHERE org_id = %s
  AND usage_date >= %s
  AND usage_date <= %s
"""
