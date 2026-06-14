-- Query #5: GenAI tokens and estimated cost by day.

USE cloud_analytics;

SELECT
    usage_date,
    total_genai_tokens,
    estimated_cost_usd
FROM genai_tokens_by_org_date
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-08-01'
  AND usage_date <= '2025-08-31';
