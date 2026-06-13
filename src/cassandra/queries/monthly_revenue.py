from src.cassandra.queries.constants import TABLE_REVENUE

QUERY_MONTHLY_REVENUE = f"""
SELECT
    billing_month,
    subtotal_usd,
    credits_usd,
    taxes_usd,
    revenue_usd,
    invoice_count
FROM {TABLE_REVENUE}
WHERE org_id = %s
  AND billing_month >= %s
  AND billing_month <= %s
"""
