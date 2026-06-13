-- Query #4: monthly revenue with credits and taxes (USD).

USE cloud_analytics;

SELECT
    billing_month,
    subtotal_usd,
    credits_usd,
    taxes_usd,
    revenue_usd,
    invoice_count
FROM revenue_by_org_month
WHERE org_id = 'org_rixa11dp'
  AND billing_month >= '2025-06-01'
  AND billing_month <= '2025-08-01';
