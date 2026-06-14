-- Query #3: critical tickets (high severity) and SLA breach rate by day.
-- ticket_date range = rolling 30 días hasta max(ticket_date) en Gold (ver demo.py).

USE cloud_analytics;

SELECT
    ticket_date,
    ticket_count,
    sla_breach_count,
    sla_breach_rate,
    avg_csat
FROM tickets_by_org_date
WHERE org_id = 'org_rixa11dp'
  AND severity = 'high'
  AND ticket_date >= '2025-08-02'
  AND ticket_date <= '2025-08-31';
