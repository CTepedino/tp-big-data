from src.cassandra.queries.constants import TABLE_TICKETS

INSERT_CQL = f"""
INSERT INTO {TABLE_TICKETS} (
    org_id, severity, ticket_date,
    org_name, ticket_count, sla_breach_count,
    sla_breach_rate, avg_csat
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
