"""Silver layer thresholds and constants."""

COST_ANOMALY_THRESHOLD = -0.01

QUARANTINE_REASONS = {
    "null_event_id": "null event_id",
    "duplicate_event_id": "duplicate event_id",
    "missing_unit_with_value": "null unit with non-null value",
    "orphan_org_id": "org_id not found in customers_orgs",
}
