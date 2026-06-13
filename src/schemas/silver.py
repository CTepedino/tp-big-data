"""Silver layer thresholds and constants."""

ZSCORE_THRESHOLD = 3.0
MAD_THRESHOLD = 3.5
MAD_MIN_FLOOR = 0.01
PERCENTILE_LOW = 0.01
PERCENTILE_HIGH = 0.99
FUTURE_EVENT_TOLERANCE_SEC = 300

QUARANTINE_REASONS = {
    "null_event_id": "null event_id",
    "duplicate_event_id": "duplicate event_id",
    "missing_unit_with_value": "null unit with non-null value",
    "orphan_org_id": "org_id not found in customers_orgs",
    "orphan_resource_id": "resource_id not found in resources",
    "null_primary_key": "null primary key",
    "late_arrival_exceeds_catchup": "event latency exceeds catch-up window",
    "future_event_ts": "event_ts beyond allowed future tolerance",
}
