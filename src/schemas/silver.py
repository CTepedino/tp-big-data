"""Silver layer thresholds and constants."""

# Consigna: cost_usd_increment ∈ [-0.01, +∞)
COST_USD_INCREMENT_MIN = -0.01
# Consigna: flag de anomalía si supera p99 * X
P99_ANOMALY_MULTIPLIER = 2.0

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
    "cost_below_minimum": "cost_usd_increment below -0.01",
    "invalid_service": "service not in allowed cloud catalog",
    "invalid_region": "region not in allowed cloud catalog",
    "null_event_ts": "event_ts missing or unparseable",
    "orphan_org_id": "org_id not found in customers_orgs",
    "orphan_resource_id": "resource_id not found in resources",
    "null_primary_key": "null primary key",
    "late_arrival_exceeds_catchup": "event latency exceeds catch-up window",
    "future_event_ts": "event_ts beyond allowed future tolerance",
}
