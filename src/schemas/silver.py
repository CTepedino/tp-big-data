"""Umbrales y constantes de la capa Silver."""

COST_ANOMALY_THRESHOLD = -0.01

QUARANTINE_REASONS = {
    "null_event_id": "event_id nulo",
    "duplicate_event_id": "event_id duplicado",
    "missing_unit_with_value": "unit nulo cuando value tiene dato",
    "orphan_org_id": "org_id no existe en customers_orgs",
}
