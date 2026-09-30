"""Event contract for smart-meter readings, and the validation rules.

The limits below are the single source of truth: the Spark transformations in
`processing/transforms.py` build their column expressions from these constants,
and the pure-Python `validate_reading` is what the unit tests exercise.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import GRID_ZONES

SCHEMA_VERSION = 1

REQUIRED_FIELDS = (
    "event_id",
    "meter_id",
    "household_id",
    "grid_zone",
    "power_consumption_kwh",
    "solar_generation_kwh",
    "timestamp",
)

# A household meter cannot plausibly record more than this in one 15-minute interval.
MAX_KWH_PER_READING = 10.0

# Reject reasons (also the values stored in the dead-letter topic).
REJECT_MALFORMED_JSON = "malformed_json"
REJECT_MISSING_FIELD = "missing_field"
REJECT_BAD_TIMESTAMP = "bad_timestamp"
REJECT_UNKNOWN_ZONE = "unknown_zone"
REJECT_OUT_OF_RANGE = "value_out_of_range"


def validate_reading(event: Any) -> str | None:
    """Return None if the event is valid, otherwise the reject reason."""
    if not isinstance(event, dict):
        return REJECT_MALFORMED_JSON
    if any(event.get(f) is None for f in REQUIRED_FIELDS):
        return REJECT_MISSING_FIELD
    try:
        datetime.fromisoformat(str(event["timestamp"]))
    except ValueError:
        return REJECT_BAD_TIMESTAMP
    if event["grid_zone"] not in GRID_ZONES:
        return REJECT_UNKNOWN_ZONE
    try:
        consumption = float(event["power_consumption_kwh"])
        solar = float(event["solar_generation_kwh"])
    except (TypeError, ValueError):
        return REJECT_OUT_OF_RANGE
    if not (0 <= consumption <= MAX_KWH_PER_READING and 0 <= solar <= MAX_KWH_PER_READING):
        return REJECT_OUT_OF_RANGE
    return None
