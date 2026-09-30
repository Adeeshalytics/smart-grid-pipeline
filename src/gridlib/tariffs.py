"""Validation of the daily tariff/billing reference file (the batch source)."""
from __future__ import annotations

from typing import Any

from .config import BILLING_TIERS

TARIFF_COLUMNS = ("household_id", "tariff_rate", "billing_tier", "subsidy_flag")
MIN_TARIFF_RATE = 1.0
MAX_TARIFF_RATE = 200.0
MAX_REJECT_RATIO = 0.20

_TRUE = {"true", "1", "yes", "y"}
_FALSE = {"false", "0", "no", "n"}


def validate_tariff_row(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Return (clean_row, None) or (None, reject_reason)."""
    household_id = (row.get("household_id") or "").strip()
    if not household_id:
        return None, "missing_household_id"
    try:
        rate = float(row.get("tariff_rate"))
    except (TypeError, ValueError):
        return None, "tariff_rate_not_numeric"
    if not MIN_TARIFF_RATE <= rate <= MAX_TARIFF_RATE:
        return None, "tariff_rate_out_of_range"
    tier = (row.get("billing_tier") or "").strip().upper()
    if tier not in BILLING_TIERS:
        return None, "unknown_billing_tier"
    flag = str(row.get("subsidy_flag", "")).strip().lower()
    if flag not in _TRUE | _FALSE:
        return None, "subsidy_flag_not_boolean"
    return (
        {
            "household_id": household_id,
            "tariff_rate": rate,
            "billing_tier": tier,
            "subsidy_flag": flag in _TRUE,
        },
        None,
    )


def validate_tariff_file(rows: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Split a tariff file into clean rows and rejects."""
    clean: list[dict] = []
    rejects: list[dict] = []
    seen: set[str] = set()
    for line_no, row in enumerate(rows, start=2):  # line 1 is the CSV header
        cleaned, reason = validate_tariff_row(row)
        if cleaned and cleaned["household_id"] in seen:
            cleaned, reason = None, "duplicate_household"
        if cleaned:
            seen.add(cleaned["household_id"])
            clean.append(cleaned)
        else:
            rejects.append({"line_no": line_no, "reason": reason, "raw": dict(row)})
    return clean, rejects


def file_is_acceptable(clean_count: int, reject_count: int) -> bool:
    total = clean_count + reject_count
    return total > 0 and reject_count / total <= MAX_REJECT_RATIO
