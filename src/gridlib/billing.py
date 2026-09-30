"""Billing rules - the business logic that turns energy into money.

Rules (per household, per day):
  net_kwh         = consumption - solar generation
  import_kwh      = energy drawn from the grid      = max(net_kwh, 0)
  export_kwh      = surplus solar fed to the grid   = max(-net_kwh, 0)
  energy_charge   = import_kwh * tariff_rate * tier multiplier
  subsidy_discount= SUBSIDY_DISCOUNT * energy_charge   (only if subsidy_flag)
  export_credit   = export_kwh * tariff_rate * FEED_IN_FACTOR
  total_bill      = energy_charge - subsidy_discount - export_credit + fixed charge

`compute_bill` is the reference implementation. The Spark version in
`processing/transforms.py` is built from the same constants, and a test checks
that the two agree row by row.
"""
from __future__ import annotations

TIER_MULTIPLIER = {"TIER_1": 1.00, "TIER_2": 1.25, "TIER_3": 1.60}
SUBSIDY_DISCOUNT = 0.20
FEED_IN_FACTOR = 0.50
FIXED_DAILY_CHARGE = 15.0
CURRENCY = "LKR"


def compute_bill(
    consumption_kwh: float,
    solar_kwh: float,
    tariff_rate: float,
    billing_tier: str,
    subsidy_flag: bool,
) -> dict[str, float]:
    net_kwh = consumption_kwh - solar_kwh
    import_kwh = max(net_kwh, 0.0)
    export_kwh = max(-net_kwh, 0.0)
    energy_charge = import_kwh * tariff_rate * TIER_MULTIPLIER[billing_tier]
    subsidy_discount = energy_charge * SUBSIDY_DISCOUNT if subsidy_flag else 0.0
    export_credit = export_kwh * tariff_rate * FEED_IN_FACTOR
    total_bill = energy_charge - subsidy_discount - export_credit + FIXED_DAILY_CHARGE
    return {
        "net_kwh": round(net_kwh, 4),
        "import_kwh": round(import_kwh, 4),
        "export_kwh": round(export_kwh, 4),
        "energy_charge": round(energy_charge, 2),
        "subsidy_discount": round(subsidy_discount, 2),
        "export_credit": round(export_credit, 2),
        "fixed_charge": FIXED_DAILY_CHARGE,
        "total_bill": round(total_bill, 2),
    }
