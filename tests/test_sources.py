"""Tests for the simulated sources and their contracts (no Spark, no services)."""
import csv
import io
import json
import random
from datetime import date, datetime, timedelta, timezone

import pytest

from gridlib import schemas
from gridlib.config import Settings
from gridlib.fleet import build_households, cloud_factor, make_reading_values, solar_kwh
from gridlib.simclock import SimClock, floor_to_day_boundary
from gridlib.tariffs import file_is_acceptable, validate_tariff_file, validate_tariff_row

VALID = {
    "event_id": "e-1",
    "meter_id": "MTR-0001",
    "household_id": "HH-0001",
    "grid_zone": "ZONE-NORTH",
    "power_consumption_kwh": 0.25,
    "solar_generation_kwh": 0.0,
    "timestamp": "2026-01-01T10:15:00+00:00",
}


def test_valid_reading_passes():
    assert schemas.validate_reading(VALID) is None


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"power_consumption_kwh": None}, schemas.REJECT_MISSING_FIELD),
        ({"timestamp": "not-a-timestamp"}, schemas.REJECT_BAD_TIMESTAMP),
        ({"grid_zone": "ZONE-UNKNOWN"}, schemas.REJECT_UNKNOWN_ZONE),
        ({"power_consumption_kwh": -1.0}, schemas.REJECT_OUT_OF_RANGE),
        ({"solar_generation_kwh": 99.0}, schemas.REJECT_OUT_OF_RANGE),
    ],
)
def test_invalid_readings_are_rejected_with_a_reason(change, reason):
    assert schemas.validate_reading({**VALID, **change}) == reason


def test_non_object_payload_is_malformed():
    assert schemas.validate_reading("garbage") == schemas.REJECT_MALFORMED_JSON


def test_fleet_is_deterministic_for_a_seed():
    settings = Settings()
    assert build_households(settings) == build_households(settings)
    assert len(build_households(settings)) == settings.num_households


def test_simulated_readings_satisfy_the_contract():
    settings = Settings()
    rng = random.Random(1)
    ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    for hh in build_households(settings):
        consumption, solar = make_reading_values(hh, ts, settings, rng)
        event = {**VALID, "power_consumption_kwh": consumption, "solar_generation_kwh": solar, "grid_zone": hh.grid_zone}
        assert schemas.validate_reading(event) is None


def test_corrupt_events_are_all_rejected():
    from simulators.stream_meter_simulator import corrupt

    rng = random.Random(7)
    for _ in range(200):
        payload = corrupt(dict(VALID), rng)
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        assert schemas.validate_reading(event) is not None


def test_no_solar_at_night_and_weather_is_reproducible():
    assert solar_kwh(hour=2.0, kw_peak=3.0, cloud=1.0, interval_hours=0.25) == 0.0
    assert solar_kwh(hour=12.0, kw_peak=3.0, cloud=1.0, interval_hours=0.25) == pytest.approx(0.75)
    day = date(2026, 1, 5)
    assert cloud_factor("ZONE-EAST", day, 8203, 0.25) == cloud_factor("ZONE-EAST", day, 8203, 0.25)
    assert 0.05 <= cloud_factor("ZONE-EAST", day, 8203, 0.25) <= 1.0


def test_clock_maps_five_minutes_to_one_day():
    anchor = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    clock = SimClock(anchor, datetime(2026, 1, 1, tzinfo=timezone.utc), day_seconds=300)
    assert clock.speedup == 288
    assert clock.to_sim(anchor + timedelta(minutes=5)) == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert clock.day_index(anchor + timedelta(minutes=12)) == 2
    assert clock.sim_date(2) == date(2026, 1, 3)
    assert clock.day_end_wall(0) == anchor + timedelta(minutes=5)
    sim = datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)
    assert clock.to_sim(clock.to_wall(sim)) == sim


def test_anchor_is_aligned_to_the_schedule():
    wall = datetime(2026, 9, 30, 12, 3, 41, tzinfo=timezone.utc)
    assert floor_to_day_boundary(wall, 300) == datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def test_tariff_row_validation():
    good = {"household_id": "HH-0001", "tariff_rate": "30.5", "billing_tier": "tier_2", "subsidy_flag": "true"}
    clean, reason = validate_tariff_row(good)
    assert reason is None
    assert clean == {"household_id": "HH-0001", "tariff_rate": 30.5, "billing_tier": "TIER_2", "subsidy_flag": True}
    assert validate_tariff_row({**good, "tariff_rate": ""})[1] == "tariff_rate_not_numeric"
    assert validate_tariff_row({**good, "tariff_rate": "-5"})[1] == "tariff_rate_out_of_range"
    assert validate_tariff_row({**good, "billing_tier": "TIER_9"})[1] == "unknown_billing_tier"
    assert validate_tariff_row({**good, "subsidy_flag": "maybe"})[1] == "subsidy_flag_not_boolean"
    assert validate_tariff_row({**good, "household_id": " "})[1] == "missing_household_id"


def test_duplicate_households_are_rejected_after_the_first():
    row = {"household_id": "HH-0001", "tariff_rate": "30", "billing_tier": "TIER_1", "subsidy_flag": "false"}
    clean, rejects = validate_tariff_file([row, dict(row)])
    assert len(clean) == 1
    assert rejects[0]["reason"] == "duplicate_household"
    assert rejects[0]["line_no"] == 3


def test_generated_tariff_file_is_dirty_but_acceptable():
    from simulators.batch_tariff_dropper import build_tariff_csv

    settings = Settings()
    households = build_households(settings)
    body, stats = build_tariff_csv(households, date(2026, 1, 1), settings.random_seed)
    assert body == build_tariff_csv(households, date(2026, 1, 1), settings.random_seed)[0]  # deterministic
    rows = list(csv.DictReader(io.StringIO(body.decode())))
    assert len(rows) == stats["rows"]
    clean, rejects = validate_tariff_file(rows)
    assert rejects, "the simulated feed should contain defects"
    assert file_is_acceptable(len(clean), len(rejects))
    assert len({r["household_id"] for r in clean}) == len(clean)


def test_mostly_broken_file_is_refused():
    assert not file_is_acceptable(clean_count=5, reject_count=5)
    assert not file_is_acceptable(clean_count=0, reject_count=0)
