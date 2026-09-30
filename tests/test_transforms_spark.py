"""Tests for the Spark transformations shared by the speed and batch layers."""
import json
from datetime import date, datetime

import pytest

from gridlib import billing, schemas

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from processing import transforms as T  # noqa: E402


def reading(event_id, household="HH-0001", zone="ZONE-NORTH", kwh=0.5, solar=0.1, ts="2026-01-01T10:05:00+00:00"):
    return {
        "event_id": event_id,
        "meter_id": household.replace("HH", "MTR"),
        "household_id": household,
        "grid_zone": zone,
        "power_consumption_kwh": kwh,
        "solar_generation_kwh": solar,
        "timestamp": ts,
        "emitted_at": ts,
        "schema_version": 1,
    }


def kafka_frame(spark, payloads):
    """Build a DataFrame shaped like the Kafka source."""
    rows = [(f"k{i}", p, 0, i, datetime(2026, 1, 1)) for i, p in enumerate(payloads)]
    return spark.createDataFrame(
        rows, "key string, value string, partition int, offset long, timestamp timestamp"
    ).select(
        F.col("key").cast("binary").alias("key"),
        F.col("value").cast("binary").alias("value"),
        "partition",
        "offset",
        "timestamp",
    )


def clean(spark, events):
    return T.valid_readings(T.with_reject_reason(T.parse_kafka(kafka_frame(spark, [json.dumps(e) for e in events]))))


def test_spark_validation_agrees_with_the_python_contract(spark):
    good = reading("e1")
    cases = [
        good,
        {**good, "event_id": "e2", "power_consumption_kwh": -3},
        {**good, "event_id": "e3", "grid_zone": "ZONE-UNKNOWN"},
        {**good, "event_id": "e4", "timestamp": "not-a-timestamp"},
        {k: v for k, v in good.items() if k != "solar_generation_kwh"},
    ]
    payloads = [json.dumps(c) for c in cases] + ['{"event_id": "trunc']
    checked = T.with_reject_reason(T.parse_kafka(kafka_frame(spark, payloads)))
    got = [r["reject_reason"] for r in checked.orderBy("kafka_offset").collect()]
    expected = [schemas.validate_reading(c) for c in cases] + [schemas.REJECT_MALFORMED_JSON]
    assert got == expected
    assert T.valid_readings(checked).count() == 1
    assert T.rejected_readings(checked).count() == 5


def test_duplicates_are_removed_by_event_id(spark):
    df = clean(spark, [reading("e1"), reading("e1"), reading("e2")])
    assert T.deduplicate(df).count() == 2


def test_zone_windows_sum_load_and_compute_renewable_share(spark):
    df = clean(
        spark,
        [
            reading("e1", "HH-0001", kwh=1.0, solar=0.5, ts="2026-01-01T10:01:00+00:00"),
            reading("e2", "HH-0002", kwh=3.0, solar=0.5, ts="2026-01-01T10:14:00+00:00"),
            reading("e3", "HH-0001", kwh=2.0, solar=0.0, ts="2026-01-01T10:16:00+00:00"),  # next window
            reading("e4", "HH-0003", zone="ZONE-SOUTH", kwh=5.0, solar=0.0, ts="2026-01-01T10:02:00+00:00"),
        ],
    )
    rows = {(r.grid_zone, r.window_start.strftime("%H:%M")): r for r in T.zone_window_load(df, "15 minutes").collect()}
    first = rows[("ZONE-NORTH", "10:00")]
    assert first.consumption_kwh == pytest.approx(4.0)
    assert first.solar_kwh == pytest.approx(1.0)
    assert first.net_load_kwh == pytest.approx(3.0)
    assert first.renewable_share == pytest.approx(0.25)
    assert first.active_meters == 2
    assert rows[("ZONE-NORTH", "10:15")].consumption_kwh == pytest.approx(2.0)
    assert rows[("ZONE-SOUTH", "10:00")].renewable_share == 0.0


def test_household_totals_are_split_by_simulated_day(spark):
    df = clean(
        spark,
        [
            reading("e1", kwh=1.0, ts="2026-01-01T23:50:00+00:00"),
            reading("e2", kwh=2.0, ts="2026-01-01T23:59:00+00:00"),
            reading("e3", kwh=4.0, ts="2026-01-02T00:01:00+00:00"),
        ],
    )
    totals = {r.sim_date: r for r in T.household_daily_totals(df).collect()}
    assert totals[date(2026, 1, 1)].consumption_kwh == pytest.approx(3.0)
    assert totals[date(2026, 1, 1)].readings == 2
    assert totals[date(2026, 1, 2)].consumption_kwh == pytest.approx(4.0)


def test_tariff_is_carried_forward_or_defaulted_when_missing(spark):
    tariffs = spark.createDataFrame(
        [
            (date(2026, 1, 1), "HH-0001", 30.0, "TIER_1", False),
            (date(2026, 1, 2), "HH-0001", 33.0, "TIER_1", False),
            (date(2026, 1, 1), "HH-0002", 40.0, "TIER_3", True),  # absent from the file of Jan 2
            (date(2026, 1, 3), "HH-0001", 99.0, "TIER_1", False),  # future tariff must be ignored
        ],
        "dt date, household_id string, tariff_rate double, billing_tier string, subsidy_flag boolean",
    )
    usage = spark.createDataFrame(
        [("HH-0001", 10.0, 0.0), ("HH-0002", 10.0, 0.0), ("HH-0003", 10.0, 0.0)],
        "household_id string, consumption_kwh double, solar_kwh double",
    )
    joined = T.attach_tariff(usage, T.tariffs_as_of(tariffs, "2026-01-02"), default_rate=30.0)
    got = {r.household_id: (r.tariff_rate, r.tariff_source) for r in joined.collect()}
    assert got == {
        "HH-0001": (33.0, "same_day"),
        "HH-0002": (40.0, "carried_forward"),
        "HH-0003": (30.0, "default"),
    }


def test_spark_billing_matches_the_reference_implementation(spark):
    cases = [
        (20.0, 0.0, 30.0, "TIER_1", False),
        (12.5, 3.25, 32.4, "TIER_2", False),
        (8.0, 0.0, 25.7, "TIER_1", True),
        (5.0, 9.0, 45.0, "TIER_3", False),  # net exporter
        (17.123, 17.123, 31.0, "TIER_2", True),  # exactly self-sufficient
    ]
    df = spark.createDataFrame(
        cases, "consumption_kwh double, solar_kwh double, tariff_rate double, billing_tier string, subsidy_flag boolean"
    )
    for row in T.with_billing(df).collect():
        expected = billing.compute_bill(
            row.consumption_kwh, row.solar_kwh, row.tariff_rate, row.billing_tier, row.subsidy_flag
        )
        for field, value in expected.items():
            assert row[field] == pytest.approx(value, abs=0.011), field
