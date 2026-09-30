import pytest

from gridlib.billing import FIXED_DAILY_CHARGE, compute_bill


def test_plain_consumer_pays_for_every_kwh():
    bill = compute_bill(20.0, 0.0, tariff_rate=30.0, billing_tier="TIER_1", subsidy_flag=False)
    assert bill["import_kwh"] == 20.0
    assert bill["export_kwh"] == 0.0
    assert bill["total_bill"] == pytest.approx(20 * 30 + FIXED_DAILY_CHARGE)


def test_higher_tier_multiplies_the_energy_charge():
    tier1 = compute_bill(10.0, 0.0, 30.0, "TIER_1", False)
    tier3 = compute_bill(10.0, 0.0, 30.0, "TIER_3", False)
    assert tier3["energy_charge"] == pytest.approx(tier1["energy_charge"] * 1.6)


def test_subsidy_discounts_only_the_energy_charge():
    bill = compute_bill(10.0, 0.0, 30.0, "TIER_1", subsidy_flag=True)
    assert bill["subsidy_discount"] == pytest.approx(60.0)  # 20% of 300
    assert bill["total_bill"] == pytest.approx(300 - 60 + FIXED_DAILY_CHARGE)


def test_solar_offsets_consumption():
    bill = compute_bill(10.0, 4.0, 30.0, "TIER_1", False)
    assert bill["net_kwh"] == 6.0
    assert bill["import_kwh"] == 6.0
    assert bill["export_credit"] == 0.0


def test_net_exporter_earns_a_credit_at_half_rate():
    bill = compute_bill(5.0, 9.0, 30.0, "TIER_2", False)
    assert bill["import_kwh"] == 0.0
    assert bill["export_kwh"] == 4.0
    assert bill["energy_charge"] == 0.0
    assert bill["export_credit"] == pytest.approx(4 * 30 * 0.5)
    assert bill["total_bill"] == pytest.approx(FIXED_DAILY_CHARGE - 60.0)


def test_unknown_tier_is_an_error():
    with pytest.raises(KeyError):
        compute_bill(1.0, 0.0, 30.0, "TIER_9", False)
