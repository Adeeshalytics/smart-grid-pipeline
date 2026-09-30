"""The simulated population of households and the physical model behind readings.

Both simulators build the same fleet from the same seed, so the tariff file
always refers to households that the meter stream actually contains.
"""
from __future__ import annotations

import math
import random
import zlib
from dataclasses import dataclass
from datetime import date, datetime

from .config import GRID_ZONES, SOLAR_PENETRATION, Settings


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    grid_zone: str
    load_scale: float  # relative size of the household's demand
    solar_kw_peak: float  # 0 if the household has no rooftop solar
    billing_tier: str
    subsidy_flag: bool


def build_households(settings: Settings) -> list[Household]:
    rng = random.Random(settings.random_seed)
    households = []
    for i in range(1, settings.num_households + 1):
        zone = GRID_ZONES[(i - 1) % len(GRID_ZONES)]
        load_scale = round(rng.uniform(0.6, 1.6), 3)
        has_solar = rng.random() < SOLAR_PENETRATION[zone]
        tier = "TIER_1" if load_scale < 0.9 else "TIER_2" if load_scale < 1.3 else "TIER_3"
        households.append(
            Household(
                household_id=f"HH-{i:04d}",
                meter_id=f"MTR-{i:04d}",
                grid_zone=zone,
                load_scale=load_scale,
                solar_kw_peak=round(rng.uniform(1.5, 3.5), 2) if has_solar else 0.0,
                billing_tier=tier,
                subsidy_flag=tier == "TIER_1" and rng.random() < 0.4,
            )
        )
    return households


def _gauss(x: float, mu: float, sigma: float) -> float:
    return math.exp(-0.5 * ((x - mu) / sigma) ** 2)


def demand_kwh(hour: float, load_scale: float, interval_hours: float) -> float:
    """Expected consumption for one interval: night base, morning and evening peaks."""
    kw = 0.35 + 0.55 * _gauss(hour, 7.5, 1.5) + 1.10 * _gauss(hour, 20.0, 2.0)
    if 9 <= hour <= 17:
        kw += 0.20
    return kw * load_scale * interval_hours


def cloud_factor(zone: str, day: date, seed: int, p_overcast: float) -> float:
    """Share of clear-sky solar output a zone gets on a given day (0..1).

    Deterministic per (zone, day), so a rerun or a second simulator instance
    reproduces the same weather.
    """
    rng = random.Random(zlib.crc32(f"{seed}|{zone}|{day.isoformat()}".encode()))
    roll = rng.random()
    if roll < p_overcast:
        return rng.uniform(0.05, 0.15)  # overcast: triggers low-renewable alerts
    if roll < p_overcast + 0.20:
        return rng.uniform(0.35, 0.60)  # partly cloudy
    return rng.uniform(0.75, 1.00)  # clear


def solar_kwh(hour: float, kw_peak: float, cloud: float, interval_hours: float) -> float:
    """Solar generation for one interval: a bell between 06:00 and 18:00."""
    if kw_peak <= 0 or not 6 <= hour <= 18:
        return 0.0
    return kw_peak * math.sin(math.pi * (hour - 6) / 12) ** 1.5 * cloud * interval_hours


def make_reading_values(
    hh: Household, sim_ts: datetime, settings: Settings, rng: random.Random
) -> tuple[float, float]:
    """Return (consumption_kwh, solar_kwh) for one reading, with measurement noise."""
    hour = sim_ts.hour + sim_ts.minute / 60
    interval_hours = settings.reading_interval_sim_minutes / 60
    cloud = cloud_factor(hh.grid_zone, sim_ts.date(), settings.random_seed, settings.p_overcast)
    consumption = demand_kwh(hour, hh.load_scale, interval_hours) * rng.uniform(0.85, 1.15)
    solar = solar_kwh(hour, hh.solar_kw_peak, cloud, interval_hours) * rng.uniform(0.92, 1.08)
    return round(consumption, 4), round(solar, 4)
