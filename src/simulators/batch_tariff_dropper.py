"""Batch source: drops one tariff/billing reference file per simulated day."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import signal
import time
import zlib
from datetime import date, datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server

from gridlib import db, s3
from gridlib.config import Settings, get_settings
from gridlib.fleet import Household, build_households
from gridlib.logging_utils import get_logger
from gridlib.simclock import load_clock
from gridlib.tariffs import TARIFF_COLUMNS

log = get_logger("tariff-dropper", "ingestion")

FILES = Counter("batch_files_dropped_total", "Tariff files uploaded to the landing zone")
ROWS = Counter("batch_rows_dropped_total", "Tariff rows uploaded", ["quality"])
LAST_DROP = Gauge("batch_last_drop_unix_seconds", "Wall-clock time of the last upload")

BASE_RATE = {"TIER_1": 25.0, "TIER_2": 32.0, "TIER_3": 45.0}
CATCH_UP_DAYS = 3

_running = True


def _stop(*_):
    global _running
    _running = False


def build_tariff_csv(households: list[Household], day: date, seed: int) -> tuple[bytes, dict]:
    """Generate the file for one day. Deterministic per (seed, day)."""
    rng = random.Random(zlib.crc32(f"{seed}|tariff|{day.isoformat()}".encode()))
    daily_factor = rng.uniform(0.95, 1.10)
    rows: list[dict] = []
    stats = {"good": 0, "bad": 0, "missing": 0, "duplicate": 0}

    for hh in households:
        roll = rng.random()
        if roll < 0.02:
            stats["missing"] += 1
            continue
        row = {
            "household_id": hh.household_id,
            "tariff_rate": round(BASE_RATE[hh.billing_tier] * daily_factor, 2),
            "billing_tier": hh.billing_tier,
            "subsidy_flag": str(hh.subsidy_flag).lower(),
        }
        if roll < 0.04:  # malformed row
            defect = rng.choice(["blank_rate", "bad_tier", "negative_rate"])
            if defect == "blank_rate":
                row["tariff_rate"] = ""
            elif defect == "bad_tier":
                row["billing_tier"] = "TIER_9"
            else:
                row["tariff_rate"] = -5
            stats["bad"] += 1
        else:
            stats["good"] += 1
        rows.append(row)

    rows.append(dict(rng.choice(rows)))  # one duplicated household
    stats["duplicate"] += 1
    rng.shuffle(rows)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=TARIFF_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode(), {**stats, "rows": len(rows)}


def drop_file(s3c, settings: Settings, households: list[Household], day: date) -> None:
    body, stats = build_tariff_csv(households, day, settings.random_seed)
    key = s3.tariff_key(day.isoformat())
    manifest = {
        "dt": day.isoformat(),
        "rows": stats["rows"],
        "sha256": hashlib.sha256(body).hexdigest(),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    s3c.put_object(Bucket=settings.bucket_landing, Key=key, Body=body, ContentType="text/csv")
    s3c.put_object(
        Bucket=settings.bucket_landing,
        Key=key.replace("tariffs.csv", "_manifest.json"),
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    FILES.inc()
    ROWS.labels("good").inc(stats["good"])
    ROWS.labels("bad").inc(stats["bad"] + stats["duplicate"])
    LAST_DROP.set(time.time())
    log.info("tariff_file_dropped", bucket=settings.bucket_landing, key=key, **stats)


def main() -> None:
    settings = get_settings()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_http_server(9102)

    conn = db.connect(settings)
    clock = load_clock(settings, conn)
    conn.close()
    s3c = s3.client(settings)
    households = build_households(settings)
    log.info("dropper_started", sim_day_seconds=clock.day_seconds, households=len(households))

    while _running:
        now = datetime.now(timezone.utc)
        current = clock.day_index(now)
        for index in range(max(0, current - CATCH_UP_DAYS), current):
            due = clock.day_end_wall(index).timestamp() + settings.tariff_drop_delay_seconds
            day = clock.sim_date(index)
            manifest_key = s3.tariff_key(day.isoformat()).replace("tariffs.csv", "_manifest.json")
            if time.time() >= due and not s3.object_exists(s3c, settings.bucket_landing, manifest_key):
                try:
                    drop_file(s3c, settings, households, day)
                except Exception:  # noqa: BLE001
                    log.exception("tariff_drop_failed", dt=day.isoformat())
        time.sleep(2)

    log.info("dropper_stopping")


if __name__ == "__main__":
    main()
