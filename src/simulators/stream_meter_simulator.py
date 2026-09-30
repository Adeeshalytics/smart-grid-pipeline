"""Streaming source: simulated smart meters publishing readings to Kafka.

Each household's meter reports once per READING_INTERVAL_SIM_MINUTES of
simulated time (15 simulated minutes = ~3 s of wall-clock time by default).
The meters are staggered, so the topic receives a steady flow of events rather
than one burst per interval.

To make the pipeline prove its robustness, the simulator deliberately injects
the faults a real meter network produces:
  * corrupt events   - invalid JSON, missing fields, impossible values
  * duplicates       - the same event_id sent twice (at-least-once delivery)
  * late events      - held back 20-60 simulated minutes before being sent
  * very late events - held back 1-3 simulated hours (past the watermark)
"""
from __future__ import annotations

import heapq
import json
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from gridlib import db
from gridlib.config import Settings, get_settings
from gridlib.fleet import Household, build_households, make_reading_values
from gridlib.logging_utils import get_logger
from gridlib.schemas import SCHEMA_VERSION
from gridlib.simclock import SimClock, load_clock

log = get_logger("meter-simulator", "ingestion")

EVENTS = Counter("sim_events_produced_total", "Events handed to the Kafka producer", ["kind"])
DELIVERY_FAILURES = Counter("sim_delivery_failures_total", "Events Kafka failed to acknowledge")
DELIVERED = Counter("sim_events_delivered_total", "Events acknowledged by the broker")
HELD_BACK = Gauge("sim_late_events_pending", "Late events waiting to be released")
SIM_TIME = Gauge("sim_clock_unix_seconds", "Current simulated time as a Unix timestamp")

_running = True


def _stop(*_):
    global _running
    _running = False


def build_event(hh: Household, sim_ts: datetime, settings: Settings, rng: random.Random) -> dict:
    consumption, solar = make_reading_values(hh, sim_ts, settings, rng)
    return {
        "event_id": str(uuid.uuid4()),
        "meter_id": hh.meter_id,
        "household_id": hh.household_id,
        "grid_zone": hh.grid_zone,
        "power_consumption_kwh": consumption,
        "solar_generation_kwh": solar,
        "timestamp": sim_ts.isoformat(timespec="seconds"),
        "emitted_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "schema_version": SCHEMA_VERSION,
    }


def corrupt(event: dict, rng: random.Random) -> bytes:
    """Return a broken version of the event. Every variant must end up in the DLQ."""
    variant = rng.choice(["invalid_json", "missing_field", "negative", "unknown_zone", "bad_ts"])
    broken = dict(event)
    if variant == "invalid_json":
        return json.dumps(event).encode()[: rng.randint(10, 40)]  # truncated payload
    if variant == "missing_field":
        broken.pop("power_consumption_kwh")
    elif variant == "negative":
        broken["power_consumption_kwh"] = -abs(event["power_consumption_kwh"]) - 1
    elif variant == "unknown_zone":
        broken["grid_zone"] = "ZONE-UNKNOWN"
    else:
        broken["timestamp"] = "not-a-timestamp"
    return json.dumps(broken).encode()


def on_delivery(err, _msg) -> None:
    if err is not None:
        DELIVERY_FAILURES.inc()
        log.error("delivery_failed", error=str(err))
    else:
        DELIVERED.inc()


def main() -> None:
    settings = get_settings()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_http_server(9101)

    conn = db.connect(settings)
    clock: SimClock = load_clock(settings, conn)
    conn.close()

    households = build_households(settings)
    rng = random.Random(settings.random_seed + 1)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "meter-simulator",
            # Idempotent producer + acks=all: no loss and no broker-side duplicates on retry.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )

    interval = clock.wall_seconds(timedelta(minutes=settings.reading_interval_sim_minutes))
    gap = interval / len(households)  # stagger meters evenly across the interval
    held: list[tuple[float, int, str, bytes]] = []  # (release_at, seq, key, payload)
    seq = 0
    log.info(
        "simulator_started",
        households=len(households),
        wall_seconds_per_reading=round(interval, 3),
        sim_speedup=clock.speedup,
        sim_now=clock.now().isoformat(),
    )

    next_send = time.monotonic()
    last_report = time.monotonic()
    sent_since_report = 0
    while _running:
        for hh in households:
            if not _running:
                break
            # Release any held-back (late) events that are now due.
            now = time.monotonic()
            while held and held[0][0] <= now:
                _, _, key, payload = heapq.heappop(held)
                producer.produce(settings.topic_readings, key=key, value=payload, on_delivery=on_delivery)
            HELD_BACK.set(len(held))

            sim_ts = clock.now()
            SIM_TIME.set(sim_ts.timestamp())
            event = build_event(hh, sim_ts, settings, rng)
            payload = json.dumps(event).encode()
            roll = rng.random()

            if roll < settings.p_corrupt:
                payload, kind = corrupt(event, rng), "corrupt"
            elif roll < settings.p_corrupt + settings.p_very_late:
                kind = "very_late"
            elif roll < settings.p_corrupt + settings.p_very_late + settings.p_late:
                kind = "late"
            else:
                kind = "on_time"

            if kind in ("late", "very_late"):
                sim_delay = (
                    timedelta(minutes=rng.uniform(60, 180))
                    if kind == "very_late"
                    else timedelta(minutes=rng.uniform(20, 60))
                )
                seq += 1
                heapq.heappush(held, (now + clock.wall_seconds(sim_delay), seq, hh.meter_id, payload))
            else:
                producer.produce(
                    settings.topic_readings, key=hh.meter_id, value=payload, on_delivery=on_delivery
                )
                if kind == "on_time" and rng.random() < settings.p_duplicate:
                    producer.produce(
                        settings.topic_readings, key=hh.meter_id, value=payload, on_delivery=on_delivery
                    )
                    EVENTS.labels("duplicate").inc()
            EVENTS.labels(kind).inc()
            sent_since_report += 1
            producer.poll(0)

            next_send += gap
            time.sleep(max(0.0, next_send - time.monotonic()))

        if time.monotonic() - last_report >= 30:
            log.info(
                "simulator_heartbeat",
                events_last_30s=sent_since_report,
                late_pending=len(held),
                sim_now=clock.now().isoformat(timespec="seconds"),
            )
            last_report, sent_since_report = time.monotonic(), 0

    log.info("simulator_stopping", flushing=len(producer))
    producer.flush(10)


if __name__ == "__main__":
    main()
