"""Task logic for the daily batch pipeline."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from datetime import datetime, timedelta, timezone

from gridlib import db, s3
from gridlib.config import Settings, get_settings
from gridlib.lineage import record_run
from gridlib.logging_utils import get_logger
from gridlib.simclock import load_clock
from gridlib.tariffs import file_is_acceptable, validate_tariff_file

log = get_logger("batch-pipeline", "processing")

LATE_DATA_GRACE_SECONDS = 45
MAX_RECONCILIATION_DIFF_PCT = 25.0
WARN_RECONCILIATION_DIFF_PCT = 5.0


class DataQualityError(Exception):
    """Raised when a check fails and the pipeline must not publish results."""


class NoCompletedDay(Exception):
    """Raised when a run fires before the first simulated day has ended."""


def resolve_sim_date(data_interval_end: datetime, override: str | None = None) -> str:
    """Which simulated day does this run bill?"""
    if override:
        return override
    settings = get_settings()
    conn = db.connect(settings)
    clock = load_clock(settings, conn)
    conn.close()
    elapsed = (data_interval_end - clock.anchor_wall).total_seconds()
    index = int((elapsed + 1) // clock.day_seconds) - 1
    if index < 0:
        raise NoCompletedDay("No simulated day has completed yet")
    return clock.sim_date(index).isoformat()


def tariff_file_ready(sim_date: str) -> bool:
    """Sensor condition: the manifest is written last, so its presence means 'upload complete'."""
    settings = get_settings()
    key = s3.tariff_key(sim_date).replace("tariffs.csv", "_manifest.json")
    return s3.object_exists(s3.client(settings), settings.bucket_landing, key)


def late_data_grace_elapsed(sim_date: str) -> bool:
    """Sensor condition: enough wall-clock time has passed since the day ended."""
    settings = get_settings()
    conn = db.connect(settings)
    clock = load_clock(settings, conn)
    conn.close()
    index = (datetime.fromisoformat(sim_date).date() - clock.sim_epoch.date()).days
    ready_at = clock.day_end_wall(index) + timedelta(seconds=LATE_DATA_GRACE_SECONDS)
    return datetime.now(timezone.utc) >= ready_at


def load_tariffs(sim_date: str, run_id: str) -> dict:
    """Validate the day's tariff file and load it into the serving store."""
    started = time.time()
    settings = get_settings()
    s3c = s3.client(settings)
    key = s3.tariff_key(sim_date)
    body = s3c.get_object(Bucket=settings.bucket_landing, Key=key)["Body"].read()
    manifest = json.loads(
        s3c.get_object(
            Bucket=settings.bucket_landing, Key=key.replace("tariffs.csv", "_manifest.json")
        )["Body"].read()
    )

    if hashlib.sha256(body).hexdigest() != manifest["sha256"]:
        raise DataQualityError(f"Checksum mismatch for {key}: file is corrupt or incomplete")
    rows = list(csv.DictReader(io.StringIO(body.decode())))
    if len(rows) != manifest["rows"]:
        raise DataQualityError(f"{key}: manifest says {manifest['rows']} rows, file has {len(rows)}")

    clean, rejects = validate_tariff_file(rows)
    if not file_is_acceptable(len(clean), len(rejects)):
        raise DataQualityError(f"{key}: {len(rejects)} of {len(rows)} rows rejected - file refused")

    conn = db.connect(settings)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM tariffs WHERE dt = %s", (sim_date,))
        cur.execute("DELETE FROM tariff_rejects WHERE dt = %s", (sim_date,))
    db.execute_values(
        conn,
        "INSERT INTO tariffs (dt, household_id, tariff_rate, billing_tier, subsidy_flag, pipeline_run_id) VALUES %s",
        [
            (sim_date, r["household_id"], r["tariff_rate"], r["billing_tier"], r["subsidy_flag"], run_id)
            for r in clean
        ],
    )
    db.execute_values(
        conn,
        "INSERT INTO tariff_rejects (dt, line_no, reason, raw, pipeline_run_id) VALUES %s",
        [(sim_date, r["line_no"], r["reason"], json.dumps(r["raw"]), run_id) for r in rejects],
    )
    reasons: dict[str, int] = {}
    for r in rejects:
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
    result = {"key": key, "rows": len(rows), "loaded": len(clean), "rejected": len(rejects), "reject_reasons": reasons}
    record_run(conn, run_id, "load_tariffs", sim_date, "success", int((time.time() - started) * 1000), result)
    conn.close()
    log.info("tariffs_loaded", sim_date=sim_date, run_id=run_id, **result)
    return result


RECONCILE_SQL = """
DELETE FROM reconciliation WHERE sim_date = %(d)s;
INSERT INTO reconciliation (sim_date, grid_zone, batch_kwh, speed_kwh, diff_kwh, diff_pct,
                            batch_readings, speed_readings, pipeline_run_id)
SELECT %(d)s::date, b.grid_zone, b.kwh, COALESCE(s.kwh, 0),
       COALESCE(s.kwh, 0) - b.kwh,
       100.0 * (COALESCE(s.kwh, 0) - b.kwh) / NULLIF(b.kwh, 0),
       b.readings, COALESCE(s.readings, 0), %(run_id)s
FROM (SELECT grid_zone, sum(consumption_kwh) AS kwh, sum(readings) AS readings
      FROM daily_household_bill WHERE sim_date = %(d)s GROUP BY grid_zone) b
LEFT JOIN (SELECT grid_zone, sum(consumption_kwh) AS kwh, sum(readings) AS readings
           FROM rt_household_today WHERE sim_date = %(d)s GROUP BY grid_zone) s USING (grid_zone);
"""


def reconcile(sim_date: str, run_id: str) -> dict:
    """Compare the speed layer's view of the day with the batch layer's exact result."""
    started = time.time()
    settings = get_settings()
    conn = db.connect(settings)
    with conn.cursor() as cur:
        cur.execute(RECONCILE_SQL, {"d": sim_date, "run_id": run_id})
    conn.commit()
    rows = db.fetch_all(conn, "SELECT * FROM reconciliation WHERE sim_date = %s ORDER BY grid_zone", (sim_date,))
    batch_kwh = sum(r["batch_kwh"] for r in rows)
    speed_kwh = sum(r["speed_kwh"] for r in rows)
    result = {
        "batch_kwh": round(batch_kwh, 3),
        "speed_kwh": round(speed_kwh, 3),
        "diff_pct": round(100 * (speed_kwh - batch_kwh) / batch_kwh, 3) if batch_kwh else None,
        "max_zone_abs_diff_pct": round(max((abs(r["diff_pct"] or 0) for r in rows), default=0), 3),
        "missing_readings": sum(r["batch_readings"] - r["speed_readings"] for r in rows),
    }
    record_run(conn, run_id, "reconcile", sim_date, "success", int((time.time() - started) * 1000), result)
    conn.close()
    log.info("reconciled", sim_date=sim_date, run_id=run_id, **result)
    return result


def quality_gate(sim_date: str, run_id: str) -> dict:
    """Refuse to publish a report built on bad data."""
    started = time.time()
    settings = get_settings()
    conn = db.connect(settings)
    checks = db.fetch_all(
        conn,
        """
        SELECT
          (SELECT count(*) FROM daily_household_bill WHERE sim_date = %(d)s)                   AS bills,
          (SELECT count(*) FROM daily_household_bill
             WHERE sim_date = %(d)s AND (total_bill IS NULL OR consumption_kwh < 0))           AS invalid_bills,
          (SELECT count(*) FROM daily_household_bill
             WHERE sim_date = %(d)s AND tariff_source <> 'same_day')                           AS tariff_fallbacks,
          (SELECT count(DISTINCT grid_zone) FROM daily_zone_summary WHERE sim_date = %(d)s)    AS zones,
          (SELECT COALESCE(max(abs(diff_pct)), 0) FROM reconciliation WHERE sim_date = %(d)s)  AS max_diff_pct
        """,
        {"d": sim_date},
    )[0]
    failures = []
    if checks["bills"] == 0:
        failures.append("no bills were produced")
    if checks["invalid_bills"]:
        failures.append(f"{checks['invalid_bills']} bills have invalid values")
    if checks["bills"] and checks["tariff_fallbacks"] / checks["bills"] > 0.25:
        failures.append(f"{checks['tariff_fallbacks']} bills used a fallback tariff")
    if checks["max_diff_pct"] > MAX_RECONCILIATION_DIFF_PCT:
        failures.append(f"speed and batch layers differ by {checks['max_diff_pct']:.1f}%")

    status = "failed" if failures else "success"
    record_run(
        conn, run_id, "quality_gate", sim_date, status, int((time.time() - started) * 1000),
        {**checks, "failures": failures},
    )
    conn.close()
    if checks["max_diff_pct"] > WARN_RECONCILIATION_DIFF_PCT:
        log.warning("reconciliation_drift", sim_date=sim_date, max_diff_pct=checks["max_diff_pct"])
    if failures:
        log.error("quality_gate_failed", sim_date=sim_date, run_id=run_id, failures=failures)
        raise DataQualityError("; ".join(failures))
    log.info("quality_gate_passed", sim_date=sim_date, run_id=run_id, **checks)
    return dict(checks)


def record_failure(run_id: str, task: str, sim_date: str | None, error: str) -> None:
    """Called by Airflow's on_failure_callback so failures appear in the audit trail."""
    settings: Settings = get_settings()
    conn = db.connect(settings, retries=3)
    record_run(conn, run_id, task, sim_date, "failed", None, {"error": error[:2000]})
    conn.close()
    log.error("task_failed", run_id=run_id, task=task, sim_date=sim_date, error=error[:500])
