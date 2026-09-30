"""Serving layer: REST API over the real-time and batch views."""
from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from datetime import date

import psycopg2.pool
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.core import REGISTRY, GaugeMetricFamily

from gridlib import db, s3
from gridlib.config import GRID_ZONES, get_settings
from gridlib.logging_utils import get_logger

settings = get_settings()
log = get_logger("api", "serving")
app = FastAPI(title="Smart Grid Serving API", version="1.0.0")

REQUESTS = Counter("api_requests_total", "HTTP requests", ["path", "status"])
LATENCY = Histogram("api_request_seconds", "HTTP request latency", ["path"])

STALE_AFTER_SECONDS = 60

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        db.connect(settings).close()
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 8, settings.pg_dsn, options="-c timezone=UTC")
    return _pool


@contextmanager
def connection():
    conn = pool().getconn()
    try:
        yield conn
    finally:
        conn.rollback()
        pool().putconn(conn)


def query(sql: str, params=None) -> list[dict]:
    with connection() as conn:
        return db.fetch_all(conn, sql, params)


@app.middleware("http")
async def access_log(request: Request, call_next):
    """One structured log line and one metric sample per request, with a trace id."""
    trace_id = request.headers.get("x-request-id", uuid.uuid4().hex[:16])
    started = time.perf_counter()
    route = request.scope.get("route")
    try:
        response = await call_next(request)
    except Exception:
        log.exception("request_failed", trace_id=trace_id, path=request.url.path)
        REQUESTS.labels(request.url.path, "500").inc()
        raise
    route = request.scope.get("route") or route
    path = getattr(route, "path", request.url.path)
    elapsed = time.perf_counter() - started
    REQUESTS.labels(path, str(response.status_code)).inc()
    LATENCY.labels(path).observe(elapsed)
    response.headers["x-request-id"] = trace_id
    if path not in ("/metrics", "/health"):
        log.info(
            "request",
            trace_id=trace_id,
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(elapsed * 1000, 1),
        )
    return response


class PipelineCollector:
    """Exports the state of the serving store as Prometheus gauges at scrape time."""

    def collect(self):
        try:
            zones = query("SELECT * FROM v_grid_now")
            fresh = query(
                """SELECT extract(epoch FROM now() - max(updated_at)) AS age FROM rt_zone_load"""
            )[0]["age"]
            batch = query(
                """SELECT extract(epoch FROM now() - COALESCE(max(generated_at),
                                                             (SELECT anchor_wall FROM sim_clock))) AS age,
                          count(*) AS n
                   FROM daily_reports"""
            )[0]
            recon = query(
                """SELECT COALESCE(max(abs(diff_pct)), 0) AS diff FROM reconciliation
                   WHERE sim_date = (SELECT max(sim_date) FROM reconciliation)"""
            )[0]["diff"]
            failed = query(
                "SELECT count(*) AS n FROM pipeline_runs WHERE status = 'failed' AND finished_at > now() - INTERVAL '15 minutes'"
            )[0]["n"]
            alerts = query(
                "SELECT count(*) AS n FROM grid_alerts WHERE created_at > now() - INTERVAL '5 minutes'"
            )[0]["n"]
        except Exception:  # noqa: BLE001
            log.exception("metrics_collection_failed")
            up = GaugeMetricFamily("grid_serving_store_up", "1 if the serving store answered")
            up.add_metric([], 0)
            yield up
            return

        up = GaugeMetricFamily("grid_serving_store_up", "1 if the serving store answered")
        up.add_metric([], 1)
        yield up

        load = GaugeMetricFamily("grid_zone_consumption_kwh", "Consumption in the latest closed window", labels=["zone"])
        solar = GaugeMetricFamily("grid_zone_solar_kwh", "Solar generation in the latest closed window", labels=["zone"])
        share = GaugeMetricFamily("grid_zone_renewable_share", "Solar / consumption, latest closed window", labels=["zone"])
        meters = GaugeMetricFamily("grid_zone_active_meters", "Meters reporting in the latest closed window", labels=["zone"])
        for z in zones:
            load.add_metric([z["grid_zone"]], z["consumption_kwh"])
            solar.add_metric([z["grid_zone"]], z["solar_kwh"])
            share.add_metric([z["grid_zone"]], z["renewable_share"])
            meters.add_metric([z["grid_zone"]], z["active_meters"])
        yield from (load, solar, share, meters)

        g = GaugeMetricFamily("grid_realtime_view_age_seconds", "Seconds since the speed layer last wrote a window")
        g.add_metric([], float(fresh) if fresh is not None else float("inf"))
        yield g
        g = GaugeMetricFamily("grid_daily_report_age_seconds", "Seconds since the batch layer last published a report")
        g.add_metric([], float(batch["age"]) if batch["age"] is not None else float("inf"))
        yield g
        g = GaugeMetricFamily("grid_daily_reports_published", "Number of daily reports available")
        g.add_metric([], batch["n"])
        yield g
        g = GaugeMetricFamily("grid_reconciliation_max_abs_diff_pct", "Largest speed-vs-batch difference, latest day")
        g.add_metric([], float(recon))
        yield g
        g = GaugeMetricFamily("grid_batch_failed_tasks_recent", "Batch tasks failed in the last 15 minutes")
        g.add_metric([], failed)
        yield g
        g = GaugeMetricFamily("grid_business_alerts_recent", "Business alerts raised in the last 5 minutes")
        g.add_metric([], alerts)
        yield g


REGISTRY.register(PipelineCollector())


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health(response: Response) -> dict:
    """Health check: is the serving store reachable and is real-time data still arriving?"""
    try:
        row = query(
            """SELECT extract(epoch FROM now() - max(updated_at)) AS age, max(window_start) AS latest
               FROM rt_zone_load"""
        )[0]
    except Exception as exc:  # noqa: BLE001
        response.status_code = 503
        return {"status": "down", "reason": f"serving store unreachable: {exc}"}
    if row["age"] is None:
        response.status_code = 503
        return {"status": "degraded", "reason": "no real-time data has arrived yet"}
    stale = row["age"] > STALE_AFTER_SECONDS
    if stale:
        response.status_code = 503
    return {
        "status": "degraded" if stale else "ok",
        "realtime_view_age_seconds": round(float(row["age"]), 1),
        "latest_window_start": row["latest"],
        "stale_after_seconds": STALE_AFTER_SECONDS,
    }


@app.get("/api/v1/grid/realtime")
def grid_realtime() -> dict:
    """Current grid load and renewable mix by zone (latest closed 15-minute window)."""
    zones = query("SELECT * FROM v_grid_now ORDER BY grid_zone")
    if not zones:
        raise HTTPException(503, "No real-time data yet")
    consumption = sum(z["consumption_kwh"] for z in zones)
    solar = sum(z["solar_kwh"] for z in zones)
    return {
        "as_of_window_start": max(z["window_start"] for z in zones),
        "grid": {
            "consumption_kwh": round(consumption, 3),
            "solar_kwh": round(solar, 3),
            "net_load_kwh": round(consumption - solar, 3),
            "renewable_share": round(solar / consumption, 4) if consumption else 0,
            "active_meters": sum(z["active_meters"] for z in zones),
        },
        "zones": [
            {
                "grid_zone": z["grid_zone"],
                "window_start": z["window_start"],
                "window_end": z["window_end"],
                "consumption_kwh": z["consumption_kwh"],
                "solar_kwh": z["solar_kwh"],
                "net_load_kwh": z["net_load_kwh"],
                "renewable_share": z["renewable_share"],
                "active_meters": z["active_meters"],
                "low_renewable": z["renewable_share"] < settings.low_renewable_threshold,
            }
            for z in zones
        ],
    }


@app.get("/api/v1/grid/zones/{zone}/history")
def zone_history(zone: str, windows: int = 24) -> dict:
    if zone not in GRID_ZONES:
        raise HTTPException(404, f"Unknown zone. Valid zones: {', '.join(GRID_ZONES)}")
    rows = query(
        """SELECT window_start, window_end, consumption_kwh, solar_kwh, net_load_kwh,
                  renewable_share, active_meters
           FROM rt_zone_load WHERE grid_zone = %s ORDER BY window_start DESC LIMIT %s""",
        (zone, min(max(windows, 1), 500)),
    )
    return {"grid_zone": zone, "windows": rows}


@app.get("/api/v1/households/{household_id}/today")
def household_today(household_id: str) -> dict:
    rows = query(
        """SELECT * FROM rt_household_today WHERE household_id = %s
           ORDER BY sim_date DESC LIMIT 1""",
        (household_id,),
    )
    if not rows:
        raise HTTPException(404, "Household not found")
    return {**rows[0], "note": "provisional estimate from the speed layer; the daily batch bill is authoritative"}


@app.get("/api/v1/households/{household_id}/bill-to-date")
def bill_to_date(household_id: str) -> dict:
    """Lambda serving-layer merge: exact batch bills for closed days + real-time estimate for the rest."""
    batch = query(
        "SELECT sim_date, consumption_kwh, solar_kwh, total_bill FROM daily_household_bill WHERE household_id = %s ORDER BY sim_date",
        (household_id,),
    )
    last_billed = batch[-1]["sim_date"] if batch else date.min
    speed = query(
        """SELECT sim_date, consumption_kwh, solar_kwh, provisional_bill FROM rt_household_today
           WHERE household_id = %s AND sim_date > %s ORDER BY sim_date""",
        (household_id, last_billed),
    )
    if not batch and not speed:
        raise HTTPException(404, "Household not found")
    return {
        "household_id": household_id,
        "billed_days": len(batch),
        "billed_total": round(sum(b["total_bill"] for b in batch), 2),
        "unbilled_days": [s["sim_date"] for s in speed],
        "unbilled_estimate": round(sum(s["provisional_bill"] for s in speed), 2),
        "total_to_date": round(
            sum(b["total_bill"] for b in batch) + sum(s["provisional_bill"] for s in speed), 2
        ),
    }


@app.get("/api/v1/billing/{sim_date}")
def billing_report(sim_date: date) -> dict:
    """Consolidated daily billing and solar-contribution report."""
    zones = query("SELECT * FROM daily_zone_summary WHERE sim_date = %s ORDER BY grid_zone", (sim_date,))
    if not zones:
        raise HTTPException(404, f"No billing run has completed for {sim_date}")
    bills = query(
        """SELECT household_id, grid_zone, billing_tier, consumption_kwh, solar_kwh, import_kwh,
                  export_kwh, solar_contribution_pct, tariff_rate, subsidy_flag, tariff_source, total_bill
           FROM daily_household_bill WHERE sim_date = %s ORDER BY household_id""",
        (sim_date,),
    )
    return {
        "sim_date": sim_date,
        "pipeline_run_id": zones[0]["pipeline_run_id"],
        "totals": {
            "households": len(bills),
            "consumption_kwh": round(sum(z["consumption_kwh"] for z in zones), 3),
            "solar_kwh": round(sum(z["solar_kwh"] for z in zones), 3),
            "total_billed": round(sum(z["total_billed"] for z in zones), 2),
        },
        "zones": zones,
        "households": bills,
    }


@app.get("/api/v1/billing/{sim_date}/households/{household_id}")
def household_bill(sim_date: date, household_id: str) -> dict:
    rows = query(
        "SELECT * FROM daily_household_bill WHERE sim_date = %s AND household_id = %s",
        (sim_date, household_id),
    )
    if not rows:
        raise HTTPException(404, "No bill for that household and day")
    return rows[0]


@app.get("/api/v1/reconciliation/{sim_date}")
def reconciliation(sim_date: date) -> dict:
    rows = query("SELECT * FROM reconciliation WHERE sim_date = %s ORDER BY grid_zone", (sim_date,))
    if not rows:
        raise HTTPException(404, f"No reconciliation for {sim_date}")
    return {"sim_date": sim_date, "zones": rows}


@app.get("/api/v1/reports")
def list_reports() -> list[dict]:
    return query("SELECT sim_date, html_key, csv_key, summary, generated_at FROM daily_reports ORDER BY sim_date DESC LIMIT 50")


@app.get("/api/v1/reports/latest", response_class=HTMLResponse)
def latest_report() -> HTMLResponse:
    rows = query("SELECT html_key FROM daily_reports ORDER BY sim_date DESC LIMIT 1")
    if not rows:
        raise HTTPException(404, "No report has been published yet")
    body = s3.client(settings).get_object(Bucket=settings.bucket_reports, Key=rows[0]["html_key"])["Body"].read()
    return HTMLResponse(body)


@app.get("/api/v1/alerts")
def alerts(limit: int = 50) -> dict:
    limit = min(max(limit, 1), 500)
    return {
        "business": query("SELECT * FROM grid_alerts ORDER BY created_at DESC LIMIT %s", (limit,)),
        "operational": query("SELECT * FROM ops_alerts ORDER BY received_at DESC LIMIT %s", (limit,)),
    }


@app.post("/internal/alertmanager", include_in_schema=False)
async def alertmanager_webhook(request: Request) -> dict:
    """Receive operational alerts from Alertmanager and keep them with the data they concern."""
    payload = await request.json()
    rows = [
        (
            a["labels"].get("alertname", "unknown"),
            a.get("status", "firing"),
            a["labels"].get("severity"),
            a.get("annotations", {}).get("summary"),
            json.dumps(a["labels"]),
            a.get("startsAt"),
        )
        for a in payload.get("alerts", [])
    ]
    with connection() as conn:
        db.execute_values(
            conn,
            "INSERT INTO ops_alerts (alertname, status, severity, summary, labels, starts_at) VALUES %s",
            rows,
        )
    for alertname, status, severity, summary, _labels, _starts in rows:
        log.warning("ops_alert_received", alertname=alertname, status=status, severity=severity, summary=summary)
    return {"stored": len(rows)}
