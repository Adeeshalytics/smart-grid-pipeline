"""Generate the Grafana dashboards as JSON."""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "observability" / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "griddb"}

_next_id = 0


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def prom(expr: str, legend: str = "", instant: bool = False) -> dict:
    target = {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": "A"}
    if instant:
        target.update({"instant": True, "range": False, "format": "table"})
    return target


def sql(query: str, fmt: str = "table") -> dict:
    return {"datasource": PG, "rawSql": query, "format": fmt, "rawQuery": True, "editorMode": "code", "refId": "A"}


def _refs(targets: list[dict]) -> list[dict]:
    return [{**t, "refId": chr(ord("A") + i)} for i, t in enumerate(targets)]


def panel(kind: str, title: str, targets: list[dict], pos: tuple, **extra) -> dict:
    x, y, w, h = pos
    base = {
        "id": _id(),
        "type": kind,
        "title": title,
        "datasource": targets[0]["datasource"],
        "targets": _refs(targets),
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
    }
    base.update(extra)
    return base


def stat(title, targets, pos, unit="short", decimals=1, thresholds=None, text=False, description=""):
    steps = thresholds or [{"color": "green", "value": None}]
    return panel(
        "stat", title, targets, pos,
        description=description,
        fieldConfig={"defaults": {"unit": unit, "decimals": decimals, "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
        options={
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/" if text else "", "values": False},
            "colorMode": "value", "graphMode": "none", "textMode": "value",
        },
    )


def timeseries(title, targets, pos, unit="short", description="", threshold=None, stack=False):
    defaults = {
        "unit": unit,
        "custom": {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never",
                   "stacking": {"mode": "normal" if stack else "none"}},
    }
    if threshold is not None:
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "transparent", "value": threshold}]}
        defaults["custom"]["thresholdsStyle"] = {"mode": "line+area"}
    return panel(
        "timeseries", title, targets, pos,
        description=description,
        fieldConfig={"defaults": defaults, "overrides": []},
        options={"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    )


def table(title, targets, pos, description=""):
    return panel(
        "table", title, targets, pos,
        description=description,
        fieldConfig={"defaults": {"decimals": 2, "custom": {"align": "auto"}}, "overrides": []},
        options={"showHeader": True, "cellHeight": "sm"},
    )


def dashboard(uid: str, title: str, panels: list[dict], time_from: str, tags: list[str]) -> dict:
    return {
        "uid": uid,
        "title": title,
        "tags": tags,
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "5s",
        "time": {"from": time_from, "to": "now"},
        "panels": panels,
        "annotations": {"list": []},
        "templating": {"list": []},
    }


RED_BELOW_20 = [{"color": "red", "value": None}, {"color": "orange", "value": 20}, {"color": "green", "value": 50}]
RED_ABOVE_60 = [{"color": "green", "value": None}, {"color": "orange", "value": 30}, {"color": "red", "value": 60}]


def grid_operations() -> dict:
    zone_series = lambda column: sql(  # noqa: E731
        f"SELECT wall_time AS time, grid_zone AS metric, {column} AS value FROM v_rt_zone_load "
        "WHERE $__timeFilter(wall_time) AND window_end <= (SELECT max(window_start) FROM rt_zone_load) ORDER BY 1",
        "time_series",
    )
    panels = [
        stat("Simulated time now", [sql("SELECT to_char(max(window_end), 'YYYY-MM-DD HH24:MI') AS sim_time FROM rt_zone_load")],
             (0, 0, 5, 4), text=True, description="1 simulated day = 5 minutes of wall-clock time"),
        stat("Grid load (kWh per 15 min)", [sql("SELECT sum(consumption_kwh) FROM v_grid_now")], (5, 0, 4, 4), unit="kwatth"),
        stat("Solar generation (kWh per 15 min)", [sql("SELECT sum(solar_kwh) FROM v_grid_now")], (9, 0, 4, 4), unit="kwatth"),
        stat("Renewable contribution", [sql("SELECT 100 * sum(solar_kwh) / NULLIF(sum(consumption_kwh), 0) FROM v_grid_now")],
             (13, 0, 4, 4), unit="percent", thresholds=RED_BELOW_20),
        stat("Meters reporting", [sql("SELECT sum(active_meters) FROM v_grid_now")], (17, 0, 3, 4), decimals=0),
        stat("Low-renewable alerts (last hour)", [sql("SELECT count(*) FROM grid_alerts WHERE created_at > now() - INTERVAL '1 hour'")],
             (20, 0, 4, 4), decimals=0, thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]),
        timeseries("Grid load by zone (kWh per 15-minute window)", [zone_series("consumption_kwh")], (0, 4, 12, 8), unit="kwatth",
                   description="Speed layer: tumbling 15-minute windows, plotted at the wall-clock time they occurred"),
        timeseries("Solar generation by zone (kWh per 15-minute window)", [zone_series("solar_kwh")], (12, 4, 12, 8), unit="kwatth"),
        timeseries("Renewable contribution by zone (%)", [zone_series("100 * renewable_share")], (0, 12, 12, 8), unit="percent",
                   threshold=20, description="Solar generation / consumption. Below the red line during daylight raises an alert"),
        timeseries("Net load the grid must supply (kWh per window)", [zone_series("net_load_kwh")], (12, 12, 12, 8), unit="kwatth",
                   description="Negative values mean the zone is exporting surplus solar"),
        table("Low renewable contribution alerts", [sql(
            "SELECT to_char(window_start, 'YYYY-MM-DD HH24:MI') AS \"hour (simulated)\", grid_zone AS zone, severity, "
            "round((100 * value)::numeric, 1) AS \"renewable %\", message FROM grid_alerts ORDER BY created_at DESC LIMIT 15")],
              (0, 20, 24, 6)),
        table("Latest daily billing report by zone (batch layer)", [sql(
            "SELECT sim_date::text AS \"simulated day\", grid_zone AS zone, households::text AS households, round(consumption_kwh::numeric, 1) AS \"consumption kWh\", "
            "round(solar_kwh::numeric, 1) AS \"solar kWh\", round((100 * renewable_share)::numeric, 1) AS \"renewable %\", "
            "round(total_billed::numeric, 2) AS \"billed LKR\" FROM daily_zone_summary "
            "WHERE sim_date = (SELECT max(sim_date) FROM daily_zone_summary) ORDER BY grid_zone")],
              (0, 26, 24, 7)),
        table("Speed layer vs batch layer (latest day)", [sql(
            "SELECT sim_date::text AS \"simulated day\", grid_zone AS zone, round(batch_kwh::numeric, 2) AS \"batch kWh\", "
            "round(speed_kwh::numeric, 2) AS \"speed kWh\", round(diff_pct::numeric, 2) AS \"diff %\", "
            "(batch_readings - speed_readings)::text AS \"readings missed by speed layer\" FROM reconciliation "
            "WHERE sim_date = (SELECT max(sim_date) FROM reconciliation) ORDER BY grid_zone")],
              (0, 33, 24, 7), description="The batch layer is exact; the difference is what arrived after the watermark"),
        table("Household bills, latest day (top 8)", [sql(
            "SELECT household_id AS household, grid_zone AS zone, billing_tier AS tier, round(consumption_kwh::numeric, 2) AS \"consumption kWh\", "
            "round(solar_kwh::numeric, 2) AS \"solar kWh\", solar_contribution_pct AS \"solar %\", tariff_rate AS rate, tariff_source AS \"tariff source\", "
            "total_bill AS \"bill LKR\" FROM daily_household_bill WHERE sim_date = (SELECT max(sim_date) FROM daily_household_bill) "
            "ORDER BY total_bill DESC LIMIT 8")],
              (0, 40, 24, 9)),
    ]
    return dashboard("grid-ops", "Grid Operations", panels, "now-15m", ["smart-grid", "business"])


def pipeline_health() -> dict:
    panels = [
        stat("Pipeline targets up", [prom('sum(up{job!="prometheus"})')], (0, 0, 4, 4), decimals=0,
             thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 4}]),
        stat("Events produced / s", [prom("sum(rate(sim_events_produced_total[1m]))")], (4, 0, 4, 4)),
        stat("Max Kafka lag (offsets)", [prom("max(speed_kafka_offsets_behind_latest)")], (8, 0, 4, 4), decimals=0,
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1000}, {"color": "red", "value": 5000}]),
        stat("Real-time view age", [prom("grid_realtime_view_age_seconds")], (12, 0, 4, 4), unit="s", thresholds=RED_ABOVE_60),
        stat("Daily report age", [prom("grid_daily_report_age_seconds")], (16, 0, 4, 4), unit="s",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 450}, {"color": "red", "value": 900}]),
        stat("Alerts firing", [prom('count(ALERTS{alertstate="firing"}) or vector(0)')], (20, 0, 4, 4), decimals=0,
             thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        timeseries("Ingestion: events produced by kind (per second)", [prom("sum by (kind) (rate(sim_events_produced_total[1m]))", "{{kind}}")],
                   (0, 4, 12, 7), unit="ops", stack=True, description="on_time, late, very_late, duplicate and corrupt events injected by the simulator"),
        timeseries("Processing: rows read from Kafka by streaming query (per second)",
                   [prom("rate(speed_input_rows_total[1m])", "{{query}}")], (12, 4, 12, 7), unit="ops"),
        timeseries("Processing: micro-batch duration (ms)", [prom("speed_batch_duration_ms", "{{query}}")], (0, 11, 8, 7), unit="ms"),
        timeseries("Processing: Kafka consumer lag (offsets)", [prom("speed_kafka_offsets_behind_latest", "{{query}}")], (8, 11, 8, 7)),
        timeseries("Processing: rows held in state stores", [prom("speed_state_rows", "{{query}}")], (16, 11, 8, 7)),
        timeseries("Data quality: events rejected to the dead-letter topic (per minute)",
                   [prom("sum by (reason) (rate(speed_rejected_events_total[2m])) * 60", "{{reason}}")], (0, 18, 8, 7), stack=True),
        timeseries("Data quality: readings dropped as too late (per minute)",
                   [prom("rate(speed_rows_dropped_by_watermark_total[2m]) * 60", "{{query}}")], (8, 18, 8, 7),
                   description="Arrived after the 30-minute watermark. The batch layer still counts them."),
        timeseries("Storage: rows written to the serving store (per second)", [prom("rate(speed_sink_rows_total[1m])", "{{sink}}")],
                   (16, 18, 8, 7), unit="ops"),
        timeseries("Serving: API latency p95 (s)",
                   [prom("histogram_quantile(0.95, sum by (le, path) (rate(api_request_seconds_bucket[2m])))", "{{path}}")],
                   (0, 25, 12, 7), unit="s"),
        timeseries("Data freshness (seconds)", [
            prom("grid_realtime_view_age_seconds", "real-time view age"),
            prom("grid_daily_report_age_seconds", "daily report age"),
        ], (12, 25, 12, 7), unit="s"),
        table("Alerts currently firing", [prom('ALERTS{alertstate="firing"}', instant=True)], (0, 32, 12, 8)),
        table("Batch layer audit trail (pipeline_runs)", [sql(
            "SELECT to_char(finished_at, 'HH24:MI:SS') AS finished, sim_date::text AS \"simulated day\", task, status, duration_ms::text AS \"ms\" "
            "FROM pipeline_runs ORDER BY id DESC LIMIT 20")], (12, 32, 12, 8)),
    ]
    return dashboard("grid-pipeline", "Pipeline Health", panels, "now-15m", ["smart-grid", "observability"])


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for name, build in (("grid_operations", grid_operations), ("pipeline_health", pipeline_health)):
        (OUT / f"{name}.json").write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
        print("wrote", OUT / f"{name}.json")
