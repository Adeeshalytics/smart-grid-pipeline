"""Daily consolidated billing and solar-contribution report.

This is the batch layer's final deliverable: one HTML report (for people) and
one CSV (for downstream systems) per simulated day, stored in the object store
and copied to the local ./reports folder.
"""
from __future__ import annotations

import csv
import io
import json
import os
import time
from datetime import datetime, timezone
from html import escape

from gridlib import db, s3
from gridlib.billing import CURRENCY
from gridlib.config import get_settings
from gridlib.lineage import record_run
from gridlib.logging_utils import get_logger

log = get_logger("batch-report", "serving")

LOCAL_REPORT_DIR = os.environ.get("REPORT_DIR", "/opt/airflow/reports")

CSS = """
body{font-family:Segoe UI,Arial,sans-serif;margin:32px;color:#1c2733;background:#f6f8fa}
h1{margin:0 0 4px}h2{margin:28px 0 8px;font-size:18px}
.meta{color:#5b6b7b;font-size:13px;margin-bottom:20px}
.kpis{display:flex;flex-wrap:wrap;gap:12px}
.kpi{background:#fff;border:1px solid #d9e0e7;border-radius:8px;padding:12px 16px;min-width:150px}
.kpi b{display:block;font-size:22px}.kpi span{color:#5b6b7b;font-size:12px}
table{border-collapse:collapse;background:#fff;width:100%;font-size:13px}
th,td{border:1px solid #d9e0e7;padding:6px 10px;text-align:left}
th{background:#eaf0f5}td.num{text-align:right;font-variant-numeric:tabular-nums}
.warn{color:#b35900}.ok{color:#1a7f37}.note{color:#5b6b7b;font-size:12px;margin-top:6px}
"""


def _cell(value) -> str:
    text = str(value)
    numeric = text.replace(",", "").replace(".", "").replace("-", "").isdigit()
    return f'<td class="num">{escape(text)}</td>' if numeric else f"<td>{escape(text)}</td>"


def _table(headers: list[str], rows: list[list]) -> str:
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(_cell(c) for c in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _n(value, digits: int = 2) -> str:
    return f"{value:,.{digits}f}" if value is not None else "-"


def build_html(sim_date: str, run_id: str, data: dict) -> str:
    t = data["totals"]
    kpis = [
        ("Households billed", f"{t['households']:,}"),
        ("Consumption (kWh)", _n(t["consumption_kwh"], 1)),
        ("Solar generation (kWh)", _n(t["solar_kwh"], 1)),
        ("Renewable contribution", f"{_n(100 * t['solar_kwh'] / t['consumption_kwh'], 1)}%"),
        ("Grid import (kWh)", _n(t["import_kwh"], 1)),
        ("Exported to grid (kWh)", _n(t["export_kwh"], 1)),
        (f"Total billed ({CURRENCY})", _n(t["total_billed"])),
    ]
    kpi_html = "".join(f'<div class="kpi"><b>{v}</b><span>{escape(k)}</span></div>' for k, v in kpis)

    zones = _table(
        ["Zone", "Households", "Consumption kWh", "Solar kWh", "Renewable %", "Import kWh", "Export kWh", f"Billed {CURRENCY}"],
        [
            [z["grid_zone"], z["households"], _n(z["consumption_kwh"], 1), _n(z["solar_kwh"], 1),
             _n(100 * z["renewable_share"], 1), _n(z["import_kwh"], 1), _n(z["export_kwh"], 1), _n(z["total_billed"])]
            for z in data["zones"]
        ],
    )
    bill_headers = ["Household", "Zone", "Tier", "Consumption kWh", "Solar kWh", "Solar %", "Rate", "Subsidy", "Tariff source", f"Bill {CURRENCY}"]
    bill_row = lambda b: [  # noqa: E731
        b["household_id"], b["grid_zone"], b["billing_tier"], _n(b["consumption_kwh"], 2), _n(b["solar_kwh"], 2),
        _n(b["solar_contribution_pct"], 1), _n(b["tariff_rate"]), "yes" if b["subsidy_flag"] else "no",
        b["tariff_source"], _n(b["total_bill"]),
    ]
    recon = _table(
        ["Zone", "Batch kWh (exact)", "Speed kWh (real-time)", "Difference kWh", "Difference %", "Readings missed by speed layer"],
        [
            [r["grid_zone"], _n(r["batch_kwh"], 2), _n(r["speed_kwh"], 2), _n(r["diff_kwh"], 2),
             _n(r["diff_pct"], 2), r["batch_readings"] - r["speed_readings"]]
            for r in data["reconciliation"]
        ],
    )
    alerts = (
        _table(
            ["Hour (simulated)", "Zone", "Severity", "Renewable %", "Message"],
            [[a["window_start"].strftime("%H:%M"), a["grid_zone"], a["severity"], _n(100 * a["value"], 1), a["message"]]
             for a in data["alerts"]],
        )
        if data["alerts"]
        else '<p class="ok">No low-renewable alerts were raised for this day.</p>'
    )
    tq = data["tariff_quality"]
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>Grid billing report {escape(sim_date)}</title><style>{CSS}</style></head><body>
<h1>Daily Billing &amp; Solar Contribution Report</h1>
<div class="meta">Simulated day <b>{escape(sim_date)}</b> &middot; pipeline run <code>{escape(run_id)}</code>
 &middot; generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC</div>
<div class="kpis">{kpi_html}</div>
<h2>Grid load and renewable contribution by zone</h2>{zones}
<h2>Ten highest bills</h2>{_table(bill_headers, [bill_row(b) for b in data["top_bills"]])}
<h2>Ten largest solar contributors</h2>{_table(bill_headers, [bill_row(b) for b in data["top_solar"]])}
<h2>Low renewable contribution alerts</h2>{alerts}
<h2>Tariff file quality</h2>
<p>{tq["loaded"]} tariff rows loaded, <span class="warn">{tq["rejected"]} rejected</span>
 ({escape(json.dumps(tq["reasons"]))}). {tq["fallbacks"]} households were billed with a
 carried-forward or default tariff.</p>
<h2>Reconciliation: speed layer vs batch layer</h2>{recon}
<p class="note">The batch layer recomputes the day from the raw data and is the source of truth.
 The difference is what the real-time view missed: readings that arrived after the watermark.</p>
</body></html>"""


def build_csv(bills: list[dict]) -> bytes:
    buf = io.StringIO()
    fields = [
        "sim_date", "household_id", "grid_zone", "billing_tier", "readings", "consumption_kwh", "solar_kwh",
        "net_kwh", "import_kwh", "export_kwh", "solar_contribution_pct", "tariff_rate", "subsidy_flag",
        "tariff_source", "energy_charge", "subsidy_discount", "export_credit", "fixed_charge", "total_bill",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(bills)
    return buf.getvalue().encode()


def publish_report(sim_date: str, run_id: str) -> dict:
    started = time.time()
    settings = get_settings()
    conn = db.connect(settings)
    q = lambda sql: db.fetch_all(conn, sql, {"d": sim_date})  # noqa: E731

    bills = q("SELECT * FROM daily_household_bill WHERE sim_date = %(d)s ORDER BY household_id")
    data = {
        "totals": q(
            """SELECT count(*) AS households, sum(consumption_kwh) AS consumption_kwh,
                      sum(solar_kwh) AS solar_kwh, sum(import_kwh) AS import_kwh,
                      sum(export_kwh) AS export_kwh, sum(total_bill) AS total_billed
               FROM daily_household_bill WHERE sim_date = %(d)s"""
        )[0],
        "zones": q("SELECT * FROM daily_zone_summary WHERE sim_date = %(d)s ORDER BY grid_zone"),
        "top_bills": sorted(bills, key=lambda b: -b["total_bill"])[:10],
        "top_solar": sorted(bills, key=lambda b: -b["solar_kwh"])[:10],
        "reconciliation": q("SELECT * FROM reconciliation WHERE sim_date = %(d)s ORDER BY grid_zone"),
        "alerts": q(
            """SELECT * FROM grid_alerts WHERE window_start::date = %(d)s
               ORDER BY window_start, grid_zone"""
        ),
        "tariff_quality": {
            "loaded": q("SELECT count(*) AS n FROM tariffs WHERE dt = %(d)s")[0]["n"],
            "rejected": q("SELECT count(*) AS n FROM tariff_rejects WHERE dt = %(d)s")[0]["n"],
            "reasons": {
                r["reason"]: r["n"]
                for r in q("SELECT reason, count(*) AS n FROM tariff_rejects WHERE dt = %(d)s GROUP BY reason")
            },
            "fallbacks": sum(1 for b in bills if b["tariff_source"] != "same_day"),
        },
    }

    html = build_html(sim_date, run_id, data).encode()
    csv_bytes = build_csv(bills)
    html_key = f"daily/dt={sim_date}/billing_report.html"
    csv_key = f"daily/dt={sim_date}/household_bills.csv"
    s3c = s3.client(settings)
    s3c.put_object(Bucket=settings.bucket_reports, Key=html_key, Body=html, ContentType="text/html")
    s3c.put_object(Bucket=settings.bucket_reports, Key=csv_key, Body=csv_bytes, ContentType="text/csv")

    os.makedirs(LOCAL_REPORT_DIR, exist_ok=True)
    with open(os.path.join(LOCAL_REPORT_DIR, f"billing_report_{sim_date}.html"), "wb") as f:
        f.write(html)
    with open(os.path.join(LOCAL_REPORT_DIR, f"household_bills_{sim_date}.csv"), "wb") as f:
        f.write(csv_bytes)

    summary = {k: (round(v, 3) if isinstance(v, float) else v) for k, v in data["totals"].items()}
    summary["alerts"] = len(data["alerts"])
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_reports (sim_date, html_key, csv_key, summary, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sim_date) DO UPDATE SET
                html_key = EXCLUDED.html_key, csv_key = EXCLUDED.csv_key, summary = EXCLUDED.summary,
                pipeline_run_id = EXCLUDED.pipeline_run_id, generated_at = now()
            """,
            (sim_date, html_key, csv_key, json.dumps(summary), run_id),
        )
    conn.commit()
    result = {"html_key": html_key, "csv_key": csv_key, **summary}
    record_run(conn, run_id, "publish_report", sim_date, "success", int((time.time() - started) * 1000), result)
    conn.close()
    log.info("report_published", sim_date=sim_date, run_id=run_id, bucket=settings.bucket_reports, **result)
    return result
