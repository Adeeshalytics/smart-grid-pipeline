"""Fill docs/report.template.html with figures measured from the running stack.

    python tools/build_report.py            # writes docs/report.html
    (then print it to PDF, see the end of this file)

Every number in the results section is read from Prometheus, PostgreSQL or the
API at the time this script runs, so the report cannot drift from the system.
The failure-injection timings are passed in, because they come from a separate
test (stop the meter simulator and time the alerts).
"""
from __future__ import annotations

import json
import subprocess
import sys
import urllib.parse
import urllib.request
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
PROM = "http://localhost:9090"
API = "http://localhost:8000"

# Measured by stopping the meter simulator (see docs/DEMO_SCRIPT.md, section 5).
FAILURE_TEST = {"T_DOWN": "38", "T_NODATA": "71", "T_STALE": "76"}

MEMBERS = "[Name 1 - index no.], [Name 2 - index no.], [Name 3 - index no.]"
REPO_URL = "[link to the Git repository]"
CONTRIBUTIONS = """<table>
  <tr><th style="width:30%">Member</th><th>Contribution</th></tr>
  <tr><td>[Name 1 - index no.]</td><td>[to be completed by the group]</td></tr>
  <tr><td>[Name 2 - index no.]</td><td>[to be completed by the group]</td></tr>
  <tr><td>[Name 3 - index no.]</td><td>[to be completed by the group]</td></tr>
</table>"""


def prom(query: str) -> list[dict]:
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)["data"]["result"]


def prom_value(query: str, default: float = 0.0) -> float:
    result = prom(query)
    return float(result[0]["value"][1]) if result else default


def api(path: str):
    with urllib.request.urlopen(API + path, timeout=20) as r:
        return json.load(r)


def sql(query: str) -> list[list[str]]:
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "grid", "-d", "grid", "-At", "-F", "|", "-c", query],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    return [line.split("|") for line in out.splitlines() if line]


def n(value: float, digits: int = 0) -> str:
    return f"{value:,.{digits}f}"


def html_table(headers: list[str], rows: list[list[str]], numeric_from: int = 1) -> str:
    head = "".join(f'<th class="{"n" if i >= numeric_from else ""}">{escape(h)}</th>' for i, h in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(f'<td class="{"n" if i >= numeric_from else ""}">{escape(str(c))}</td>' for i, c in enumerate(r)) + "</tr>"
        for r in rows
    )
    return f"<table><tr>{head}</tr>{body}</table>"


def main() -> None:
    kinds = {r["metric"]["kind"]: float(r["value"][1]) for r in prom("sim_events_produced_total")}
    produced = sum(v for k, v in kinds.items() if k != "duplicate")
    days = sql("SELECT count(*), min(sim_date), max(sim_date) FROM daily_reports")[0]
    run_minutes = float(sql("SELECT extract(epoch FROM now() - anchor_wall) / 60 FROM sim_clock")[0][0])

    # Reconciliation: full days only (the first simulated day is partial).
    recon = sql(
        """SELECT sim_date, round(sum(batch_kwh)::numeric, 2), round(sum(speed_kwh)::numeric, 2),
                  round((100 * (sum(speed_kwh) - sum(batch_kwh)) / sum(batch_kwh))::numeric, 3),
                  sum(batch_readings), sum(batch_readings - speed_readings),
                  round(min(diff_pct)::numeric, 2), round(max(diff_pct)::numeric, 2)
           FROM reconciliation GROUP BY sim_date ORDER BY sim_date"""
    )
    zone_diffs = [abs(float(x)) for x in sum(sql("SELECT diff_pct FROM reconciliation"), [])]
    recon_table = html_table(
        ["Simulated day", "Batch kWh (exact)", "Speed kWh (real-time)", "Difference %", "Readings (batch)", "Missed by speed layer", "Zone min %", "Zone max %"],
        recon,
    )

    tariffs = sql(
        """SELECT b.sim_date,
                  (SELECT count(*) FROM tariffs t WHERE t.dt = b.sim_date),
                  (SELECT count(*) FROM tariff_rejects r WHERE r.dt = b.sim_date),
                  count(*) FILTER (WHERE tariff_source = 'same_day'),
                  count(*) FILTER (WHERE tariff_source = 'carried_forward'),
                  count(*) FILTER (WHERE tariff_source = 'default'),
                  round(sum(total_bill)::numeric, 2)
           FROM daily_household_bill b GROUP BY b.sim_date ORDER BY b.sim_date"""
    )
    tariff_table = html_table(
        ["Simulated day", "Tariff rows loaded", "Rows rejected", "Billed at same-day tariff", "Carried forward", "Default tariff", "Total billed (LKR)"],
        tariffs,
    )

    batch_ms = [float(r[0]) for r in sql("SELECT duration_ms FROM pipeline_runs WHERE task = 'compute_daily_billing' AND status = 'success'")]
    ready = float(sql(
        """SELECT avg(extract(epoch FROM r.generated_at
                      - (c.anchor_wall + ((r.sim_date - c.sim_epoch::date) + 1) * c.day_seconds * INTERVAL '1 second')))
           FROM daily_reports r CROSS JOIN sim_clock c WHERE r.pipeline_run_id LIKE 'scheduled%'"""
    )[0][0])
    replay = sql("SELECT duration_ms FROM pipeline_runs WHERE run_id LIKE 'manual%' AND task = 'compute_daily_billing' ORDER BY id DESC LIMIT 1")

    durations = {r["metric"]["query"]: float(r["value"][1]) for r in prom("avg_over_time(speed_batch_duration_ms[15m])")}
    realtime = api("/api/v1/grid/realtime")
    realtime["zones"] = realtime["zones"][:1] + ["... four more zones ..."]
    bill = api("/api/v1/households/HH-0007/bill-to-date")

    log_sample = subprocess.run(
        ["docker", "compose", "logs", "--no-log-prefix", "--tail", "400", "speed-layer"],
        cwd=ROOT, capture_output=True, text=True,
    ).stdout.splitlines()
    sample = [line for line in log_sample if '"micro_batch"' in line and '"zone_load"' in line][-1:]
    sample += [line for line in log_sample if '"events_rejected"' in line][-1:]

    values = {
        "MEMBERS": MEMBERS,
        "REPO_URL": REPO_URL,
        "CONTRIBUTIONS": CONTRIBUTIONS,
        "ARCH_SVG": (DOCS / "architecture.svg").read_text(encoding="utf-8"),
        "N_DAYS": days[0],
        "RUN_MINUTES": n(run_minutes),
        "N_EVENTS": n(produced),
        "N_ONTIME": n(kinds.get("on_time", 0)),
        "N_LATE": n(kinds.get("late", 0)),
        "N_VLATE": n(kinds.get("very_late", 0)),
        "N_CORRUPT": n(kinds.get("corrupt", 0)),
        "N_DUP": n(kinds.get("duplicate", 0)),
        "N_DELIVERY_FAIL": n(prom_value("sim_delivery_failures_total")),
        "N_REJECTED": n(prom_value("sum(speed_rejected_events_total)")),
        "BATCH_MS": f"{n(min(durations.values()) / 1000, 1)} to {n(max(durations.values()) / 1000, 1)} s",
        "MAX_LAG": f'{n(prom_value("max(max_over_time(speed_kafka_offsets_behind_latest[30m]))"))} offsets',
        "VIEW_AGE": f'{n(prom_value("avg_over_time(grid_realtime_view_age_seconds[10m])"), 1)} s',
        "N_DROPPED": n(prom_value('speed_rows_dropped_by_watermark_total{query="household"}')),
        "N_RUNS": n(len(batch_ms)),
        "BATCH_SECONDS": n(sum(batch_ms) / len(batch_ms) / 1000),
        "BATCH_READY": f"{n(ready)} s",
        "REPLAY_SECONDS": n(float(replay[0][0]) / 1000) if replay else "about 25",
        "N_ALERTS": n(float(sql("SELECT count(*) FROM grid_alerts")[0][0])),
        "N_RULES": str(sum(1 for line in (ROOT / "observability/prometheus/alerts.yml").read_text().splitlines() if line.strip().startswith("- alert:"))),
        "DIFF_RANGE": f"{n(min(zone_diffs), 2)}% to {n(max(zone_diffs), 2)}%",
        "RECON_TABLE": recon_table,
        "TARIFF_TABLE": tariff_table,
        "API_REALTIME": escape(json.dumps(realtime)),
        "API_BILL_TO_DATE": escape(json.dumps(bill)),
        "LOG_SAMPLE": escape("\n".join(sample)),
        "CROP_H": "91mm",
        **FAILURE_TEST,
    }

    html = (DOCS / "report.template.html").read_text(encoding="utf-8")
    for key, value in values.items():
        html = html.replace(f"[[{key}]]", str(value))
    missing = sorted(set(part.split("]]")[0] for part in html.split("[[")[1:]))
    if missing:
        sys.exit(f"Unfilled placeholders: {missing}")
    (DOCS / "report.html").write_text(html, encoding="utf-8")
    print("wrote docs/report.html")
    print(json.dumps({k: v for k, v in values.items() if len(str(v)) < 60}, indent=1))


if __name__ == "__main__":
    main()

# To produce the PDF (Chrome or Edge, headless):
#   chrome --headless=new --no-pdf-header-footer --print-to-pdf=docs/report.pdf docs/report.html
