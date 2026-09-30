"""Audit trail for the batch layer: one row in `pipeline_runs` per task execution."""
from __future__ import annotations

import json
from typing import Any


def record_run(
    conn,
    run_id: str,
    task: str,
    sim_date: str | None,
    status: str,
    duration_ms: int | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (run_id, task, sim_date, status, duration_ms, details)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, task, sim_date, status, duration_ms, json.dumps(details or {}, default=str)),
        )
    conn.commit()
