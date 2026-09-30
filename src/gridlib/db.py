"""PostgreSQL helpers (serving store)."""
from __future__ import annotations

import time

import psycopg2
import psycopg2.extras

from .config import Settings


def connect(settings: Settings, retries: int = 30, delay: float = 2.0):
    """Open a connection, retrying while the database container starts up."""
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(settings.pg_dsn)
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
            conn.commit()
            return conn
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"PostgreSQL not reachable after {retries} attempts") from last_error


def execute_values(conn, sql: str, rows: list[tuple], page_size: int = 500) -> int:
    """Bulk insert/upsert. Returns the number of rows sent."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
    conn.commit()
    return len(rows)


def fetch_all(conn, sql: str, params: tuple | dict | None = None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.commit()
    return rows
