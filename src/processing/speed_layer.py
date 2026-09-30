"""Speed layer: Spark Structured Streaming over the meter.readings topic."""
from __future__ import annotations

import json
import time

import psycopg2
from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from gridlib import db
from gridlib.config import Settings, get_settings
from gridlib.logging_utils import get_logger
from processing import transforms as T
from processing.spark_session import build_spark, jdbc_options

log = get_logger("speed-layer", "processing")
settings: Settings = get_settings()

INPUT_ROWS = Counter("speed_input_rows_total", "Rows read from Kafka", ["query"])
INPUT_RATE = Gauge("speed_input_rows_per_second", "Arrival rate", ["query"])
PROCESS_RATE = Gauge("speed_processed_rows_per_second", "Processing rate", ["query"])
BATCH_DURATION = Gauge("speed_batch_duration_ms", "Micro-batch duration", ["query"])
KAFKA_LAG = Gauge("speed_kafka_offsets_behind_latest", "Max consumer lag in offsets", ["query"])
LATE_DROPPED = Counter("speed_rows_dropped_by_watermark_total", "Rows too late for the window", ["query"])
STATE_ROWS = Gauge("speed_state_rows", "Rows held in the state store", ["query"])
LAST_PROGRESS = Gauge("speed_last_progress_unix_seconds", "Last micro-batch completion time", ["query"])
REJECTED = Counter("speed_rejected_events_total", "Events sent to the dead-letter topic", ["reason"])
SINK_ROWS = Counter("speed_sink_rows_total", "Rows written to a sink", ["sink"])
SINK_ERRORS = Counter("speed_sink_errors_total", "Failed sink writes", ["sink"])
GRID_ALERTS = Counter("grid_alerts_raised_total", "Business alerts raised", ["alert_type", "zone"])
QUERIES_ACTIVE = Gauge("speed_active_queries", "Streaming queries currently running")


class MetricsListener(StreamingQueryListener):
    """Turns Spark's per-batch progress reports into Prometheus metrics and logs."""

    def __init__(self):
        super().__init__()
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event):
        self._names[str(event.id)] = event.name
        log.info("query_started", query=event.name, query_id=str(event.id))

    def onQueryProgress(self, event):
        p = event.progress
        name = p.name
        INPUT_ROWS.labels(name).inc(p.numInputRows)
        INPUT_RATE.labels(name).set(p.inputRowsPerSecond or 0)
        PROCESS_RATE.labels(name).set(p.processedRowsPerSecond or 0)
        BATCH_DURATION.labels(name).set(p.durationMs.get("triggerExecution", 0))
        LAST_PROGRESS.labels(name).set(time.time())
        lag = 0
        for source in p.sources:
            lag = max(lag, int(float((source.metrics or {}).get("maxOffsetsBehindLatest", 0))))
        KAFKA_LAG.labels(name).set(lag)
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        if dropped:
            LATE_DROPPED.labels(name).inc(dropped)
        STATE_ROWS.labels(name).set(sum(op.numRowsTotal for op in p.stateOperators))
        if p.numInputRows:
            log.info(
                "micro_batch",
                query=name,
                batch_id=p.batchId,
                input_rows=p.numInputRows,
                duration_ms=p.durationMs.get("triggerExecution"),
                kafka_lag=lag,
                dropped_late=dropped,
                watermark=(p.eventTime or {}).get("watermark"),
            )

    def onQueryIdle(self, event):
        name = self._names.get(str(event.id))
        if name:
            LAST_PROGRESS.labels(name).set(time.time())
            INPUT_RATE.labels(name).set(0)

    def onQueryTerminated(self, event):
        log.error("query_terminated", query_id=str(event.id), exception=event.exception)


_conn = None


def pg():
    """A driver-side connection, re-opened if PostgreSQL restarted."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = db.connect(settings)
    return _conn


def upsert(sink: str, sql: str, rows: list[tuple]) -> None:
    """Idempotent write: replaying a micro-batch after a failure rewrites the same keys."""
    global _conn
    try:
        db.execute_values(pg(), sql, rows)
        SINK_ROWS.labels(sink).inc(len(rows))
    except psycopg2.Error:
        SINK_ERRORS.labels(sink).inc()
        if _conn is not None:
            _conn.close()
        _conn = None
        raise


ZONE_UPSERT = """
INSERT INTO rt_zone_load (window_start, window_end, grid_zone, consumption_kwh, solar_kwh,
                          net_load_kwh, renewable_share, active_meters, readings)
VALUES %s
ON CONFLICT (window_start, grid_zone) DO UPDATE SET
    consumption_kwh = EXCLUDED.consumption_kwh,
    solar_kwh       = EXCLUDED.solar_kwh,
    net_load_kwh    = EXCLUDED.net_load_kwh,
    renewable_share = EXCLUDED.renewable_share,
    active_meters   = EXCLUDED.active_meters,
    readings        = EXCLUDED.readings,
    updated_at      = now()
"""

HOUSEHOLD_UPSERT = """
INSERT INTO rt_household_today (sim_date, household_id, grid_zone, consumption_kwh, solar_kwh,
                                readings, tariff_rate, billing_tier, subsidy_flag, tariff_source,
                                provisional_bill)
VALUES %s
ON CONFLICT (sim_date, household_id) DO UPDATE SET
    consumption_kwh  = EXCLUDED.consumption_kwh,
    solar_kwh        = EXCLUDED.solar_kwh,
    readings         = EXCLUDED.readings,
    tariff_rate      = EXCLUDED.tariff_rate,
    billing_tier     = EXCLUDED.billing_tier,
    subsidy_flag     = EXCLUDED.subsidy_flag,
    tariff_source    = EXCLUDED.tariff_source,
    provisional_bill = EXCLUDED.provisional_bill,
    updated_at       = now()
"""

LOW_RENEWABLE_ALERT = """
WITH closed AS (
    SELECT * FROM rt_zone_load
    WHERE window_end <= (SELECT max(window_start) FROM rt_zone_load)
      AND window_start >= (SELECT max(window_start) FROM rt_zone_load) - INTERVAL '4 hours'
), hourly AS (
    SELECT grid_zone,
           date_trunc('hour', window_start)                     AS hour_start,
           sum(solar_kwh) / NULLIF(sum(consumption_kwh), 0)     AS share,
           count(*)                                             AS windows
    FROM closed
    GROUP BY 1, 2
)
INSERT INTO grid_alerts (alert_type, grid_zone, window_start, severity, value, threshold, message)
SELECT 'LOW_RENEWABLE_CONTRIBUTION', grid_zone, hour_start,
       CASE WHEN share < %(threshold)s / 2 THEN 'critical' ELSE 'warning' END,
       round(share::numeric, 4), %(threshold)s,
       format('Renewable contribution in %%s was %%s%%%% for the hour starting %%s (threshold %%s%%%%)',
              grid_zone, round((share * 100)::numeric, 1),
              to_char(hour_start, 'YYYY-MM-DD HH24:MI'), round((%(threshold)s * 100)::numeric, 0))
FROM hourly
WHERE windows >= 4
  AND extract(hour FROM hour_start) >= %(start_hour)s
  AND extract(hour FROM hour_start) <  %(end_hour)s
  AND share < %(threshold)s
ON CONFLICT (alert_type, grid_zone, window_start) DO NOTHING
RETURNING alert_type, grid_zone, window_start, severity, value, threshold, message
"""


def raise_low_renewable_alerts(spark: SparkSession) -> None:
    conn = pg()
    with conn.cursor() as cur:
        cur.execute(
            LOW_RENEWABLE_ALERT,
            {
                "threshold": settings.low_renewable_threshold,
                "start_hour": settings.daylight_start_hour,
                "end_hour": settings.daylight_end_hour,
            },
        )
        new_alerts = cur.fetchall()
    conn.commit()
    if not new_alerts:
        return
    messages = []
    for alert_type, zone, window_start, severity, value, threshold, message in new_alerts:
        GRID_ALERTS.labels(alert_type, zone).inc()
        log.warning(
            "grid_alert_raised",
            alert_type=alert_type,
            grid_zone=zone,
            hour_start=window_start,
            severity=severity,
            renewable_share=float(value),
            threshold=threshold,
        )
        payload = {
            "alert_type": alert_type,
            "grid_zone": zone,
            "hour_start": window_start.isoformat(),
            "severity": severity,
            "renewable_share": float(value),
            "threshold": threshold,
            "message": message,
        }
        messages.append((zone, json.dumps(payload)))
    (
        spark.createDataFrame(messages, "key string, value string")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("topic", settings.topic_alerts)
        .save()
    )


def write_dlq(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    try:
        counts = {r["reject_reason"]: r["count"] for r in batch_df.groupBy("reject_reason").count().collect()}
        if not counts:
            return
        (
            batch_df.select(
                F.col("kafka_key").alias("key"),
                F.to_json(
                    F.struct(
                        "raw_value",
                        "reject_reason",
                        "kafka_partition",
                        "kafka_offset",
                        "kafka_ts",
                        F.lit(batch_id).alias("batch_id"),
                    )
                ).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("topic", settings.topic_dlq)
            .save()
        )
        for reason, count in counts.items():
            REJECTED.labels(reason).inc(count)
        log.warning("events_rejected", batch_id=batch_id, by_reason=counts, dlq_topic=settings.topic_dlq)
    finally:
        batch_df.unpersist()


def write_zone_load(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r.window_start,
            r.window_end,
            r.grid_zone,
            round(r.consumption_kwh, 4),
            round(r.solar_kwh, 4),
            round(r.net_load_kwh, 4),
            round(r.renewable_share, 4),
            r.active_meters,
            r.readings,
        )
        for r in batch_df.collect()
    ]
    if not rows:
        return
    upsert("rt_zone_load", ZONE_UPSERT, rows)
    raise_low_renewable_alerts(batch_df.sparkSession)


def write_household_today(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    tariffs = (
        spark.read.format("jdbc")
        .options(**jdbc_options(settings), dbtable="v_latest_tariff")
        .load()
        .select("household_id", "tariff_rate", "billing_tier", "subsidy_flag")
        .withColumn("tariff_source", F.lit("latest_known"))
    )
    billed = T.with_billing(T.attach_tariff(batch_df, tariffs, settings.default_tariff_rate))
    rows = [
        (
            r.sim_date,
            r.household_id,
            r.grid_zone,
            round(r.consumption_kwh, 4),
            round(r.solar_kwh, 4),
            r.readings,
            r.tariff_rate,
            r.billing_tier,
            r.subsidy_flag,
            r.tariff_source,
            r.total_bill,
        )
        for r in billed.collect()
    ]
    if rows:
        upsert("rt_household_today", HOUSEHOLD_UPSERT, rows)


def main() -> None:
    start_http_server(9103)
    spark = build_spark("grid-speed-layer", settings)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(MetricsListener())

    def kafka_source() -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.topic_readings)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50000)
            .option("failOnDataLoss", "false")
            .load()
        )

    def checked() -> DataFrame:
        return T.with_reject_reason(T.parse_kafka(kafka_source()))

    ckpt = settings.checkpoint_dir

    (
        T.rejected_readings(checked())
        .writeStream.queryName("dlq")
        .foreachBatch(write_dlq)
        .option("checkpointLocation", f"{ckpt}/dlq")
        .trigger(processingTime="10 seconds")
        .start()
    )

    (
        T.valid_readings(checked())
        .coalesce(1)
        .writeStream.queryName("archive")
        .format("parquet")
        .option("path", settings.lake_raw_path)
        .option("checkpointLocation", f"{ckpt}/archive")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    (
        T.zone_window_load(
            T.deduplicate(T.valid_readings(checked()), settings.watermark), settings.zone_window
        )
        .writeStream.queryName("zone_load")
        .outputMode("update")
        .foreachBatch(write_zone_load)
        .option("checkpointLocation", f"{ckpt}/zone_load")
        .trigger(processingTime="5 seconds")
        .start()
    )

    (
        T.household_daily_totals(T.deduplicate(T.valid_readings(checked()), settings.watermark))
        .writeStream.queryName("household")
        .outputMode("update")
        .foreachBatch(write_household_today)
        .option("checkpointLocation", f"{ckpt}/household")
        .trigger(processingTime="10 seconds")
        .start()
    )

    QUERIES_ACTIVE.set(len(spark.streams.active))
    log.info(
        "speed_layer_started",
        queries=[q.name for q in spark.streams.active],
        watermark=settings.watermark,
        zone_window=settings.zone_window,
    )
    spark.streams.awaitAnyTermination()
    QUERIES_ACTIVE.set(len(spark.streams.active))
    log.error("speed_layer_exiting", reason="a streaming query terminated")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
