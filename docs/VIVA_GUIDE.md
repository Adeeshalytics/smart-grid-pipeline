# Viva guide

The brief says you must be able to explain and defend every architectural
decision and every line of core pipeline logic. This guide is the study sheet
for that. Read it with the code open. It is organised by the marking rubric.

Core files to know line by line, in this order:

1. `src/processing/transforms.py` (the business logic, about 190 lines)
2. `src/processing/speed_layer.py` (streaming job)
3. `src/processing/batch_billing.py` (batch job)
4. `airflow/dags/grid_daily_billing.py` and `src/batch/tasks.py` (orchestration)
5. `src/gridlib/simclock.py`, `billing.py`, `schemas.py`, `tariffs.py`
6. `src/simulators/*.py`

---

## 1. Architecture decision (20 marks)

**Q: Why Lambda and not Kappa?**
The use case has two requirements that pull in opposite directions. Grid
monitoring needs an answer in seconds and tolerates a small error. Billing needs
an exact, re-computable answer and cannot be final until the tariff file arrives
once a day. Lambda gives each its own layer. In Kappa the bill would come from
the streaming job, so its accuracy would depend on the watermark.

**Q: What exactly goes wrong with billing in a stream?**
A stream must drop data that is later than the watermark, otherwise its state
grows forever. Our speed layer has a 30-minute watermark and the simulator delays
some events by up to 3 hours. Those are dropped by the streaming aggregations. We
measured it: the speed layer is a fraction of a percent low on every day (see the
reconciliation table). The batch layer reads the raw files with no watermark and
counts everything.

**Q: Could Kappa not just use a bigger watermark?**
A bigger watermark delays every result and enlarges state, and there is still
always a later event. It reduces the loss; it cannot make it zero.

**Q: How does Kappa reprocess, and why is ours simpler here?**
Kappa replays the topic from an old offset through a new copy of the job and
then switches over. It needs the full history in Kafka. We re-run one idempotent
job on one Parquet partition: `airflow dags trigger grid_daily_billing --conf
'{"sim_date": "2026-01-02"}'`.

**Q: What is the weakness of Lambda and what did you do about it?**
Two code paths that can drift. We use one engine (Spark) for both, put all
transformations in `transforms.py` as functions of a DataFrame that both jobs
call, keep the constants in plain Python, test that Spark billing equals the
reference implementation, and measure the difference between the two layers
every day (`reconcile` task, metric, alert above 5%).

**Q: When would you choose Kappa instead?**
If tariffs were a real-time price stream, if bills were only advisory, or if the
team could not maintain two paths. Also a lakehouse format (Iceberg/Delta) with
exactly-once streaming narrows the gap.

**Q: What are the three layers of Lambda in your code?**
Batch layer: Airflow DAG + `batch_billing.py`, master dataset in
`raw/meter_readings`. Speed layer: `speed_layer.py`. Serving layer: PostgreSQL
tables + `api.py`; the merge is `/households/{id}/bill-to-date`.

**Q: Where is the master dataset and why is it immutable?**
`s3a://grid-lake/raw/meter_readings`, written append-only by the `archive`
query. Immutable means any result can be recomputed from it, and a bug in the
processing code cannot corrupt the source.

---

## 2. Technology choices (10 marks)

| Question | Answer |
|---|---|
| Why Kafka? | Durable replayable log; four queries read the same events independently; a restarted query resumes from its offset. |
| Why 6 partitions keyed by `meter_id`? | Ordering per meter, and 120 keys spread evenly. Keying by zone would give 5 hot keys. 6 allows parallel consumers later. |
| Why Spark Structured Streaming and not Storm? | Event-time windows, watermark, stateful dedup and checkpointing are built in, and it shares the DataFrame API with the batch job. Storm has lower per-event latency, which a 15-minute window does not need, and would force a second copy of the logic. |
| Why Airflow? | The batch path has external waits (file arrival, grace period), dependencies, retries and re-runs by parameter. cron has none of these. |
| Why Parquet on an object store? | Columnar, compressed, immutable, cheap; the daily job reads one partition. |
| Why PostgreSQL for serving and not Cassandra? | The views are small and keyed; we need upserts (idempotent sinks), transactions (atomic daily swap) and joins (API, Grafana). Cassandra is the answer at millions of meters, at the cost of transactions and joins. |
| Why SeaweedFS? | It is an S3-compatible store. MinIO's public images were withdrawn. Only the endpoint changed, no pipeline code. |
| Why FastAPI? | The brief asks for an API endpoint; it generates OpenAPI docs. |

---

## 3. Ingestion (15 marks)

**Q: Walk through the stream simulator.** (`stream_meter_simulator.py`)
- Loads the shared clock from PostgreSQL, builds 120 households from a seed.
- Loops over households, sleeping `interval / 120` between sends, so each meter
  reports once per 15 simulated minutes and the flow is steady.
- `build_event` makes the reading from the physical model in `fleet.py`.
- A random roll decides if the event is corrupt, late, very late or on time. Late
  events go onto a heap with a release time and are sent when due.
- 1% of on-time events are sent twice with the same `event_id`.
- Producer settings: `enable.idempotence=True`, `acks=all` (no loss, no
  broker-side duplicates on retry), `linger.ms=50` and lz4 for batching.

**Q: How do readings get realistic values?** (`fleet.py`)
Demand is a base load plus two Gaussian peaks (07:30 and 20:00) times the
household's scale. Solar is `kw_peak * sin(pi*(hour-6)/12)^1.5 * cloud` between
06:00 and 18:00. `cloud_factor` is deterministic per zone and day, and is
overcast (5 to 15%) for about one zone-day in four.

**Q: How does the batch source signal a complete file?** (`batch_tariff_dropper.py`)
It uploads `tariffs.csv`, then `_manifest.json` with row count and SHA-256. The
sensor waits for the manifest, so a half-uploaded CSV is never read. The loader
recomputes the hash.

**Q: What happens to a bad event?**
`with_reject_reason` sets a reason (malformed JSON, missing field, bad
timestamp, unknown zone, value out of range). The `dlq` query writes the raw
payload, reason, partition and offset to `meter.readings.dlq`.

**Q: What happens to a bad tariff file?**
Checksum or row-count mismatch: the task fails. More than 20% bad rows: the file
is refused. Otherwise bad rows go to `tariff_rejects` and good rows load.
Households missing from the file get yesterday's tariff (`carried_forward`).

**Q: How does the simulated clock work?** (`simclock.py`)
`sim = sim_epoch + (wall - anchor) * 288`. The anchor is stored once in
`sim_clock`, floored to a 5-minute boundary so the Airflow cron `*/5` lines up
with simulated midnight.

---

## 4. Processing (15 marks)

**Q: Explain the watermark.**
`withWatermark("event_ts", "30 minutes")` tells Spark that events more than 30
minutes older than the newest event time seen may be dropped, which lets it
close windows and free state.

**Q: How are duplicates removed?**
Streaming: `dropDuplicatesWithinWatermark(["event_id"])` keeps ids in state only
for the watermark period. Batch: `dropDuplicates(["event_id"])` over the whole
day, which is exact.

**Q: Explain `zone_window_load`.**
Group by a 15-minute tumbling window on event time and by zone; sum consumption
and solar; approximate distinct meters (HyperLogLog, 1% error); count readings;
then net load = consumption - solar and renewable share = solar / consumption.

**Q: Why `outputMode("update")` with `foreachBatch`?**
Update mode emits only the windows that changed in this micro-batch.
`foreachBatch` lets us write them with an upsert (`ON CONFLICT ... DO UPDATE`),
which makes the sink idempotent.

**Q: Is it exactly-once?**
Reading is exactly-once through checkpointed offsets. The PostgreSQL sinks are
idempotent, so a replayed batch produces the same rows: effectively exactly-once.
The Parquet file sink is exactly-once through its metadata log. The two Kafka
sinks (DLQ, alerts) are at-least-once.

**Q: How does the speed layer join the two sources?**
In `write_household_today`, each micro-batch re-reads `v_latest_tariff` over JDBC
and left-joins it (a stream-static join), then applies `with_billing` to get a
provisional bill.

**Q: How is the low-renewable alert evaluated?**
After each upsert, one SQL statement aggregates closed windows by hour, keeps
hours between 10:00 and 15:00 with all four windows present and share below 20%,
and inserts into `grid_alerts` with a unique key, so re-evaluation cannot
duplicate an alert. New alerts are also logged and published to `grid.alerts`.

**Q: Walk through the batch job.** (`batch_billing.py`)
Read the day's partition; count; `deduplicate`; `household_daily_totals`;
`tariffs_as_of` (latest tariff on or before the day per household);
`attach_tariff` (left join, default if none); `with_billing`; write to the
staging table by JDBC; one transaction deletes the day and inserts from staging
and rebuilds the zone summary; write Parquet to the curated zone; record the
run; print a JSON summary.

**Q: Why is it idempotent?**
Every write replaces exactly one day: delete + insert in one transaction, and
`mode("overwrite")` on the day's curated path. We re-ran a day and got identical
totals.

**Q: Walk through the DAG.**
`resolve_sim_date` (which day; skip if none finished) -> `wait_for_tariff_file`
(sensor) -> `load_tariffs`; in parallel `wait_for_late_readings` (45 s grace) ->
`compute_daily_billing` (spark-submit) -> `reconcile` -> `quality_gate` ->
`publish_report`. Retries: 2 with 20 s delay. `max_active_runs=1`.
`on_failure_callback` writes to `pipeline_runs`.

**Q: What does the quality gate check?**
Bills exist, no invalid values, at most 25% fallback tariffs, and speed/batch
difference at most 25%. It fails the run rather than publish a bad report.

**Q: The billing formula?**
`net = consumption - solar`; `import = max(net, 0)`; `export = max(-net, 0)`;
`energy = import * rate * tier multiplier (1.00 / 1.25 / 1.60)`; minus 20%
subsidy if flagged; minus `export * rate * 0.5`; plus a fixed charge of 15.

---

## 5. Storage and serving (10 marks)

**Q: Which tables are which layer?**
`rt_zone_load`, `rt_household_today`, `grid_alerts`: speed layer.
`daily_household_bill`, `daily_zone_summary`, `reconciliation`, `daily_reports`:
batch layer. `tariffs`, `tariff_rejects`: reference. `pipeline_runs`,
`ops_alerts`: lineage and operations.

**Q: How does the serving layer merge the views?**
`bill_to_date` sums `daily_household_bill` for billed days and adds
`rt_household_today.provisional_bill` for days after the last billed day.

**Q: What is `v_grid_now`?**
The latest *closed* window per zone (window end <= the newest window start), so
the API never shows a half-filled window.

**Q: Why a staging table?**
Spark bulk-loads it; the swap into the real table is one transaction, so readers
never see a partial day.

---

## 6. Observability (10 marks)

**Q: What is logged and how?**
One JSON formatter (`logging_utils.py`) for all components: `ts, level, service,
stage, event` plus context such as `batch_id`, `run_id`, `sim_date`.

**Q: What metrics and from where?**
Four exporters: simulator (9101), dropper (9102), Spark driver (9103, fed by a
`StreamingQueryListener`), API (8000, including gauges read from the serving
store: view age, report age, reconciliation difference).

**Q: Name the alert rules the brief requires.**
"No data in N minutes": `NoMeterDataReceived` (input rate is 0 for 30 s).
"Error rate above threshold": `HighRejectRate` (rejects / input > 5%).

**Q: How would you diagnose "the dashboard is frozen"?**
Pipeline Health: is input rate 0 (source or Kafka) or is lag growing (Spark is
slow or stuck)? `up{job=...}` says which process is down. The speed-layer log
shows the last `micro_batch` and any exception.

**Q: Do you have tracing?**
Not a tracer with spans. We carry identifiers: `event_id`, Kafka
partition/offset, `batch_id`, Airflow `run_id` on every bill row and in
`pipeline_runs`, `x-request-id` on API calls. Say this plainly; it is listed as a
limitation.

---

## 7. Limitations you should state before being asked

- Single broker, replication factor 1; Spark local mode; one PostgreSQL.
- Readings later than the 45 s grace period need a re-run to reach the bill.
- The watermark advances only between micro-batches, so tolerance is coarser
  than 30 minutes; mostly the very late events are dropped.
- About 30 small Parquet files per day; needs compaction at scale.
- JSON with no schema registry.
- Changing a stateful aggregation invalidates the checkpoint.
- DLQ and alert topics are at-least-once.
- Development credentials, no TLS.

## 8. Numbers to remember

| | |
|---|---|
| Time compression | 1 simulated day = 300 s (288x) |
| Meters / zones | 120 / 5 |
| Reading interval | 15 simulated min = 3.125 s |
| Event rate | about 38 per second |
| Watermark | 30 simulated min |
| Zone window | 15 simulated min, tumbling |
| Triggers | 5 s (zone), 10 s (others) |
| Fault rates | corrupt 0.5%, duplicate 1%, late 2%, very late 0.5% |
| Alert threshold | renewable share < 20%, 10:00 to 15:00 |
| Batch grace period | 45 s |
| Tests | 29 |
