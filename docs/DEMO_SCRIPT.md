# Demo script (5-10 minute video or live demo)

Target length: about 8 minutes. Everything below is timed against the simulated
clock: **one simulated day = 5 minutes**, and a daily batch run starts on every
5-minute wall-clock boundary (hh:00, hh:05, hh:10 ...).

## Before you record

1. Start the stack **at least 12 minutes before recording**, so two daily reports
   already exist:
   ```bash
   docker compose up -d --build
   ```
2. Check everything is up:
   ```bash
   docker compose ps
   ```
   `init` should show `Exited (0)`; everything else `Up`.
3. Open these tabs in this order:
   1. http://localhost:3000/d/grid-ops (Grid Operations)
   2. http://localhost:8000/docs (API)
   3. http://localhost:8080 (Airflow, admin / admin) and open the `grid_daily_billing` DAG, Graph view
   4. http://localhost:8000/api/v1/reports/latest (daily report)
   5. http://localhost:3000/d/grid-pipeline (Pipeline Health)
   6. http://localhost:9090/alerts (Prometheus alerts)
4. Have one terminal open in the project folder.
5. Aim to start recording about 1 minute after a 5-minute boundary, so a DAG run is
   visibly in progress during section 4.

## 1. Introduction (45 s)

Say:
- "This is Use Case 3, smart grid monitoring and billing, built as a Lambda architecture."
- "The business question has two halves. What is the grid load and solar
  contribution by zone right now, and what will each household's bill be once the
  daily tariff file is applied."
- "One simulated day is five minutes. 120 households in five zones each send a
  reading every 15 simulated minutes."

Show: the architecture diagram from the report (or the README).

## 2. The two sources (60 s)

Terminal:
```bash
docker compose logs --tail=5 meter-simulator tariff-dropper
```
Say: "Both sources are Python scripts. The stream simulator deliberately injects
corrupt events, duplicates and late events. The batch source drops one tariff
CSV per simulated day, with a checksum manifest, and the file contains bad rows
on purpose."

Show one raw event from Kafka:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic meter.readings --max-messages 2
```

## 3. Speed layer: the real-time answer (90 s)

Tab 1, Grid Operations dashboard.
- Point at "Simulated time now" and the four time-series panels moving.
- "These are 15-minute tumbling windows per zone from Spark Structured Streaming,
  with a 30-minute watermark and de-duplication by event id."
- Point at the renewable-contribution panel and the red 20% line, then the alert
  table below it: "When a zone's hourly solar share stays under 20% in daylight,
  the speed layer raises an alert. It is stored, logged and published to Kafka."

Tab 2, API. Execute `GET /api/v1/grid/realtime`.
- "This is the API endpoint the brief asks for: load and renewable mix by zone."

Terminal, show rejected events:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic meter.readings.dlq --from-beginning --max-messages 3
```
- "Invalid events are never dropped silently. They go to a dead-letter topic with the reason."

## 4. Batch layer: the exact answer (120 s)

Tab 3, Airflow graph view of the running or latest run.
- Walk the tasks left to right: "Wait for the tariff file, validate and load it,
  wait for late readings, run the Spark batch job over the raw Parquet, reconcile
  against the speed layer, pass a quality gate, publish the report."
- Click `load_tariffs` > Logs or XCom: show loaded vs rejected rows.

Tab 4, the daily report.
- "This is the consolidated daily report: bills per household, solar contribution,
  totals by zone."
- Scroll to the reconciliation table: "The batch layer recomputes from raw data, so
  it includes readings that arrived too late for the streaming windows. This table
  shows how much the real-time view missed. That is the reason for Lambda."

API tab: execute `GET /api/v1/households/HH-0007/bill-to-date`.
- "The serving layer merges both: exact bills for closed days plus the live
  estimate for today."

## 5. Observability and a failure (150 s)

Tab 5, Pipeline Health. Point at: events per second by kind, Kafka lag, batch
duration, rejected events by reason, rows dropped by the watermark, data freshness,
the audit trail table.

Terminal, show structured logs:
```bash
docker compose logs --tail=3 speed-layer
```
- "Every stage writes JSON logs with the same envelope: service, stage, event."

Now break it:
```bash
docker compose stop meter-simulator
```
- Keep talking for about 90 seconds (explain the alert rules in tab 6). Then show:
  - Tab 6: `MeterSimulatorDown` goes red after about 40 seconds, then
    `NoMeterDataReceived` and `RealtimeViewStale` within about a minute and a half.
  - `curl http://localhost:8000/health` returns `degraded` with HTTP 503.
  - `curl http://localhost:8000/api/v1/alerts` shows the operational alert stored
    through Alertmanager.

Recover:
```bash
docker compose start meter-simulator
```
- "The alerts resolve by themselves. If I restart the speed layer, the four queries
  resume from their checkpoints with no loss."

## 6. Replay (45 s)

```bash
docker compose exec airflow airflow dags trigger grid_daily_billing --conf '{"sim_date": "2026-01-02"}'
```
- "Reprocessing a day is one command. The job is idempotent: it replaces that
  day's bills in one transaction. This is how a corrected tariff file would be applied."

## 7. Close (30 s)

- "Tests: 29 unit and Spark tests, including one that checks the Spark billing
  logic against a reference implementation."
  ```bash
  docker compose run --rm --no-deps --entrypoint "" speed-layer python -m pytest -q
  ```
- "Limitations are in the report: single broker, Spark local mode, JSON rather than
  Avro, and the serving store would need to change at production scale."

## If something goes wrong during a live demo

| Symptom | Fix |
|---|---|
| A port is already in use | Stop the other program, or change the left side of the port mapping in `docker-compose.yml` |
| Dashboards empty | Wait 30 s after start; check `docker compose ps` |
| No daily report yet | The first one appears about 7 minutes after start |
| Speed layer keeps restarting after a code change to an aggregation | The state schema changed; run `docker compose down -v` and start again |
| Want a clean slate | `docker compose down -v` then `docker compose up -d` |
