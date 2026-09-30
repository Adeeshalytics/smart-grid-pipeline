# Smart Grid Energy Monitoring & Billing

EC8203 Applied Big Data Engineering mini-project, Use Case 3.

An end-to-end **Lambda architecture** data platform. Simulated smart meters stream
readings into Kafka; a speed layer (Spark Structured Streaming) gives the control
room a live view of grid load and solar contribution by zone; a batch layer
(Airflow + Spark) recomputes each day exactly from the raw data, joins it with the
tariff file delivered once a day, and publishes a per-household billing and
solar-contribution report.

**Business question.** What is the current grid load and renewable contribution
by zone, and what will each household's bill look like once the daily tariff data
is applied to their consumption?

## Architecture

```
  SOURCES              INGESTION            PROCESSING                       STORAGE / SERVING

 meter-simulator ──▶  Kafka                SPEED LAYER                      PostgreSQL (serving store)
 (stream, every       meter.readings ──▶   Spark Structured Streaming        rt_zone_load
  ~3 s per meter)     6 partitions         ├─ validate ─▶ DLQ topic          rt_household_today   ──▶  FastAPI
                      key = meter_id       ├─ 15-min windows by zone ──────▶ grid_alerts               /api/v1/grid/realtime
                                           ├─ household running totals ───▶                            /api/v1/billing/{day}
                                           │   (joined with latest tariff)                             /api/v1/households/{id}/bill-to-date
                                           └─ archive raw events ──┐
                                                                   ▼                              ──▶  Grafana dashboards
                                           Data lake        raw/meter_readings (Parquet)
                                                                   │  immutable master dataset
 tariff-dropper ───▶  Landing zone         BATCH LAYER             ▼
 (one CSV per         tariffs/dt=.../      Airflow DAG, one run per simulated day
  simulated day)      + manifest           wait file ▶ validate+load tariffs ▶ Spark batch
                                           (dedup, totals, join tariffs, bill) ▶ reconcile
                                           ▶ quality gate ▶ publish report ─▶ daily_household_bill
                                                                              daily_zone_summary
                                                                              HTML + CSV report in the object store

 OBSERVABILITY  JSON logs at every stage · Prometheus metrics from every stage · 15 alert rules
                · Alertmanager ▶ API ▶ ops_alerts · pipeline_runs audit trail · Grafana "Pipeline Health"
```

Why Lambda and not Kappa, and why each tool was chosen, is argued in the report
(`docs/report.pdf`). In one sentence: grid monitoring needs answers in seconds and
can tolerate small errors, while billing needs exact, auditable, re-computable
numbers and must wait for a file that only arrives once a day.

## Simulated clock

**One simulated day = 5 minutes of wall-clock time** (a 288x speed-up).

| Simulated | Wall-clock |
|---|---|
| 15-minute meter reading interval | ~3.1 s |
| 30-minute watermark | ~6.3 s |
| 1 day (one Airflow run, one tariff file) | 5 min |

The mapping is stored once in PostgreSQL (`sim_clock`), so every component agrees
on simulated time. Change it with `SIM_DAY_SECONDS` in `.env`.

## Run it

Requirements: Docker Desktop with about 6 GB of memory available to Docker.

```bash
docker compose up -d --build
```

The first build downloads Spark and Airflow and takes several minutes. After the
containers start, real-time data appears within about 30 seconds and the first
daily report within about 7 minutes (one simulated day plus the batch run).

| What | Where | Login |
|---|---|---|
| Real-time API (Swagger UI) | http://localhost:8000/docs | none |
| Grafana dashboards | http://localhost:3000 | none needed (admin / admin to edit) |
| Airflow | http://localhost:8080 | admin / admin |
| Prometheus alerts | http://localhost:9090/alerts | none |
| Alertmanager | http://localhost:9093 | none |
| Data lake file browser (SeaweedFS) | http://localhost:8888/buckets/ | none |
| S3 API of the object store | http://localhost:8333 | gridadmin / gridadmin123 |
| PostgreSQL | localhost:5433, database `grid` | grid / grid |

These are local development credentials; override them in `.env` (see `.env.example`).

## Reproduce the results

```bash
# 1. Real-time view (speed layer)
curl http://localhost:8000/api/v1/grid/realtime

# 2. Daily consolidated report (batch layer); replace the date with a completed simulated day
curl http://localhost:8000/api/v1/billing/2026-01-01
#    or open the HTML report: http://localhost:8000/api/v1/reports/latest
#    or look in ./reports/

# 3. Merged view: exact bills for closed days + live estimate for today
curl http://localhost:8000/api/v1/households/HH-0001/bill-to-date

# 4. Speed layer vs batch layer for a day
curl http://localhost:8000/api/v1/reconciliation/2026-01-01

# 5. Alerts (business and operational)
curl http://localhost:8000/api/v1/alerts

# 6. Rejected events in the dead-letter topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic meter.readings.dlq --from-beginning --max-messages 5

# 7. Reprocess a day from the raw data (idempotent)
docker compose exec airflow airflow dags trigger grid_daily_billing --conf '{"sim_date": "2026-01-01"}'
```

### Demonstrate the alerts

```bash
docker compose stop meter-simulator     # "no data received" fires after about a minute
docker compose start meter-simulator    # it resolves
docker compose restart speed-layer      # the queries resume from their checkpoints
```

Low-renewable alerts need no action: about one zone-day in four is simulated as
overcast, which pushes the zone's solar contribution under the 20% threshold.

## Tests

```bash
docker compose run --rm --no-deps --entrypoint "" speed-layer python -m pytest -q
```

The tests cover the billing rules, the event contract, the tariff file validation,
the simulated clock, both simulators, and the Spark transformations (including a
check that the Spark billing expressions agree with the reference implementation).

## Repository layout

```
docker-compose.yml          the whole platform
docker/                     Dockerfiles (python, spark, airflow)
sql/init/                   serving-store schema
src/gridlib/                shared library: config, logging, clock, contracts, billing rules
src/simulators/             the two simulated data sources
src/ingestion/              Kafka topic and bucket creation
src/processing/transforms.py   transformations shared by both layers
src/processing/speed_layer.py  Spark Structured Streaming job
src/processing/batch_billing.py Spark batch job
src/batch/                  task logic and report generation for the DAG
airflow/dags/               the daily DAG
src/serving/api.py          FastAPI serving layer
observability/              Prometheus rules, Alertmanager, Grafana dashboards
tests/                      unit and Spark tests
docs/                       report, demo script, viva guide
```

## Configuration

All settings are environment variables with defaults, defined in one place:
`src/gridlib/config.py`. Nothing is hard-coded in the jobs.

## Assumptions and simplifications

- Time is compressed 288x; all "daily" behaviour happens every 5 minutes.
- Spark runs in local mode (one container for streaming, and inside the Airflow
  worker for batch). The jobs are unchanged on a cluster; only `--master` differs.
- Kafka runs as a single broker with replication factor 1.
- Events are JSON validated in the speed layer. A schema registry with Avro is the
  production choice.
- SeaweedFS (an S3-compatible object store) stands in for S3/HDFS; PostgreSQL stands
  in for a scalable serving store.
- The billing rules (tiers, subsidy, feed-in credit, fixed charge) are our own
  reasonable interpretation; the brief only specifies the tariff file fields.

## Stop

```bash
docker compose down        # keep the data
docker compose down -v     # delete everything, including the simulated clock
```
