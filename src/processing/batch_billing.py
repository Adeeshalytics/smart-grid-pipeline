"""Batch layer: recompute one simulated day of billing from the raw master dataset.

    spark-submit batch_billing.py --sim-date 2026-01-03 --run-id <airflow run id>

Reads every raw reading for the day from the data lake, removes duplicates
exactly, totals each household, joins the day's tariff data and applies the
billing rules. The output replaces whatever was stored for that day, so the job
is idempotent: running it twice, or re-running it after a late tariff
correction, gives the same correct result.

The last line printed to stdout is a JSON summary, which Airflow stores in XCom.
"""
from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import functions as F

from gridlib import db
from gridlib.config import get_settings
from gridlib.lineage import record_run
from gridlib.logging_utils import get_logger
from processing import transforms as T
from processing.spark_session import build_spark, jdbc_options

log = get_logger("batch-billing", "processing")

BILL_COLUMNS = [
    "sim_date",
    "household_id",
    "grid_zone",
    "readings",
    "consumption_kwh",
    "solar_kwh",
    "net_kwh",
    "import_kwh",
    "export_kwh",
    "solar_contribution_pct",
    "tariff_rate",
    "billing_tier",
    "subsidy_flag",
    "tariff_source",
    "energy_charge",
    "subsidy_discount",
    "export_credit",
    "fixed_charge",
    "total_bill",
    "pipeline_run_id",
]

# Atomic swap: delete the day and insert the new rows in a single transaction.
SWAP_SQL = f"""
DELETE FROM daily_household_bill WHERE sim_date = %(d)s;
INSERT INTO daily_household_bill ({", ".join(BILL_COLUMNS)})
SELECT {", ".join(BILL_COLUMNS)} FROM stg_daily_household_bill WHERE sim_date = %(d)s;

DELETE FROM daily_zone_summary WHERE sim_date = %(d)s;
INSERT INTO daily_zone_summary (sim_date, grid_zone, households, consumption_kwh, solar_kwh,
                                renewable_share, import_kwh, export_kwh, total_billed,
                                pipeline_run_id)
SELECT sim_date, grid_zone, count(*), sum(consumption_kwh), sum(solar_kwh),
       sum(solar_kwh) / NULLIF(sum(consumption_kwh), 0), sum(import_kwh), sum(export_kwh),
       sum(total_bill), %(run_id)s
FROM daily_household_bill
WHERE sim_date = %(d)s
GROUP BY sim_date, grid_zone;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim-date", required=True, help="Simulated day to bill, YYYY-MM-DD")
    parser.add_argument("--run-id", required=True, help="Pipeline run id, stored as lineage")
    args = parser.parse_args()

    settings = get_settings()
    started = time.time()
    spark = build_spark(f"grid-batch-billing-{args.sim_date}", settings)
    spark.sparkContext.setLogLevel("WARN")
    log.info("batch_started", sim_date=args.sim_date, run_id=args.run_id, source=settings.lake_raw_path)

    # 1. Raw master dataset for the day (partition pruning on event_date).
    raw = spark.read.parquet(settings.lake_raw_path).where(
        F.col("event_date") == F.lit(args.sim_date).cast("date")
    )
    raw.cache()
    raw_count = raw.count()
    if raw_count == 0:
        raise SystemExit(f"No raw readings in the data lake for {args.sim_date}")

    # 2. Exact de-duplication, then household totals - the same functions the speed layer uses.
    unique = T.deduplicate(raw)
    unique_count = unique.count()
    usage = T.household_daily_totals(unique).where(
        F.col("sim_date") == F.lit(args.sim_date).cast("date")
    )

    # 3. Join with the batch source: the tariff data loaded for this day.
    tariffs = T.tariffs_as_of(
        spark.read.format("jdbc").options(**jdbc_options(settings), dbtable="tariffs").load(),
        args.sim_date,
    )
    billed = (
        T.with_billing(T.attach_tariff(usage, tariffs, settings.default_tariff_rate))
        .withColumn("pipeline_run_id", F.lit(args.run_id))
        .select(*BILL_COLUMNS)
        .cache()
    )

    # 4a. Serving store: bulk-load a staging table, then swap atomically.
    (
        billed.write.format("jdbc")
        .options(**jdbc_options(settings), dbtable="stg_daily_household_bill", truncate="true")
        .mode("overwrite")
        .save()
    )
    conn = db.connect(settings)
    with conn.cursor() as cur:
        cur.execute(SWAP_SQL, {"d": args.sim_date, "run_id": args.run_id})
    conn.commit()

    # 4b. Curated zone of the data lake: the same result as Parquet, for analytics.
    curated = f"{settings.lake_curated_path}/sim_date={args.sim_date}"
    billed.drop("sim_date").coalesce(1).write.mode("overwrite").parquet(curated)

    totals = billed.agg(
        F.count("*").alias("households"),
        F.sum("consumption_kwh").alias("consumption_kwh"),
        F.sum("solar_kwh").alias("solar_kwh"),
        F.sum("total_bill").alias("total_billed"),
        F.sum(F.when(F.col("tariff_source") != "same_day", 1).otherwise(0)).alias("tariff_fallbacks"),
    ).first()
    summary = {
        "sim_date": args.sim_date,
        "run_id": args.run_id,
        "raw_readings": raw_count,
        "duplicates_removed": raw_count - unique_count,
        "households_billed": totals["households"],
        "consumption_kwh": round(totals["consumption_kwh"], 3),
        "solar_kwh": round(totals["solar_kwh"], 3),
        "total_billed": round(totals["total_billed"], 2),
        "tariff_fallbacks": totals["tariff_fallbacks"],
        "curated_path": curated,
        "duration_s": round(time.time() - started, 1),
    }
    record_run(
        conn,
        args.run_id,
        "compute_daily_billing",
        args.sim_date,
        "success",
        int((time.time() - started) * 1000),
        summary,
    )
    conn.close()
    log.info("batch_finished", **summary)
    spark.stop()
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
