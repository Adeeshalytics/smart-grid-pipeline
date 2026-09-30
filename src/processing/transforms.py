"""Transformations shared by the speed layer and the batch layer.

The classic weakness of a Lambda architecture is that the same business logic
has to be written twice, once per layer, and the two copies drift apart. Here
every transformation is a function of a DataFrame, and a DataFrame can be
either streaming or static. Both `speed_layer.py` and `batch_billing.py` call
these same functions, so validation, aggregation and billing rules exist once.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from gridlib import billing, schemas
from gridlib.config import GRID_ZONES

READING_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("meter_id", StringType()),
        StructField("household_id", StringType()),
        StructField("grid_zone", StringType()),
        StructField("power_consumption_kwh", DoubleType()),
        StructField("solar_generation_kwh", DoubleType()),
        StructField("timestamp", StringType()),
        StructField("emitted_at", StringType()),
        StructField("schema_version", IntegerType()),
    ]
)


# --------------------------------------------------------------------- ingestion
def parse_kafka(kafka_df: DataFrame) -> DataFrame:
    """Decode the Kafka value as JSON, keeping the raw payload and its coordinates."""
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("j", F.from_json("raw_value", READING_SCHEMA))
        .select("*", "j.*")
        .drop("j")
    )


def with_reject_reason(df: DataFrame) -> DataFrame:
    """Add `event_ts` and `reject_reason` (NULL means the reading is valid).

    Mirrors gridlib.schemas.validate_reading; the checks run in the same order.
    """
    required = [F.col(c) for c in schemas.REQUIRED_FIELDS]
    all_null = reduce(Column.__and__, [c.isNull() for c in required])
    any_null = reduce(Column.__or__, [c.isNull() for c in required])
    in_range = lambda c: F.col(c).between(0, schemas.MAX_KWH_PER_READING)  # noqa: E731

    return df.withColumn("event_ts", F.to_timestamp("timestamp")).withColumn(
        "reject_reason",
        F.when(all_null, schemas.REJECT_MALFORMED_JSON)
        .when(any_null, schemas.REJECT_MISSING_FIELD)
        .when(F.col("event_ts").isNull(), schemas.REJECT_BAD_TIMESTAMP)
        .when(~F.col("grid_zone").isin(*GRID_ZONES), schemas.REJECT_UNKNOWN_ZONE)
        .when(
            ~(in_range("power_consumption_kwh") & in_range("solar_generation_kwh")),
            schemas.REJECT_OUT_OF_RANGE,
        ),
    )


def valid_readings(df: DataFrame) -> DataFrame:
    """Keep valid rows and project them onto the clean column set."""
    return df.where(F.col("reject_reason").isNull()).select(
        "event_id",
        "meter_id",
        "household_id",
        "grid_zone",
        F.col("power_consumption_kwh").alias("consumption_kwh"),
        F.col("solar_generation_kwh").alias("solar_kwh"),
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        "kafka_partition",
        "kafka_offset",
        F.current_timestamp().alias("ingested_at"),
    )


def rejected_readings(df: DataFrame) -> DataFrame:
    return df.where(F.col("reject_reason").isNotNull()).select(
        "kafka_key", "raw_value", "reject_reason", "kafka_partition", "kafka_offset", "kafka_ts"
    )


# -------------------------------------------------------------------- processing
def deduplicate(df: DataFrame, watermark: str | None = None) -> DataFrame:
    """Remove redelivered events by event_id.

    Streaming: state is bounded by the watermark, so memory does not grow forever.
    Batch: a plain global de-duplication, which is exact.
    """
    if df.isStreaming:
        return df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(["event_id"])
    return df.dropDuplicates(["event_id"])


def zone_window_load(df: DataFrame, window: str) -> DataFrame:
    """Grid load and renewable mix per zone per tumbling window."""
    return (
        df.groupBy(F.window("event_ts", window), "grid_zone")
        .agg(
            F.sum("consumption_kwh").alias("consumption_kwh"),
            F.sum("solar_kwh").alias("solar_kwh"),
            F.approx_count_distinct("meter_id", 0.01).alias("active_meters"),
            F.count("*").alias("readings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "grid_zone",
            "consumption_kwh",
            "solar_kwh",
            (F.col("consumption_kwh") - F.col("solar_kwh")).alias("net_load_kwh"),
            renewable_share("solar_kwh", "consumption_kwh").alias("renewable_share"),
            "active_meters",
            "readings",
        )
    )


def household_daily_totals(df: DataFrame) -> DataFrame:
    """Consumption and solar generation per household per (simulated) day."""
    return (
        df.groupBy(F.window("event_ts", "1 day"), "household_id", "grid_zone")
        .agg(
            F.sum("consumption_kwh").alias("consumption_kwh"),
            F.sum("solar_kwh").alias("solar_kwh"),
            F.count("*").alias("readings"),
        )
        .select(
            F.to_date(F.col("window.start")).alias("sim_date"),
            "household_id",
            "grid_zone",
            "consumption_kwh",
            "solar_kwh",
            "readings",
        )
    )


def renewable_share(solar_col: str, consumption_col: str) -> Column:
    """Solar generation as a fraction of consumption (can exceed 1 when exporting)."""
    return F.when(F.col(consumption_col) > 0, F.col(solar_col) / F.col(consumption_col)).otherwise(
        F.lit(0.0)
    )


# ----------------------------------------------------- joining the two sources
def tariffs_as_of(tariffs: DataFrame, sim_date: str) -> DataFrame:
    """Pick, per household, the most recent tariff on or before `sim_date`.

    If a household is missing from that day's file, yesterday's tariff is
    carried forward and the row says so in `tariff_source`.
    """
    latest = Window.partitionBy("household_id").orderBy(F.col("dt").desc())
    return (
        tariffs.where(F.col("dt") <= F.lit(sim_date).cast("date"))
        .withColumn("rn", F.row_number().over(latest))
        .where("rn = 1")
        .select(
            "household_id",
            "tariff_rate",
            "billing_tier",
            "subsidy_flag",
            F.when(F.col("dt") == F.lit(sim_date).cast("date"), "same_day")
            .otherwise("carried_forward")
            .alias("tariff_source"),
        )
    )


def attach_tariff(usage: DataFrame, tariffs: DataFrame, default_rate: float) -> DataFrame:
    """Left-join usage to tariffs; households with no tariff at all get the default."""
    return (
        usage.join(F.broadcast(tariffs), "household_id", "left")
        .withColumn("tariff_source", F.coalesce("tariff_source", F.lit("default")))
        .withColumn("tariff_rate", F.coalesce("tariff_rate", F.lit(default_rate)))
        .withColumn("billing_tier", F.coalesce("billing_tier", F.lit("TIER_1")))
        .withColumn("subsidy_flag", F.coalesce("subsidy_flag", F.lit(False)))
    )


def with_billing(df: DataFrame) -> DataFrame:
    """Apply the billing rules of gridlib.billing as column expressions."""
    multiplier = F.lit(None).cast("double")
    for tier, value in billing.TIER_MULTIPLIER.items():
        multiplier = F.when(F.col("billing_tier") == tier, F.lit(value)).otherwise(multiplier)

    net = F.col("consumption_kwh") - F.col("solar_kwh")
    return (
        df.withColumn("net_kwh", net)
        .withColumn("import_kwh", F.greatest(net, F.lit(0.0)))
        .withColumn("export_kwh", F.greatest(-net, F.lit(0.0)))
        .withColumn("energy_charge", F.col("import_kwh") * F.col("tariff_rate") * multiplier)
        .withColumn(
            "subsidy_discount",
            F.when(F.col("subsidy_flag"), F.col("energy_charge") * billing.SUBSIDY_DISCOUNT).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "export_credit", F.col("export_kwh") * F.col("tariff_rate") * billing.FEED_IN_FACTOR
        )
        .withColumn("fixed_charge", F.lit(billing.FIXED_DAILY_CHARGE))
        .withColumn(
            "total_bill",
            F.col("energy_charge")
            - F.col("subsidy_discount")
            - F.col("export_credit")
            + F.col("fixed_charge"),
        )
        .withColumn(
            "solar_contribution_pct",
            F.round(renewable_share("solar_kwh", "consumption_kwh") * 100, 2),
        )
        .withColumn("net_kwh", F.round("net_kwh", 4))
        .withColumn("import_kwh", F.round("import_kwh", 4))
        .withColumn("export_kwh", F.round("export_kwh", 4))
        .withColumn("energy_charge", F.round("energy_charge", 2))
        .withColumn("subsidy_discount", F.round("subsidy_discount", 2))
        .withColumn("export_credit", F.round("export_credit", 2))
        .withColumn("total_bill", F.round("total_bill", 2))
    )
