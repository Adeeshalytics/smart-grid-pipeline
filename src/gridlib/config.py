"""Central configuration. Every tunable is an environment variable with a default."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


GRID_ZONES = ("ZONE-NORTH", "ZONE-SOUTH", "ZONE-EAST", "ZONE-WEST", "ZONE-CENTRAL")

SOLAR_PENETRATION = {
    "ZONE-NORTH": 0.45,
    "ZONE-SOUTH": 0.40,
    "ZONE-EAST": 0.30,
    "ZONE-WEST": 0.25,
    "ZONE-CENTRAL": 0.20,
}

BILLING_TIERS = ("TIER_1", "TIER_2", "TIER_3")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    topic_readings: str = field(default_factory=lambda: _env("TOPIC_READINGS", "meter.readings"))
    topic_dlq: str = field(default_factory=lambda: _env("TOPIC_DLQ", "meter.readings.dlq"))
    topic_alerts: str = field(default_factory=lambda: _env("TOPIC_ALERTS", "grid.alerts"))
    topic_partitions: int = field(default_factory=lambda: _env_int("TOPIC_PARTITIONS", 6))

    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "postgres"))
    pg_port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    pg_db: str = field(default_factory=lambda: _env("POSTGRES_DB", "grid"))
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "grid"))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "grid"))

    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://objectstore:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "gridadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "gridadmin123"))
    bucket_lake: str = field(default_factory=lambda: _env("BUCKET_LAKE", "grid-lake"))
    bucket_landing: str = field(default_factory=lambda: _env("BUCKET_LANDING", "grid-landing"))
    bucket_reports: str = field(default_factory=lambda: _env("BUCKET_REPORTS", "grid-reports"))

    sim_day_seconds: int = field(default_factory=lambda: _env_int("SIM_DAY_SECONDS", 300))
    sim_epoch: str = field(default_factory=lambda: _env("SIM_EPOCH", "2026-01-01T00:00:00+00:00"))

    num_households: int = field(default_factory=lambda: _env_int("NUM_HOUSEHOLDS", 120))
    reading_interval_sim_minutes: int = field(
        default_factory=lambda: _env_int("READING_INTERVAL_SIM_MINUTES", 15)
    )
    p_corrupt: float = field(default_factory=lambda: _env_float("P_CORRUPT", 0.005))
    p_duplicate: float = field(default_factory=lambda: _env_float("P_DUPLICATE", 0.01))
    p_late: float = field(default_factory=lambda: _env_float("P_LATE", 0.02))
    p_very_late: float = field(default_factory=lambda: _env_float("P_VERY_LATE", 0.005))
    p_overcast: float = field(default_factory=lambda: _env_float("P_OVERCAST", 0.25))
    random_seed: int = field(default_factory=lambda: _env_int("RANDOM_SEED", 8203))
    tariff_drop_delay_seconds: int = field(
        default_factory=lambda: _env_int("TARIFF_DROP_DELAY_SECONDS", 10)
    )

    watermark: str = field(default_factory=lambda: _env("WATERMARK", "30 minutes"))
    zone_window: str = field(default_factory=lambda: _env("ZONE_WINDOW", "15 minutes"))
    low_renewable_threshold: float = field(
        default_factory=lambda: _env_float("LOW_RENEWABLE_THRESHOLD", 0.20)
    )
    daylight_start_hour: int = field(default_factory=lambda: _env_int("DAYLIGHT_START_HOUR", 10))
    daylight_end_hour: int = field(default_factory=lambda: _env_int("DAYLIGHT_END_HOUR", 15))
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", "/checkpoints"))

    default_tariff_rate: float = field(default_factory=lambda: _env_float("DEFAULT_TARIFF_RATE", 30.0))

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def pg_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def lake_raw_path(self) -> str:
        return f"s3a://{self.bucket_lake}/raw/meter_readings"

    @property
    def lake_curated_path(self) -> str:
        return f"s3a://{self.bucket_lake}/curated/daily_household_bill"


def get_settings() -> Settings:
    return Settings()
