"""SparkSession factory used by both the streaming and the batch job."""
from __future__ import annotations

from pyspark.sql import SparkSession

from gridlib.config import Settings


def build_spark(app_name: str, settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def jdbc_options(settings: Settings) -> dict[str, str]:
    return {
        "url": settings.pg_jdbc_url,
        "user": settings.pg_user,
        "password": settings.pg_password,
        "driver": "org.postgresql.Driver",
    }
