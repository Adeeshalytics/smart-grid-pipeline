"""Object-store helpers (SeaweedFS locally, any S3-compatible store in production)."""
from __future__ import annotations

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import Settings


def client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        region_name="us-east-1",
    )


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def tariff_key(sim_date: str) -> str:
    """Landing-zone key of the tariff file for one simulated day."""
    return f"tariffs/dt={sim_date}/tariffs.csv"
