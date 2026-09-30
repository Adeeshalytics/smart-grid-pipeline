"""Create the object-store buckets. Run once at start-up by the `init` service. Idempotent."""
from __future__ import annotations

import sys
import time

from botocore.exceptions import ClientError, EndpointConnectionError

from gridlib import s3
from gridlib.config import get_settings
from gridlib.logging_utils import get_logger

log = get_logger("bucket-init", "storage")


def main() -> int:
    settings = get_settings()
    s3c = s3.client(settings)
    buckets = [settings.bucket_lake, settings.bucket_landing, settings.bucket_reports]
    for _ in range(30):
        try:
            existing = {b["Name"] for b in s3c.list_buckets()["Buckets"]}
            break
        except (EndpointConnectionError, ClientError):
            time.sleep(2)
    else:
        log.error("object_store_unreachable", endpoint=settings.s3_endpoint)
        return 1
    for bucket in buckets:
        if bucket not in existing:
            s3c.create_bucket(Bucket=bucket)
            log.info("bucket_created", bucket=bucket)
    log.info("buckets_ready", buckets=buckets)
    return 0


if __name__ == "__main__":
    sys.exit(main())
