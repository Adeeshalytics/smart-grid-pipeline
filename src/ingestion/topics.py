"""Create the Kafka topics explicitly (auto-creation is disabled on the broker)."""
from __future__ import annotations

import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from gridlib.config import get_settings
from gridlib.logging_utils import get_logger

log = get_logger("topic-init", "ingestion")


def main() -> int:
    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})

    # Wait for the broker.
    for _ in range(30):
        try:
            admin.list_topics(timeout=5)
            break
        except Exception:  # noqa: BLE001
            time.sleep(2)
    else:
        log.error("kafka_unreachable", bootstrap=settings.kafka_bootstrap)
        return 1

    wanted = [
        NewTopic(
            settings.topic_readings,
            num_partitions=settings.topic_partitions,
            replication_factor=1,
            config={"retention.ms": str(7 * 24 * 3600 * 1000), "cleanup.policy": "delete"},
        ),
        NewTopic(
            settings.topic_dlq,
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": str(30 * 24 * 3600 * 1000)},
        ),
        NewTopic(settings.topic_alerts, num_partitions=1, replication_factor=1),
    ]
    existing = set(admin.list_topics(timeout=10).topics)
    to_create = [t for t in wanted if t.topic not in existing]
    futures = admin.create_topics(to_create) if to_create else {}
    for topic, future in futures.items():
        try:
            future.result()
            log.info("topic_created", topic=topic)
        except Exception as exc:  # noqa: BLE001
            log.error("topic_create_failed", topic=topic, error=str(exc))
            return 1
    log.info("topics_ready", topics=[t.topic for t in wanted], already_existed=sorted(existing))
    return 0


if __name__ == "__main__":
    sys.exit(main())
