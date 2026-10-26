"""Kafka settings shared by the admin tool, producer and consumers."""
from __future__ import annotations

import os
from dataclasses import dataclass

PAYMENT_EVENTS_TOPIC = "payment-events"
PAYMENT_EVENTS_DLQ_TOPIC = "payment-events-dlq"  # dead-letter queue: messages that cannot be parsed


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_hours: int


TOPICS = (
    # 3 partitions: up to 3 consumers in one group can share the work.
    # Same key (psp_transaction_id) -> same partition -> events of one payment stay in order.
    TopicSpec(PAYMENT_EVENTS_TOPIC, partitions=3, retention_hours=24 * 7),
    TopicSpec(PAYMENT_EVENTS_DLQ_TOPIC, partitions=1, retention_hours=24 * 30),
)


def bootstrap_servers() -> str:
    """Where Kafka is. Configurable so the same code runs against a cloud cluster later."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
