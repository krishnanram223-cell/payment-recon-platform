"""Integration test against a real Kafka broker.

Skipped automatically when no broker is reachable (for example in CI without Docker).
Start one with: docker compose up -d
"""
import json
import uuid
from collections import defaultdict
from datetime import date

import pytest

confluent_kafka = pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, KafkaException  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from payrecon.generator.generate_events import EventConfig, generate_events  # noqa: E402
from payrecon.generator.generate_psp_files import GeneratorConfig, generate  # noqa: E402
from payrecon.streaming.config import bootstrap_servers  # noqa: E402
from payrecon.streaming.producer import DeliveryTracker, make_producer, send_file  # noqa: E402


@pytest.fixture(scope="module")
def admin():
    client = AdminClient({"bootstrap.servers": bootstrap_servers()})
    try:
        client.list_topics(timeout=3)
    except KafkaException:
        pytest.skip("no Kafka broker reachable; start one with: docker compose up -d")
    return client


@pytest.fixture
def topic(admin):
    name = f"test-payment-events-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result()
    yield name
    admin.delete_topics([name])[name].result()


@pytest.mark.kafka
def test_every_event_arrives_and_each_payment_stays_on_one_partition(admin, topic, tmp_path):
    generate(GeneratorConfig(start_date=date(2026, 10, 19), days=1, txns_per_file=60, seed=9, out_dir=tmp_path))
    events_file = generate_events(EventConfig(data_dir=tmp_path, seed=9))[0]
    expected = sum(1 for _ in events_file.open())

    producer = make_producer()
    tracker = DeliveryTracker()
    send_file(producer, events_file, topic, rate=0, tracker=tracker)
    assert producer.flush(30) == 0
    assert tracker.delivered == expected and not tracker.failed

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    partitions_per_key = defaultdict(set)
    received, idle = 0, 0
    try:
        while received < expected and idle < 15:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            assert message.error() is None
            event = json.loads(message.value())
            assert message.key().decode() == event["psp_transaction_id"]
            partitions_per_key[event["psp_transaction_id"]].add(message.partition())
            received += 1
    finally:
        consumer.close()

    assert received == expected
    assert all(len(p) == 1 for p in partitions_per_key.values()), "one key must always map to one partition"
