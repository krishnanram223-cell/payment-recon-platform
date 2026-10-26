"""Spark Structured Streaming against a real Kafka broker (skipped when none is reachable).

Start one with: docker compose up -d
"""
import json
import uuid
from datetime import date

import pytest

pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, KafkaException  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from payrecon.common.paths import Paths  # noqa: E402
from payrecon.generator.generate_events import (  # noqa: E402
    DUPLICATE_DELIVERY,
    LATE_EVENT,
    EventConfig,
    generate_events,
)
from payrecon.generator.generate_psp_files import GeneratorConfig, generate  # noqa: E402
from payrecon.streaming.config import bootstrap_servers  # noqa: E402
from payrecon.streaming.producer import DeliveryTracker, make_producer, send_file  # noqa: E402
from payrecon.streaming.stream_to_bronze import StreamConfig, run_available_now  # noqa: E402

BAD_MESSAGES = [b"this is not json", b'{"hello": "no event_id here"}']


@pytest.fixture(scope="module")
def admin():
    client = AdminClient({"bootstrap.servers": bootstrap_servers()})
    try:
        client.list_topics(timeout=3)
    except KafkaException:
        pytest.skip("no Kafka broker reachable; start one with: docker compose up -d")
    return client


@pytest.fixture
def topics(admin):
    suffix = uuid.uuid4().hex[:8]
    names = [f"test-events-{suffix}", f"test-events-dlq-{suffix}"]
    futures = admin.create_topics([NewTopic(n, num_partitions=3, replication_factor=1) for n in names])
    for future in futures.values():
        future.result()
    yield names
    for future in admin.delete_topics(names).values():
        future.result()


def produce(path, topic, extra=()):
    producer = make_producer()
    tracker = DeliveryTracker()
    sent = send_file(producer, path, topic, rate=0, tracker=tracker)
    for value in extra:
        producer.produce(topic, key=b"bad", value=value, callback=tracker)
    assert producer.flush(30) == 0 and not tracker.failed
    return sent + len(extra)


def count_messages(topic: str) -> int:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap_servers(), "group.id": f"count-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "earliest"}
    )
    consumer.subscribe([topic])
    count, idle = 0, 0
    try:
        while idle < 10:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error() is None:
                count += 1
                idle = 0
    finally:
        consumer.close()
    return count


@pytest.mark.kafka
def test_stream_lands_every_message_once_and_every_event_once(spark, admin, topics, tmp_path):
    topic, dlq = topics
    generate(GeneratorConfig(start_date=date(2026, 10, 19), days=1, txns_per_file=60, seed=13, out_dir=tmp_path))
    events_file = generate_events(EventConfig(data_dir=tmp_path, seed=13))[0]
    manifest = json.loads((tmp_path / "ground_truth" / "stream_expected_20261019.json").read_text())
    unique_events = sum(info["unique_events"] for info in manifest["psps"].values())
    resends = sum(len(info["anomalies"][DUPLICATE_DELIVERY]) for info in manifest["psps"].values())
    late = sum(len(info["anomalies"][LATE_EVENT]) for info in manifest["psps"].values())

    paths = Paths(tmp_path)
    cfg = StreamConfig(paths=paths, topic=topic, dlq_topic=dlq, max_offsets_per_trigger=50)

    sent = produce(events_file, topic, extra=BAD_MESSAGES)
    run_available_now(spark, cfg)

    bronze = spark.read.format("delta").load(paths.bronze("payment_events"))
    silver = spark.read.format("delta").load(paths.silver("payment_events"))
    audit = spark.read.format("delta").load(paths.audit("stream_batches"))

    assert bronze.count() == sent, "every Kafka message lands in Bronze exactly once"
    assert silver.count() == unique_events, "every event lands in Silver exactly once"
    assert silver.filter(F.col("is_late")).count() == late
    assert audit.agg(F.sum("duplicate_events")).first()[0] == resends
    assert audit.count() > 1, "maxOffsetsPerTrigger=50 must split the work into several micro-batches"
    assert count_messages(dlq) == len(BAD_MESSAGES)

    # Restart with nothing new: the checkpoint says everything was processed.
    run_available_now(spark, cfg)
    assert spark.read.format("delta").load(paths.bronze("payment_events")).count() == sent

    # The gateway re-sends the whole day: Bronze keeps the new messages, Silver adds nothing.
    resent = produce(events_file, topic)
    run_available_now(spark, cfg)
    assert spark.read.format("delta").load(paths.bronze("payment_events")).count() == sent + resent
    assert spark.read.format("delta").load(paths.silver("payment_events")).count() == unique_events
