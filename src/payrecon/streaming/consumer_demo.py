"""A plain Kafka consumer, for learning offsets, consumer groups and delivery semantics.

It reads payment-events, commits its progress after processing, and reports what it saw:
messages per partition, duplicate deliveries (same event_id) and late events.
Real processing happens in Spark Structured Streaming (milestone 2.2); this tool
shows what Spark does for you under the hood.

Usage:
  python -m payrecon.streaming.consumer_demo                       group "demo"
  python -m payrecon.streaming.consumer_demo --group demo-2        a NEW group reads everything again
  python -m payrecon.streaming.consumer_demo --max-messages 100    stop early (rest stays unread)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError

from payrecon.streaming.config import PAYMENT_EVENTS_TOPIC, bootstrap_servers

LATE_AFTER = timedelta(hours=1)
COMMIT_EVERY = 200


def parse_ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read payment events and report what was seen")
    parser.add_argument("--group", default="demo")
    parser.add_argument("--topic", default=PAYMENT_EVENTS_TOPIC)
    parser.add_argument("--max-messages", type=int, default=0, help="0 = until the topic is idle")
    parser.add_argument("--idle-seconds", type=float, default=10)
    args = parser.parse_args(argv)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": args.group,
            # A group with no saved offset starts at the oldest message.
            "auto.offset.reset": "earliest",
            # We commit ourselves, AFTER processing: at-least-once delivery.
            "enable.auto.commit": False,
        }
    )

    def on_assign(_consumer, partitions):
        print("Assigned partitions:", [p.partition for p in partitions])

    consumer.subscribe([args.topic], on_assign=on_assign)

    per_partition: Counter[int] = Counter()
    offsets: dict[int, list[int]] = {}
    seen_event_ids: dict[str, int] = {}
    duplicates: list[str] = []
    duplicates_on_other_partition = 0
    late = 0
    parse_errors = 0
    processed = 0
    idle = 0.0

    try:
        while not args.max_messages or processed < args.max_messages:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1.0
                if idle >= args.idle_seconds:
                    break
                continue
            idle = 0.0
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            partition, offset = message.partition(), message.offset()
            per_partition[partition] += 1
            offsets.setdefault(partition, [offset, offset])[1] = offset
            try:
                event = json.loads(message.value())
            except (TypeError, ValueError):
                parse_errors += 1
            else:
                event_id = event["event_id"]
                if event_id in seen_event_ids:
                    duplicates.append(event["psp_transaction_id"])
                    if seen_event_ids[event_id] != partition:
                        duplicates_on_other_partition += 1
                else:
                    seen_event_ids[event_id] = partition
                if parse_ts(event["produced_at"]) - parse_ts(event["event_timestamp"]) > LATE_AFTER:
                    late += 1
            processed += 1
            if processed % COMMIT_EVERY == 0:
                consumer.commit(asynchronous=False)  # progress is saved only after processing
    finally:
        if processed:
            consumer.commit(asynchronous=False)
        consumer.close()  # leaves the group cleanly, so partitions are reassigned at once

    print(f"\nGroup '{args.group}' processed {processed} messages")
    for partition in sorted(per_partition):
        first, last = offsets[partition]
        print(f"  partition {partition}: {per_partition[partition]} messages, offsets {first}..{last}")
    print(f"Duplicate deliveries (same event_id): {len(duplicates)}"
          f"  (on a different partition: {duplicates_on_other_partition})")
    print(f"Late events (sent > 1 hour after they happened): {late}")
    print(f"Unparseable messages: {parse_errors}")
    if processed == 0:
        print("Nothing new: this group has already committed everything. Try --group <new-name>.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
