"""Send generated payment events to Kafka, playing the role of the payment gateway.

Each line of data/stream/events_*.jsonl becomes one Kafka message:
  key   = psp_transaction_id   (all events of one payment go to the same partition, in order)
  value = the JSON event, unchanged

Usage:
  python -m payrecon.streaming.producer                     all event files, 500 events/second
  python -m payrecon.streaming.producer --date 2026-10-19   one day only
  python -m payrecon.streaming.producer --rate 0            as fast as possible
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from payrecon.streaming.config import PAYMENT_EVENTS_TOPIC, bootstrap_servers


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers(),
            # acks=all + idempotence: a message is confirmed only when stored, and the
            # producer's own network retries can never write it twice.
            "acks": "all",
            "enable.idempotence": True,
            # Wait up to 20 ms to group messages into one request: far fewer network calls.
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


class DeliveryTracker:
    """Counts what Kafka confirmed. Delivery happens asynchronously, so results arrive via callback."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed: list[str] = []
        self.per_partition: Counter[int] = Counter()

    def __call__(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.failed.append(f"{message.key()!r}: {error}")
        else:
            self.delivered += 1
            self.per_partition[message.partition()] += 1


def send_file(producer: Producer, path: Path, topic: str, rate: float, tracker: DeliveryTracker) -> int:
    pause = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            key = json.loads(line)["psp_transaction_id"]
            while True:
                try:
                    producer.produce(topic, key=key.encode(), value=line.encode(), callback=tracker)
                    break
                except BufferError:
                    producer.poll(0.5)  # local queue full: let Kafka catch up, then retry
            producer.poll(0)  # serve delivery callbacks
            sent += 1
            if pause:
                time.sleep(pause)
    return sent


def event_files(data_dir: Path, day: date | None) -> list[Path]:
    stream_dir = data_dir / "stream"
    if day:
        return [stream_dir / f"events_{day:%Y%m%d}.jsonl"]
    return sorted(stream_dir.glob("events_*.jsonl"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send payment events to Kafka")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--rate", type=float, default=500, help="events per second, 0 = no limit")
    parser.add_argument("--topic", default=PAYMENT_EVENTS_TOPIC)
    args = parser.parse_args(argv)

    files = [p for p in event_files(args.data_dir, args.date) if p.exists()]
    if not files:
        print("No event files found. Run payrecon.generator.generate_events first.")
        return 1

    producer = make_producer()
    tracker = DeliveryTracker()
    started = time.monotonic()
    sent = 0
    for path in files:
        print(f"Sending {path.name} ...")
        sent += send_file(producer, path, args.topic, args.rate, tracker)
    remaining = producer.flush(30)  # wait for every outstanding delivery confirmation
    seconds = time.monotonic() - started

    print(f"\nSent {sent}, confirmed {tracker.delivered}, failed {len(tracker.failed)}, "
          f"unconfirmed {remaining} in {seconds:.1f}s")
    print("Messages per partition:", dict(sorted(tracker.per_partition.items())))
    for failure in tracker.failed[:5]:
        print("  failed:", failure)
    return 0 if not tracker.failed and remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
