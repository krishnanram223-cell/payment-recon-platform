"""Create and describe the project's Kafka topics.

Usage:
  python -m payrecon.streaming.admin create     create missing topics (safe to run again)
  python -m payrecon.streaming.admin describe   partitions and message counts per topic
  python -m payrecon.streaming.admin delete     delete the project's topics (asks first)
"""
from __future__ import annotations

import argparse
import sys

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from payrecon.streaming.config import TOPICS, bootstrap_servers


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers()})


def existing_topics(admin: AdminClient) -> set[str]:
    return set(admin.list_topics(timeout=10).topics)


def create_topics(admin: AdminClient) -> None:
    have = existing_topics(admin)
    new = [
        NewTopic(
            spec.name,
            num_partitions=spec.partitions,
            replication_factor=1,  # one broker locally; production uses 3
            config={"retention.ms": str(spec.retention_hours * 3600 * 1000)},
        )
        for spec in TOPICS
        if spec.name not in have
    ]
    if not new:
        print("All topics already exist.")
        return
    for name, future in admin.create_topics(new).items():
        future.result()  # raises if creation failed
        print(f"Created topic {name}")


def describe_topics(admin: AdminClient) -> None:
    metadata = admin.list_topics(timeout=10)
    # A consumer that never subscribes can still ask for each partition's offset range.
    probe = Consumer({"bootstrap.servers": bootstrap_servers(), "group.id": "payrecon-admin-probe"})
    try:
        for spec in TOPICS:
            topic = metadata.topics.get(spec.name)
            if topic is None:
                print(f"{spec.name}: does not exist (run: python -m payrecon.streaming.admin create)")
                continue
            print(f"\n{spec.name}: {len(topic.partitions)} partitions")
            total = 0
            for pid in sorted(topic.partitions):
                low, high = probe.get_watermark_offsets(TopicPartition(spec.name, pid), timeout=10)
                total += high - low
                print(f"  partition {pid}: offsets {low}..{high - 1 if high else 0}  messages={high - low}")
            print(f"  total messages: {total}")
    finally:
        probe.close()


def delete_topics(admin: AdminClient) -> None:
    names = [spec.name for spec in TOPICS if spec.name in existing_topics(admin)]
    if not names:
        print("Nothing to delete.")
        return
    if input(f"Delete {', '.join(names)} and ALL their messages? Type yes: ").strip() != "yes":
        print("Cancelled.")
        return
    for name, future in admin.delete_topics(names).items():
        future.result()
        print(f"Deleted topic {name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage the project's Kafka topics")
    parser.add_argument("action", choices=["create", "describe", "delete"])
    args = parser.parse_args(argv)
    admin = admin_client()
    {"create": create_topics, "describe": describe_topics, "delete": delete_topics}[args.action](admin)
    return 0


if __name__ == "__main__":
    sys.exit(main())
