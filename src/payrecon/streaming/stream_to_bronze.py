"""Spark Structured Streaming: Kafka payment events -> Bronze and Silver Delta tables.

Every micro-batch (a small slice of new Kafka messages) goes through process_batch():

  1. Bronze  bronze/payment_events   every Kafka message exactly once, raw text + Kafka coordinates
                                     (MERGE on topic/partition/offset: a replayed batch adds nothing)
  2. Parse + validate the JSON against the event contract
  3. Silver  silver/payment_events   valid events, typed, ONE row per event_id
                                     (MERGE on event_id: duplicate deliveries are dropped for good,
                                      even when the duplicate arrives days later)
  4. DLQ     Kafka topic payment-events-dlq   invalid messages with the reason, for investigation
  5. Audit   audit/stream_batches    one row per micro-batch with its counts

Spark remembers which Kafka offsets each batch covered in the CHECKPOINT folder.
After a crash or a stop, the next start continues exactly where the last one ended.

Usage:
  python -m payrecon.streaming.stream_to_bronze --available-now   process everything new, then stop
  python -m payrecon.streaming.stream_to_bronze                   run continuously (Ctrl+C to stop)
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from payrecon.common.delta_utils import merge_insert
from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.schemas.contracts import (
    EVENT_SCHEMA_VERSION,
    PAYMENT_EVENT_FIELDS,
    VALID_CURRENCIES,
    VALID_EVENT_TYPES,
    VALID_TRANSACTION_TYPES,
    field_names,
    mandatory_field_names,
)
from payrecon.streaming.config import PAYMENT_EVENTS_DLQ_TOPIC, PAYMENT_EVENTS_TOPIC, bootstrap_servers

log = logging.getLogger("payrecon.stream")

BRONZE_TABLE = "payment_events"
SILVER_TABLE = "payment_events"
AUDIT_TABLE = "stream_batches"
KAFKA_KEY = ["_kafka_topic", "_kafka_partition", "_kafka_offset"]

EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
PRODUCED_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
AMOUNT_PATTERN = r"^[0-9]+(\.[0-9]{1,2})?$"
LATE_AFTER_SECONDS = 3600

# Parse every field as text first (Bronze principle: keep what was sent, judge it afterwards).
EVENT_SCHEMA = StructType([StructField(name, StringType(), True) for name in field_names(PAYMENT_EVENT_FIELDS)])


@dataclass(frozen=True)
class StreamConfig:
    paths: Paths
    topic: str = PAYMENT_EVENTS_TOPIC
    dlq_topic: str = PAYMENT_EVENTS_DLQ_TOPIC
    bootstrap: str = ""
    max_offsets_per_trigger: int = 2000
    checkpoint_name: str = "payment_events"

    @property
    def checkpoint(self) -> str:
        return str(self.paths.lakehouse / "_checkpoints" / self.checkpoint_name)


# --------------------------------------------------------------------------- reading
def read_kafka(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    """An unbounded DataFrame: new rows appear as messages arrive in Kafka."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap or bootstrap_servers())
        .option("subscribe", cfg.topic)
        .option("startingOffsets", "earliest")  # only used on the very first run, before a checkpoint exists
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)  # at most this many messages per batch
        .load()
        .select(
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("value").cast("string").alias("value"),
        )
    )


# --------------------------------------------------------------------------- validation
def any_missing(columns: list[str]) -> Column:
    result = None
    for c in columns:
        check = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        result = check if result is None else result | check
    return result


def parse_and_validate(messages: DataFrame) -> DataFrame:
    """Parse the JSON, add typed helper columns and _reject_reason (NULL = valid)."""
    e = messages.withColumn("e", F.from_json("value", EVENT_SCHEMA)).select("*", "e.*").drop("e")
    e = (
        e.withColumn("_t_event_ts", F.try_to_timestamp(F.col("event_timestamp"), F.lit(EVENT_TS_FORMAT)))
        .withColumn("_t_produced_ts", F.try_to_timestamp(F.col("produced_at"), F.lit(PRODUCED_TS_FORMAT)))
        .withColumn("_t_amount", F.expr("try_cast(amount AS DECIMAL(18,2))"))
        .withColumn("_t_schema_version", F.expr("try_cast(schema_version AS INT)"))
    )
    reason = (
        # Not JSON at all, or JSON without an event_id: nothing to identify it by.
        F.when(F.col("event_id").isNull(), F.lit("INVALID_MESSAGE"))
        .when(
            F.col("_t_schema_version").isNull() | (F.col("_t_schema_version") != EVENT_SCHEMA_VERSION),
            F.lit("UNSUPPORTED_SCHEMA_VERSION"),
        )
        .when(any_missing(mandatory_field_names(PAYMENT_EVENT_FIELDS)), F.lit("MISSING_MANDATORY_FIELD"))
        .when(~F.col("event_type").isin(*sorted(VALID_EVENT_TYPES)), F.lit("INVALID_EVENT_TYPE"))
        .when(~F.col("transaction_type").isin(*sorted(VALID_TRANSACTION_TYPES)), F.lit("INVALID_TRANSACTION_TYPE"))
        .when(
            ~F.col("amount").rlike(AMOUNT_PATTERN) | F.col("_t_amount").isNull() | (F.col("_t_amount") <= 0),
            F.lit("INVALID_AMOUNT"),
        )
        .when(~F.col("currency").isin(*sorted(VALID_CURRENCIES)), F.lit("INVALID_CURRENCY"))
        .when(F.col("_t_event_ts").isNull() | F.col("_t_produced_ts").isNull(), F.lit("INVALID_TIMESTAMP"))
    )
    return e.withColumn("_reject_reason", reason)


def to_silver_events(valid: DataFrame, batch_id: int) -> DataFrame:
    lateness = F.col("_t_produced_ts").cast("long") - F.col("_t_event_ts").cast("long")
    return valid.select(
        "event_id",
        "event_type",
        F.col("_t_event_ts").alias("event_ts"),
        F.col("_t_produced_ts").alias("produced_ts"),
        lateness.alias("lateness_seconds"),
        (lateness > LATE_AFTER_SECONDS).alias("is_late"),
        "psp_code",
        "psp_transaction_id",
        "merchant_id",
        "terminal_id",
        "transaction_type",
        F.col("_t_amount").alias("amount"),
        "currency",
        "card_scheme",
        "masked_pan",
        "entry_mode",
        "response_code",
        (F.col("response_code") == "00").alias("is_approved"),
        F.col("_t_schema_version").alias("schema_version"),
        F.to_date("_t_event_ts").alias("event_date"),
        F.lit("STREAM").alias("_source_channel"),
        "_kafka_partition",
        "_kafka_offset",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingest_ts"),
    )


# --------------------------------------------------------------------------- one micro-batch
def process_batch(batch: DataFrame, batch_id: int, cfg: StreamConfig) -> None:
    spark = batch.sparkSession
    batch = batch.persist()  # used several times below; read the Kafka slice only once
    try:
        messages = batch.count()
        if messages == 0:
            return

        # 1. Bronze: every message, raw, exactly once (Kafka coordinates are unique).
        bronze = batch.withColumn("_source_channel", F.lit("STREAM")).withColumn(
            "_batch_id", F.lit(batch_id)
        ).withColumn("_ingest_ts", F.current_timestamp())
        bronze_inserted = merge_insert(bronze, cfg.paths.bronze(BRONZE_TABLE), KAFKA_KEY)

        # 2. Parse and validate.
        checked = parse_and_validate(batch).persist()
        valid = checked.filter(F.col("_reject_reason").isNull())
        invalid = checked.filter(F.col("_reject_reason").isNotNull())

        # 3. Silver: one row per event_id. Remove copies inside this batch first,
        #    because MERGE would insert BOTH copies of a key that is new to the table.
        silver = to_silver_events(valid, batch_id).dropDuplicates(["event_id"])
        valid_count = valid.count()
        silver_inserted = merge_insert(silver, cfg.paths.silver(SILVER_TABLE), ["event_id"], "event_date")

        # 4. Dead-letter queue: invalid messages go back to Kafka with the reason attached.
        dlq_count = invalid.count()
        if dlq_count:
            (
                invalid.select(
                    F.col("_kafka_key").alias("key"),
                    F.to_json(
                        F.struct(
                            F.col("_reject_reason").alias("reason"),
                            F.col("value").alias("original_value"),
                            "_kafka_topic",
                            "_kafka_partition",
                            "_kafka_offset",
                            F.lit(batch_id).alias("batch_id"),
                        )
                    ).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", cfg.bootstrap or bootstrap_servers())
                .option("topic", cfg.dlq_topic)
                .save()
            )

        # 5. Audit.
        duplicates = valid_count - silver_inserted
        spark.createDataFrame(
            [(batch_id, messages, bronze_inserted, valid_count, silver_inserted, duplicates, dlq_count)],
            "batch_id long, messages long, bronze_inserted long, valid_events long, "
            "silver_inserted long, duplicate_events long, dlq_messages long",
        ).withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("append").save(
            cfg.paths.audit(AUDIT_TABLE)
        )
        log.info(
            "batch %d: %d messages, %d new in Bronze, %d valid, %d new events, %d duplicates, %d to DLQ",
            batch_id, messages, bronze_inserted, valid_count, silver_inserted, duplicates, dlq_count,
        )
        checked.unpersist()
    finally:
        batch.unpersist()


# --------------------------------------------------------------------------- run
def start_stream(spark: SparkSession, cfg: StreamConfig, available_now: bool, trigger_seconds: int = 10):
    writer = (
        read_kafka(spark, cfg)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, cfg))
        .option("checkpointLocation", cfg.checkpoint)
        .queryName("payment_events_to_bronze")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)  # everything available now, in batches, then stop
    else:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")
    return writer.start()


def run_available_now(spark: SparkSession, cfg: StreamConfig) -> None:
    """Process all messages waiting in Kafka, then return. Used by tests and scheduled runs."""
    query = start_stream(spark, cfg, available_now=True)
    query.awaitTermination()
    if query.exception():
        raise RuntimeError(query.exception())


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Stream payment events from Kafka into Bronze and Silver")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--available-now", action="store_true", help="process what is in Kafka now, then stop")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-offsets", type=int, default=2000)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()
    cfg = StreamConfig(paths=paths, max_offsets_per_trigger=args.max_offsets)

    spark = get_spark("payrecon-stream")
    try:
        if args.available_now:
            run_available_now(spark, cfg)
        else:
            query = start_stream(spark, cfg, available_now=False, trigger_seconds=args.trigger_seconds)
            print(f"Streaming every {args.trigger_seconds}s. Checkpoint: {cfg.checkpoint}. Press Ctrl+C to stop.")
            try:
                query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping... (the next start continues from the checkpoint)")
                query.stop()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
