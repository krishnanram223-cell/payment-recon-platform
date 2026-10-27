"""Transaction lifecycle: ONE row per payment, filled in by both channels.

A payment is seen twice, by two different systems, in no guaranteed order:
  - the STREAM (Kafka event, seconds after the card tap)   -> silver/payment_events
  - the CLEARING file (end of day)                          -> silver/transactions

silver/transaction_lifecycle keeps one row per (psp_code, psp_transaction_id) and
records what each channel said about it:

  seen_in_stream   + stream_*   columns   (filled by the stream side)
  seen_in_clearing + clearing_* columns   (filled by the clearing side)

Each side MERGEs only its own columns:
  - key not in the table yet      -> insert a new row (first_seen_channel = this side)
  - key there, this side not seen -> fill in this side's columns
  - key there, this side seen     -> do nothing (re-runs and replays change nothing)

So the final table is the same whichever channel arrives first, and however often
each job runs. Nothing is ever counted twice.

Usage:
  python -m payrecon.transform.lifecycle                    both sides, then stop
  python -m payrecon.transform.lifecycle --source clearing  only the clearing side
  python -m payrecon.transform.lifecycle --source stream    only the stream side (available-now)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark

log = logging.getLogger("payrecon.lifecycle")

LIFECYCLE_TABLE = "transaction_lifecycle"
KEY = ["psp_code", "psp_transaction_id"]
COMMON = ["merchant_id", "currency", "transaction_type"]
STREAM_COLUMNS = ["stream_event_id", "stream_event_type", "authorised_ts", "stream_amount", "stream_received_ts"]
CLEARING_COLUMNS = [
    "clearing_business_date",
    "clearing_ts",
    "clearing_amount",
    "clearing_response_code",
    "clearing_source_file",
    "clearing_received_ts",
]

MONEY = DecimalType(18, 2)
LIFECYCLE_SCHEMA = StructType(
    [
        StructField("psp_code", StringType(), False),
        StructField("psp_transaction_id", StringType(), False),
        StructField("merchant_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("seen_in_stream", BooleanType(), False),
        StructField("stream_event_id", StringType(), True),
        StructField("stream_event_type", StringType(), True),
        StructField("authorised_ts", TimestampType(), True),
        StructField("stream_amount", MONEY, True),
        StructField("stream_received_ts", TimestampType(), True),
        StructField("seen_in_clearing", BooleanType(), False),
        StructField("clearing_business_date", DateType(), True),
        StructField("clearing_ts", TimestampType(), True),
        StructField("clearing_amount", MONEY, True),
        StructField("clearing_response_code", StringType(), True),
        StructField("clearing_source_file", StringType(), True),
        StructField("clearing_received_ts", TimestampType(), True),
        StructField("first_seen_channel", StringType(), False),
        StructField("last_updated_ts", TimestampType(), False),
    ]
)


# --------------------------------------------------------------------------- table + merge
def ensure_table(spark: SparkSession, path: str) -> None:
    """Create the empty table once, with an explicit schema, so both sides can always MERGE."""
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], LIFECYCLE_SCHEMA).write.format("delta").save(path)


def with_retry(action: Callable[[], None], attempts: int = 5) -> None:
    """Retry when another job committed to the same table at the same moment.

    Delta uses optimistic concurrency: two writers may run at once, and if both touched
    the same data, the second commit fails with a Concurrent...Exception. Because our
    MERGE is idempotent, simply running it again is always safe.
    """
    for attempt in range(1, attempts + 1):
        try:
            action()
            return
        except Exception as exc:  # the exact class name varies between Delta versions
            if "Concurrent" not in type(exc).__name__ or attempt == attempts:
                raise
            log.warning("Concurrent write on attempt %d, retrying: %s", attempt, type(exc).__name__)
            time.sleep(attempt)


def merge_side(source: DataFrame, path: str, side: str) -> None:
    """MERGE one channel's view into the lifecycle table. side = 'stream' or 'clearing'."""
    own = STREAM_COLUMNS if side == "stream" else CLEARING_COLUMNS
    flag = f"seen_in_{side}"
    other_flag = "seen_in_clearing" if side == "stream" else "seen_in_stream"
    spark = source.sparkSession
    ensure_table(spark, path)

    update = {c: f"s.{c}" for c in own}
    update.update({flag: "true", "last_updated_ts": "current_timestamp()"})
    insert = {c: f"s.{c}" for c in KEY + COMMON + own}
    insert.update(
        {
            flag: "true",
            other_flag: "false",
            "first_seen_channel": f"'{side.upper()}'",
            "last_updated_ts": "current_timestamp()",
        }
    )  # the other side's columns are simply not set: they start as NULL

    condition = " AND ".join(f"t.{k} = s.{k}" for k in KEY)

    def run() -> None:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(source.alias("s"), condition)
            .whenMatchedUpdate(condition=f"t.{flag} = false", set=update)
            .whenNotMatchedInsert(values=insert)
            .execute()
        )

    with_retry(run)


# --------------------------------------------------------------------------- the two sides
def clearing_side(transactions: DataFrame) -> DataFrame:
    """silver/transactions already has one row per key."""
    return transactions.select(
        *KEY,
        *COMMON,
        F.col("_business_date").alias("clearing_business_date"),
        F.col("transaction_ts").alias("clearing_ts"),
        F.col("amount").alias("clearing_amount"),
        F.col("response_code").alias("clearing_response_code"),
        F.col("_source_file").alias("clearing_source_file"),
        F.col("_silver_processed_ts").alias("clearing_received_ts"),
    )


def stream_side(events: DataFrame) -> DataFrame:
    """One row per key: if a payment ever has several events, the earliest one describes it.

    MERGE requires at most one source row per key; two rows for the same new key
    would fail the MERGE (or, for inserts, create two rows).
    """
    first = Window.partitionBy(*KEY).orderBy("event_ts", "event_id")
    return (
        events.withColumn("_rn", F.row_number().over(first))
        .filter(F.col("_rn") == 1)
        .select(
            *KEY,
            *COMMON,
            F.col("event_id").alias("stream_event_id"),
            F.col("event_type").alias("stream_event_type"),
            F.col("event_ts").alias("authorised_ts"),
            F.col("amount").alias("stream_amount"),
            F.col("_ingest_ts").alias("stream_received_ts"),
        )
    )


def run_clearing_side(spark: SparkSession, paths: Paths) -> None:
    source_path = paths.silver("transactions")
    if not DeltaTable.isDeltaTable(spark, source_path):
        log.info("clearing side: silver/transactions does not exist yet")
        return
    transactions = spark.read.format("delta").load(source_path)
    # Full MERGE every run: rows already marked seen_in_clearing are left untouched.
    # (A larger system would read only new rows, e.g. with Delta's change data feed.)
    merge_side(clearing_side(transactions), paths.silver(LIFECYCLE_TABLE), "clearing")
    log.info("clearing side merged")


def run_stream_side(spark: SparkSession, paths: Paths, available_now: bool = True, trigger_seconds: int = 10):
    """Delta -> Delta streaming: read NEW rows of silver/payment_events as they are committed."""
    source_path = paths.silver("payment_events")
    if not DeltaTable.isDeltaTable(spark, source_path):
        log.info("stream side: silver/payment_events does not exist yet")
        return None
    target = paths.silver(LIFECYCLE_TABLE)
    ensure_table(spark, target)

    def process(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        merge_side(stream_side(batch), target, "stream")
        log.info("stream side: batch %d merged", batch_id)

    writer = (
        spark.readStream.format("delta")
        .load(source_path)
        .writeStream.foreachBatch(process)
        .option("checkpointLocation", str(paths.lakehouse / "_checkpoints" / "lifecycle_from_stream"))
        .queryName("lifecycle_from_stream")
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=f"{trigger_seconds} seconds")
    query = writer.start()
    if available_now:
        query.awaitTermination()
        if query.exception():
            raise RuntimeError(query.exception())
    return query


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Build the transaction lifecycle from stream and clearing")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--source", choices=["both", "clearing", "stream"], default="both")
    parser.add_argument("--continuous", action="store_true", help="keep the stream side running")
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()

    spark = get_spark("payrecon-lifecycle")
    try:
        if args.source in ("both", "clearing"):
            run_clearing_side(spark, paths)
        if args.source in ("both", "stream"):
            query = run_stream_side(spark, paths, available_now=not args.continuous)
            if args.continuous and query is not None:
                print("Lifecycle stream running. Press Ctrl+C to stop.")
                try:
                    query.awaitTermination()
                except KeyboardInterrupt:
                    query.stop()
        lifecycle = spark.read.format("delta").load(paths.silver(LIFECYCLE_TABLE))
        print("\nLifecycle rows by channel:")
        lifecycle.groupBy("seen_in_stream", "seen_in_clearing").count().orderBy("seen_in_stream", "seen_in_clearing").show()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
