"""Near-real-time merchant volumes: windowed streaming aggregation with a WATERMARK.

Kafka payment-events -> parse/validate -> drop duplicate deliveries -> 15-minute windows
per merchant -> gold/merchant_volumes_15m  (one row per merchant, currency and window)

Two clocks matter in streaming:
  event time       when the payment happened   (event_ts, inside the message)
  processing time  when Spark sees the message (whenever it arrives)

Events arrive out of order: a payment from 10:05 can arrive after one from 13:00.
Spark cannot keep every window open forever (memory would grow without limit), so we
give it a WATERMARK:

    watermark = latest event time seen so far - allowed lateness (default 10 minutes)

  - a window is FINAL once the watermark passes its end; in append mode it is written
    to the table exactly once, at that moment, and its state is deleted from memory
  - an event older than the watermark is LATE: it is dropped and never counted here

So this table is fast but can be incomplete. Late events are not lost: Silver keeps every
event, and payrecon.gold.volume_restatement recomputes the same windows from Silver in
batch to show (and correct) the difference. Fast answer now, exact answer later.

Duplicate deliveries (same event_id) are removed with dropDuplicatesWithinWatermark:
the event ids are remembered only until the watermark passes, so that state stays small too.

Usage:
  python -m payrecon.streaming.merchant_volumes --available-now      everything in Kafka, then stop
  python -m payrecon.streaming.merchant_volumes                      continuous (Ctrl+C to stop)
  python -m payrecon.streaming.merchant_volumes --available-now --watermark-minutes 480
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.streaming.stream_to_bronze import StreamConfig, parse_and_validate, read_kafka

log = logging.getLogger("payrecon.volumes")

VOLUMES_TABLE = "merchant_volumes_15m"
AUDIT_TABLE = "volume_stream_runs"
WINDOW_KEY = ["window_start", "merchant_id", "currency"]
DEFAULT_WINDOW_MINUTES = 15
DEFAULT_WATERMARK_MINUTES = 10
# Small batches so the watermark moves forward often, as in a live system that runs
# every few seconds. One huge batch would see everything at once and drop nothing.
DEFAULT_MAX_OFFSETS = 60


@dataclass(frozen=True)
class VolumeConfig:
    paths: Paths
    window_minutes: int = DEFAULT_WINDOW_MINUTES
    watermark_minutes: int = DEFAULT_WATERMARK_MINUTES
    max_offsets_per_trigger: int = DEFAULT_MAX_OFFSETS
    bootstrap: str = ""
    checkpoint_name: str = "merchant_volumes"
    table: str = VOLUMES_TABLE

    @property
    def checkpoint(self) -> str:
        return str(self.paths.lakehouse / "_checkpoints" / self.checkpoint_name)


# --------------------------------------------------------------------------- transformations
def valid_events(checked: DataFrame) -> DataFrame:
    """Only what the volumes need, from the output of parse_and_validate()."""
    return checked.filter(F.col("_reject_reason").isNull()).select(
        "event_id",
        F.col("_t_event_ts").alias("event_ts"),
        "merchant_id",
        "currency",
        "event_type",
        F.col("_t_amount").alias("amount"),
    )


def aggregate_windows(events: DataFrame, window_minutes: int) -> DataFrame:
    """Tumbling (fixed, non-overlapping) windows per merchant and currency.

    The SAME function is used by the streaming job and by the batch restatement, so any
    difference between the two tables can only come from late events.
    """
    approved = F.col("event_type").isin("AUTHORISED", "REFUNDED")
    signed = F.when(F.col("event_type") == "REFUNDED", -F.col("amount")).otherwise(F.col("amount"))
    return (
        events.groupBy(F.window("event_ts", f"{window_minutes} minutes"), "merchant_id", "currency")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(approved, 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(F.col("event_type") == "DECLINED", 1).otherwise(0)).alias("declined_count"),
            F.coalesce(F.sum(F.when(approved, signed)), F.lit(0).cast("decimal(28,2)"))
            .cast("decimal(18,2)")
            .alias("net_approved_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "merchant_id",
            "currency",
            "event_count",
            "approved_count",
            "declined_count",
            "net_approved_amount",
        )
    )


def stream_volumes(events: DataFrame, window_minutes: int, watermark_minutes: int) -> DataFrame:
    """The streaming plan: watermark -> dedup within the watermark -> windowed counts."""
    return aggregate_windows(
        events.withWatermark("event_ts", f"{watermark_minutes} minutes").dropDuplicatesWithinWatermark(["event_id"]),
        window_minutes,
    )


def batch_volumes(events: DataFrame, window_minutes: int) -> DataFrame:
    """The batch plan over complete data: no watermark, nothing is late."""
    return aggregate_windows(events.dropDuplicates(["event_id"]), window_minutes)


# --------------------------------------------------------------------------- run
def start_volumes(
    spark: SparkSession, events: DataFrame, cfg: VolumeConfig, available_now: bool, trigger_seconds: int = 10
) -> StreamingQuery:
    """Write finished windows to Delta. `events` is any streaming DataFrame of valid_events()."""
    writer = (
        stream_volumes(events, cfg.window_minutes, cfg.watermark_minutes)
        .withColumn("_emitted_ts", F.current_timestamp())
        .writeStream.format("delta")
        .outputMode("append")  # a window is written once, when it is final; never updated
        .option("checkpointLocation", cfg.checkpoint)
        .queryName("merchant_volumes")
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=f"{trigger_seconds} seconds")
    return writer.start(cfg.paths.gold(cfg.table))


def kafka_events(spark: SparkSession, cfg: VolumeConfig) -> DataFrame:
    stream_cfg = StreamConfig(paths=cfg.paths, bootstrap=cfg.bootstrap, max_offsets_per_trigger=cfg.max_offsets_per_trigger)
    return valid_events(parse_and_validate(read_kafka(spark, stream_cfg)))


def _get(obj, name: str):
    """Progress reports are dicts in older PySpark and objects in PySpark 4."""
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def summarise_progress(query: StreamingQuery) -> dict:
    """What the finished run did, from Spark's own per-batch progress reports."""
    progress = query.recentProgress or []
    dropped = 0
    for p in progress:
        for op in _get(p, "stateOperators") or []:
            dropped += _get(op, "numRowsDroppedByWatermark") or 0
    last = progress[-1] if progress else None
    event_time = (_get(last, "eventTime") or {}) if last is not None else {}
    return {
        "batches": len(progress),
        "input_rows": sum(_get(p, "numInputRows") or 0 for p in progress),
        "rows_dropped_by_watermark": dropped,
        "final_watermark": event_time.get("watermark") if isinstance(event_time, dict) else None,
    }


def run_available_now(spark: SparkSession, events: DataFrame, cfg: VolumeConfig) -> dict:
    query = start_volumes(spark, events, cfg, available_now=True)
    query.awaitTermination()
    if query.exception():
        raise RuntimeError(query.exception())
    summary = summarise_progress(query)
    run_id = uuid.uuid4().hex
    spark.createDataFrame(
        [(run_id, cfg.window_minutes, cfg.watermark_minutes, summary["batches"], summary["input_rows"],
          summary["rows_dropped_by_watermark"], summary["final_watermark"])],
        "run_id string, window_minutes int, watermark_minutes int, batches long, input_rows long, "
        "rows_dropped_by_watermark long, final_watermark string",
    ).withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("append").save(
        cfg.paths.audit(AUDIT_TABLE)
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Near-real-time merchant volumes with a watermark")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--available-now", action="store_true", help="process what is in Kafka now, then stop")
    parser.add_argument("--window-minutes", type=int, default=DEFAULT_WINDOW_MINUTES)
    parser.add_argument("--watermark-minutes", type=int, default=DEFAULT_WATERMARK_MINUTES)
    parser.add_argument("--max-offsets", type=int, default=DEFAULT_MAX_OFFSETS)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()
    cfg = VolumeConfig(
        paths=paths,
        window_minutes=args.window_minutes,
        watermark_minutes=args.watermark_minutes,
        max_offsets_per_trigger=args.max_offsets,
    )

    spark = get_spark("payrecon-volumes")
    try:
        events = kafka_events(spark, cfg)
        if args.available_now:
            s = run_available_now(spark, events, cfg)
            table = spark.read.format("delta").load(paths.gold(cfg.table))
            print(f"\nMerchant volumes ({cfg.window_minutes}-min windows, watermark {cfg.watermark_minutes} min)")
            print(f"  micro-batches               {s['batches']:>7}")
            print(f"  messages read               {s['input_rows']:>7}")
            print(f"  dropped as too late         {s['rows_dropped_by_watermark']:>7}")
            print(f"  final windows in the table  {table.count():>7}")
            print(f"  watermark now               {s['final_watermark']}")
            print("  (windows after the watermark are still open and stay in the checkpoint until more data arrives)")
        else:
            query = start_volumes(spark, events, cfg, available_now=False, trigger_seconds=args.trigger_seconds)
            print(f"Streaming every {args.trigger_seconds}s. Checkpoint: {cfg.checkpoint}. Press Ctrl+C to stop.")
            try:
                query.awaitTermination()
            except KeyboardInterrupt:
                print("\nStopping... (the next start continues from the checkpoint, open windows included)")
                query.stop()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
