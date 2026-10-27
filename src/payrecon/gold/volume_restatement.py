"""Restate the real-time merchant volumes from Silver, and show what the stream missed.

The streaming job (payrecon.streaming.merchant_volumes) is fast but drops events that
arrive after its watermark. Silver (silver/payment_events) keeps EVERY event, late ones
included. This batch job recomputes the same windows from Silver with the SAME
aggregation code, writes gold/merchant_volumes_15m_restated, and compares the two.

Only windows the stream has already finalised are compared: a window that is still open
in the stream is not "wrong", it is just not finished yet.

  stream event_count + late events dropped = restated event_count     (for every final window)

In production this runs a few hours after the day ends (when late events have arrived)
and the dashboards switch from the real-time numbers to the restated ones.

Usage:
  python -m payrecon.gold.volume_restatement
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.streaming.merchant_volumes import (
    DEFAULT_WINDOW_MINUTES,
    VOLUMES_TABLE,
    WINDOW_KEY,
    batch_volumes,
)

log = logging.getLogger("payrecon.restatement")

RESTATED_TABLE = "merchant_volumes_15m_restated"
MEASURES = ["event_count", "approved_count", "declined_count", "net_approved_amount"]


def compare_volumes(streamed: DataFrame, restated: DataFrame) -> DataFrame:
    """One row per window that the stream has finalised, with stream vs batch side by side.

    A window can be MISSING from the stream table entirely when every one of its events
    was late, so the join starts from the batch side and treats a missing row as zero.
    """
    last_final_end = streamed.agg(F.max("window_end")).first()[0]  # None -> nothing final yet -> empty result
    s = streamed.select(*WINDOW_KEY, *[F.col(m).alias(f"stream_{m}") for m in MEASURES])
    b = restated.filter(F.col("window_end") <= F.lit(last_final_end).cast("timestamp")).select(
        *WINDOW_KEY, "window_end", *[F.col(m).alias(f"batch_{m}") for m in MEASURES]
    )
    joined = b.join(s, on=WINDOW_KEY, how="left")
    for m in MEASURES:
        joined = joined.withColumn(f"stream_{m}", F.coalesce(F.col(f"stream_{m}"), F.lit(0).cast(joined.schema[f"batch_{m}"].dataType)))
    return joined.withColumn("late_events_missing", F.col("batch_event_count") - F.col("stream_event_count")).withColumn(
        "is_different", F.col("late_events_missing") != 0
    )


def run_restatement(spark: SparkSession, paths: Paths, window_minutes: int = DEFAULT_WINDOW_MINUTES) -> DataFrame:
    events_path = paths.silver("payment_events")
    if not DeltaTable.isDeltaTable(spark, events_path):
        raise FileNotFoundError(f"{events_path} not found. Run payrecon.streaming.stream_to_bronze first.")
    events = spark.read.format("delta").load(events_path)
    (
        batch_volumes(events, window_minutes)
        .withColumn("_restated_ts", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(paths.gold(RESTATED_TABLE))
    )
    return spark.read.format("delta").load(paths.gold(RESTATED_TABLE))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Recompute merchant volumes from Silver and compare with the stream")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--window-minutes", type=int, default=DEFAULT_WINDOW_MINUTES)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()

    spark = get_spark("payrecon-restatement")
    try:
        restated = run_restatement(spark, paths, args.window_minutes)
        print(f"\nRestated windows (all of Silver): {restated.count()}   events: {restated.agg(F.sum('event_count')).first()[0]}")
        streamed_path = paths.gold(VOLUMES_TABLE)
        if not DeltaTable.isDeltaTable(spark, streamed_path):
            print("No streaming volumes yet. Run payrecon.streaming.merchant_volumes to compare.")
            return 0
        diff = compare_volumes(spark.read.format("delta").load(streamed_path), restated).persist()
        totals = diff.agg(
            F.count("*").alias("windows"),
            F.sum(F.col("is_different").cast("int")).alias("windows_different"),
            F.sum("batch_event_count").alias("batch_events"),
            F.sum("stream_event_count").alias("stream_events"),
        ).first()
        if not totals["windows"]:
            print("The stream has not finalised any window yet (they are all still open).")
            return 0
        print("\nWindows the stream has finalised:")
        print(f"  windows compared                {totals['windows']:>7}")
        print(f"  events counted by batch         {totals['batch_events']:>7}")
        print(f"  events counted by the stream    {totals['stream_events']:>7}")
        print(f"  late events the stream missed   {totals['batch_events'] - totals['stream_events']:>7}")
        print(f"  windows with a difference       {totals['windows_different']:>7}")
        print("\nFirst windows that differ:")
        diff.filter("is_different").select(
            *WINDOW_KEY, "batch_event_count", "stream_event_count", "batch_net_approved_amount", "stream_net_approved_amount"
        ).orderBy(*WINDOW_KEY).show(10, truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
