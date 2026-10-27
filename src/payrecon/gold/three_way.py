"""Three-way reconciliation: stream (authorisation) vs clearing vs settlement.

Reads silver/transaction_lifecycle (stream + clearing) and gold/reconciliation
(clearing vs settlement, milestone 1.4), and writes gold/three_way_reconciliation:
one row per payment with two independent verdicts.

stream_status  - does the real-time event agree with the clearing file?
  MATCHED                       seen in both, same amount
  AMOUNT_DIFF_STREAM_CLEARING   seen in both, amounts differ
  DECLINED_NOT_CLEARED          declined in the stream, never cleared (correct)
  PENDING_CLEARING              authorised, not cleared yet, clearing window still open
  MISSING_IN_CLEARING           authorised, never cleared after the window: money at risk
  MISSING_IN_STREAM             cleared, but the gateway never sent an event

settlement_status - the clearing vs settlement status from gold/reconciliation
  (NULL when the payment was never cleared or never needed settling)

A payment can have a problem on both sides, so the two verdicts are kept separate.

Usage:
  python -m payrecon.gold.three_way --as-of-date 2026-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.gold.reconciliation import MATCHED, PENDING_SETTLEMENT, RECON_TABLE
from payrecon.transform.lifecycle import KEY, LIFECYCLE_TABLE

log = logging.getLogger("payrecon.three_way")

THREE_WAY_TABLE = "three_way_reconciliation"
DEFAULT_CLEARING_WINDOW_DAYS = 2

AMOUNT_DIFF = "AMOUNT_DIFF_STREAM_CLEARING"
DECLINED_NOT_CLEARED = "DECLINED_NOT_CLEARED"
PENDING_CLEARING = "PENDING_CLEARING"
MISSING_IN_CLEARING = "MISSING_IN_CLEARING"
MISSING_IN_STREAM = "MISSING_IN_STREAM"

STREAM_OK = (MATCHED, DECLINED_NOT_CLEARED, PENDING_CLEARING)
SETTLEMENT_OK = (MATCHED, PENDING_SETTLEMENT)


def build_three_way(
    lifecycle: DataFrame, recon: DataFrame, as_of_date: date, window_days: int, run_id: str
) -> DataFrame:
    in_stream = F.col("seen_in_stream")
    in_clearing = F.col("seen_in_clearing")
    both = in_stream & in_clearing
    window_open = F.lit(as_of_date) <= F.date_add(F.to_date("authorised_ts"), window_days)

    stream_status = (
        F.when(both & (F.col("stream_amount") != F.col("clearing_amount")), F.lit(AMOUNT_DIFF))
        .when(both, F.lit(MATCHED))
        .when(in_stream & (F.col("stream_event_type") == "DECLINED"), F.lit(DECLINED_NOT_CLEARED))
        .when(in_stream & window_open, F.lit(PENDING_CLEARING))
        .when(in_stream, F.lit(MISSING_IN_CLEARING))
        .otherwise(F.lit(MISSING_IN_STREAM))
    )

    settlement = recon.select(
        *KEY,
        F.col("recon_status").alias("settlement_status"),
        "settled_gross_amount",
        "settlement_count",
    )
    joined = lifecycle.withColumn("stream_status", stream_status).join(settlement, on=KEY, how="left")

    stream_break = ~F.col("stream_status").isin(*STREAM_OK)
    settlement_break = F.col("settlement_status").isNotNull() & ~F.col("settlement_status").isin(*SETTLEMENT_OK)

    return joined.select(
        *KEY,
        "merchant_id",
        "currency",
        "transaction_type",
        F.coalesce(F.col("clearing_business_date"), F.to_date("authorised_ts")).alias("business_date"),
        "seen_in_stream",
        "seen_in_clearing",
        "first_seen_channel",
        "authorised_ts",
        "clearing_ts",
        # How long after the card tap did the clearing file confirm it? (processing lag)
        ((F.col("clearing_received_ts").cast("long") - F.col("stream_received_ts").cast("long")) / 3600.0)
        .alias("hours_stream_to_clearing"),
        "stream_amount",
        "clearing_amount",
        "settled_gross_amount",
        "settlement_count",
        F.col("stream_status"),
        F.col("settlement_status"),
        stream_break.alias("is_stream_break"),
        settlement_break.alias("is_settlement_break"),
        (stream_break | settlement_break).alias("is_break"),
        F.lit(as_of_date).alias("as_of_date"),
        F.lit(run_id).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_ts"),
    )


def run_three_way(
    spark: SparkSession,
    paths: Paths,
    as_of_date: date,
    window_days: int = DEFAULT_CLEARING_WINDOW_DAYS,
    run_id: str | None = None,
) -> dict[str, int]:
    run_id = run_id or uuid.uuid4().hex
    lifecycle_path, recon_path = paths.silver(LIFECYCLE_TABLE), paths.gold(RECON_TABLE)
    for path, step in ((lifecycle_path, "payrecon.transform.lifecycle"), (recon_path, "payrecon.gold.reconciliation")):
        if not DeltaTable.isDeltaTable(spark, path):
            raise FileNotFoundError(f"{path} not found. Run {step} first.")

    result = build_three_way(
        spark.read.format("delta").load(lifecycle_path),
        spark.read.format("delta").load(recon_path),
        as_of_date,
        window_days,
        run_id,
    )
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("business_date")
        .save(paths.gold(THREE_WAY_TABLE))
    )
    written = spark.read.format("delta").load(paths.gold(THREE_WAY_TABLE))
    return {r["stream_status"]: r["count"] for r in written.groupBy("stream_status").count().collect()}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Three-way reconciliation: stream vs clearing vs settlement")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=None, help="default: today (UTC)")
    parser.add_argument("--clearing-window-days", type=int, default=DEFAULT_CLEARING_WINDOW_DAYS)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()
    as_of_date = args.as_of_date or datetime.now(timezone.utc).date()

    spark = get_spark("payrecon-three-way")
    try:
        counts = run_three_way(spark, paths, as_of_date, args.clearing_window_days)
        table = spark.read.format("delta").load(paths.gold(THREE_WAY_TABLE))
        print(f"\nThree-way reconciliation as of {as_of_date}")
        print("\nStream vs clearing:")
        for status in sorted(counts, key=lambda s: (s != MATCHED, s)):
            print(f"  {status:<30} {counts[status]:>7}")
        print("\nStream verdict x settlement verdict:")
        table.groupBy("stream_status", "settlement_status").count().orderBy(F.col("count").desc()).show(30, truncate=False)
        print(f"Payments with at least one break: {table.filter('is_break').count()}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
