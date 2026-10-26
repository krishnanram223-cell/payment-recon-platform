"""Gold layer: reconcile clearing transactions with settlements, and summarise per merchant per day.

Reads Silver, rebuilds two Gold tables, and logs the run:

  gold/reconciliation            one row per transaction or settlement key, with a recon status
  gold/merchant_daily_summary    one row per merchant per business date: volumes, money, breaks
  audit/gold_runs                one row per status per run

Recon statuses (checked in this order, first match wins):

  ORPHAN_SETTLEMENT      settlement with no matching transaction
  UNEXPECTED_SETTLEMENT  settlement for a DECLINED transaction
  PENDING_SETTLEMENT     approved, no settlement yet, still inside the settlement window
  MISSING_SETTLEMENT     approved, no settlement, window has passed
  DUPLICATE_SETTLEMENT   more than one settlement line for one transaction
  CURRENCY_MISMATCH      settled in a different currency
  AMOUNT_MISMATCH        settled gross amount differs from the signed transaction amount
  MATCHED                everything agrees

Usage:
  python -m payrecon.gold.reconciliation
  python -m payrecon.gold.reconciliation --as-of-date 2026-10-25 --settlement-window-days 2
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark

log = logging.getLogger("payrecon.gold")

RECON_TABLE = "reconciliation"
SUMMARY_TABLE = "merchant_daily_summary"
GOLD_AUDIT_TABLE = "gold_runs"
DEFAULT_SETTLEMENT_WINDOW_DAYS = 2

MONEY = DecimalType(18, 2)

ORPHAN_SETTLEMENT = "ORPHAN_SETTLEMENT"
UNEXPECTED_SETTLEMENT = "UNEXPECTED_SETTLEMENT"
PENDING_SETTLEMENT = "PENDING_SETTLEMENT"
MISSING_SETTLEMENT = "MISSING_SETTLEMENT"
DUPLICATE_SETTLEMENT = "DUPLICATE_SETTLEMENT"
CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
MATCHED = "MATCHED"

KEY = ["psp_code", "psp_transaction_id"]


# --------------------------------------------------------------------------- reconciliation
def build_reconciliation(
    transactions: DataFrame, settlements: DataFrame, as_of_date: date, window_days: int, run_id: str
) -> DataFrame:
    """Match every approved transaction with its settlement lines and give each pair a status."""
    txn = transactions.select(
        *KEY,
        F.col("merchant_id").alias("_txn_merchant"),
        F.col("currency").alias("_txn_currency"),
        "transaction_type",
        "transaction_ts",
        "is_approved",
        F.col("signed_amount").alias("transaction_amount"),
        F.col("_business_date").alias("_txn_business_date"),
    )

    # Settlements first collapse to ONE row per transaction key, so a duplicate
    # settlement shows up as settlement_count = 2 instead of multiplying rows in the join.
    stl = settlements.groupBy(*KEY).agg(
        F.count("*").alias("settlement_count"),
        F.sum("gross_amount").cast(MONEY).alias("settled_gross_amount"),
        F.sum(F.col("interchange_fee") + F.col("scheme_fee")).cast(MONEY).alias("settled_fees"),
        F.sum("net_amount").cast(MONEY).alias("settled_net_amount"),
        F.concat_ws(",", F.sort_array(F.collect_list("settlement_id"))).alias("settlement_ids"),
        F.min("merchant_id").alias("_stl_merchant"),
        F.min("currency").alias("_stl_currency"),
        F.min("settlement_date").alias("first_settlement_date"),
        F.min("_business_date").alias("_stl_business_date"),
    )

    joined = txn.join(stl, on=KEY, how="full_outer")
    # Keep approved transactions (they must settle) and anything that was settled.
    # A declined transaction that was never settled is correct and needs no row.
    joined = joined.filter(F.col("is_approved") | F.col("settlement_count").isNotNull())

    no_transaction = F.col("_txn_business_date").isNull()
    no_settlement = F.col("settlement_count").isNull()
    window_open = F.lit(as_of_date) <= F.date_add(F.col("_txn_business_date"), window_days)

    status = (
        F.when(no_transaction, F.lit(ORPHAN_SETTLEMENT))
        .when(~F.col("is_approved"), F.lit(UNEXPECTED_SETTLEMENT))
        .when(no_settlement & window_open, F.lit(PENDING_SETTLEMENT))
        .when(no_settlement, F.lit(MISSING_SETTLEMENT))
        .when(F.col("settlement_count") > 1, F.lit(DUPLICATE_SETTLEMENT))
        .when(F.col("_stl_currency") != F.col("_txn_currency"), F.lit(CURRENCY_MISMATCH))
        .when(F.col("settled_gross_amount") != F.col("transaction_amount"), F.lit(AMOUNT_MISMATCH))
        .otherwise(F.lit(MATCHED))
    )

    return joined.select(
        *KEY,
        F.coalesce("_txn_merchant", "_stl_merchant").alias("merchant_id"),
        F.coalesce("_txn_currency", "_stl_currency").alias("currency"),
        F.coalesce("_txn_business_date", "_stl_business_date").alias("business_date"),
        "transaction_type",
        "transaction_ts",
        "transaction_amount",
        F.coalesce("settlement_count", F.lit(0)).alias("settlement_count"),
        "settlement_ids",
        "first_settlement_date",
        "settled_gross_amount",
        "settled_fees",
        "settled_net_amount",
        (F.col("settled_gross_amount") - F.col("transaction_amount")).cast(MONEY).alias("amount_difference"),
        status.alias("recon_status"),
        (status != F.lit(MATCHED)).alias("is_break"),
        F.lit(as_of_date).alias("as_of_date"),
        F.lit(run_id).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_ts"),
    )


# --------------------------------------------------------------------------- merchant summary
def load_merchant_reference(spark: SparkSession, paths: Paths) -> DataFrame:
    path = paths.reference / "merchants.csv"
    if not path.exists():
        raise FileNotFoundError(f"Merchant reference file not found: {path}")
    schema = StructType(
        [StructField(c, StringType(), True) for c in ("merchant_id", "merchant_name", "settlement_currency")]
    )
    return spark.read.option("header", True).schema(schema).csv(str(path)).dropDuplicates(["merchant_id"])


def count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def sum_if(condition: Column, amount: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, amount)), F.lit(0)).cast(MONEY)


def build_merchant_daily_summary(
    transactions: DataFrame, quarantine: DataFrame, recon: DataFrame, merchants: DataFrame, run_id: str
) -> DataFrame:
    keys = ["merchant_id", "business_date"]
    approved = F.col("is_approved")

    activity = transactions.groupBy("merchant_id", F.col("_business_date").alias("business_date")).agg(
        F.count("*").alias("transaction_count"),
        count_if(approved).alias("approved_count"),
        count_if(~approved).alias("declined_count"),
        count_if(F.col("is_late_arrival")).alias("late_arrival_count"),
        sum_if(approved & (F.col("transaction_type") == "PURCHASE"), F.col("amount")).alias("purchase_amount"),
        sum_if(approved & (F.col("transaction_type") == "REFUND"), F.col("amount")).alias("refund_amount"),
        sum_if(approved, F.col("signed_amount")).alias("net_transaction_amount"),
    )

    # Rows rejected for bad data (duplicates are not the merchant's data quality problem).
    rejected = (
        quarantine.filter(~F.col("_reject_reason").isin("DUPLICATE_IN_FILE", "CROSS_FILE_DUPLICATE"))
        .filter(F.col("merchant_id").isNotNull())
        .groupBy("merchant_id", F.col("_business_date").alias("business_date"))
        .agg(F.count("*").alias("rejected_row_count"))
    )

    status = F.col("recon_status")
    settlement = recon.groupBy(*keys).agg(
        count_if(status == MATCHED).alias("matched_count"),
        count_if(status == PENDING_SETTLEMENT).alias("pending_settlement_count"),
        count_if(status == MISSING_SETTLEMENT).alias("missing_settlement_count"),
        count_if(status == AMOUNT_MISMATCH).alias("amount_mismatch_count"),
        count_if(status == DUPLICATE_SETTLEMENT).alias("duplicate_settlement_count"),
        count_if(status == ORPHAN_SETTLEMENT).alias("orphan_settlement_count"),
        count_if(status.isin(UNEXPECTED_SETTLEMENT, CURRENCY_MISMATCH)).alias("other_break_count"),
        sum_if(F.lit(True), F.col("settled_gross_amount")).alias("settled_gross_amount"),
        sum_if(F.lit(True), F.col("settled_fees")).alias("settled_fees"),
        sum_if(F.lit(True), F.col("settled_net_amount")).alias("settled_net_amount"),
    )

    combined = (
        activity.join(rejected, on=keys, how="full_outer")
        .join(settlement, on=keys, how="full_outer")
        .fillna(0)  # a merchant-day missing from one side has zero of that measure
    )

    break_count = (
        F.col("missing_settlement_count")
        + F.col("amount_mismatch_count")
        + F.col("duplicate_settlement_count")
        + F.col("orphan_settlement_count")
        + F.col("other_break_count")
    )
    received = F.col("transaction_count") + F.col("rejected_row_count")

    return combined.join(merchants, on="merchant_id", how="left").select(
        "merchant_id",
        "merchant_name",
        F.col("settlement_currency").alias("currency"),
        "business_date",
        "transaction_count",
        "approved_count",
        "declined_count",
        "rejected_row_count",
        # try_divide gives NULL instead of a division-by-zero error (Spark 4 runs in ANSI mode)
        F.coalesce(F.round(F.try_divide(F.col("rejected_row_count"), received), 4), F.lit(0.0)).alias(
            "rejection_rate"
        ),
        "late_arrival_count",
        "purchase_amount",
        "refund_amount",
        "net_transaction_amount",
        "settled_gross_amount",
        "settled_fees",
        "settled_net_amount",
        "matched_count",
        "pending_settlement_count",
        "missing_settlement_count",
        "amount_mismatch_count",
        "duplicate_settlement_count",
        "orphan_settlement_count",
        "other_break_count",
        break_count.alias("break_count"),
        F.lit(run_id).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_ts"),
    )


# --------------------------------------------------------------------------- run
def read_silver(spark: SparkSession, paths: Paths, table: str) -> DataFrame:
    path = paths.silver(table)
    if not DeltaTable.isDeltaTable(spark, path):
        raise FileNotFoundError(f"Silver table '{table}' not found at {path}. Run the Silver job first.")
    return spark.read.format("delta").load(path)


def overwrite(df: DataFrame, path: str, partition_column: str) -> None:
    """Gold is rebuilt from Silver on every run: one atomic overwrite, old versions kept by Delta."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(partition_column)
        .save(path)
    )


def run_gold(
    spark: SparkSession,
    paths: Paths,
    as_of_date: date,
    window_days: int = DEFAULT_SETTLEMENT_WINDOW_DAYS,
    run_id: str | None = None,
) -> dict[str, int]:
    run_id = run_id or uuid.uuid4().hex
    transactions = read_silver(spark, paths, "transactions")
    settlements = read_silver(spark, paths, "settlements")
    quarantine = read_silver(spark, paths, "quarantine_transactions")
    merchants = load_merchant_reference(spark, paths)

    recon = build_reconciliation(transactions, settlements, as_of_date, window_days, run_id)
    overwrite(recon, paths.gold(RECON_TABLE), "business_date")

    # Build the summary from the table just written, so both Gold tables tell the same story.
    recon_written = spark.read.format("delta").load(paths.gold(RECON_TABLE))
    summary = build_merchant_daily_summary(transactions, quarantine, recon_written, merchants, run_id)
    overwrite(summary, paths.gold(SUMMARY_TABLE), "business_date")

    counts = {r["recon_status"]: r["count"] for r in recon_written.groupBy("recon_status").count().collect()}

    processed_at = datetime.now(timezone.utc)
    audit_rows = [(run_id, as_of_date, window_days, s, c, processed_at) for s, c in sorted(counts.items())]
    spark.createDataFrame(
        audit_rows,
        "run_id string, as_of_date date, settlement_window_days int, recon_status string, row_count long, processed_at timestamp",
    ).write.format("delta").mode("append").save(paths.audit(GOLD_AUDIT_TABLE))
    return counts


def print_summary(counts: dict[str, int], as_of_date: date) -> None:
    total = sum(counts.values())
    print(f"\nReconciliation as of {as_of_date}: {total} keys")
    for status in sorted(counts, key=lambda s: (s != MATCHED, s)):
        print(f"  {status:<24} {counts[status]:>7}")
    breaks = total - counts.get(MATCHED, 0) - counts.get(PENDING_SETTLEMENT, 0)
    print(f"\nBreaks needing attention: {breaks}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Reconcile Silver transactions with settlements into Gold")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=None, help="default: today (UTC)")
    parser.add_argument("--settlement-window-days", type=int, default=DEFAULT_SETTLEMENT_WINDOW_DAYS)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()
    as_of_date = args.as_of_date or datetime.now(timezone.utc).date()

    spark = get_spark("payrecon-gold")
    try:
        counts = run_gold(spark, paths, as_of_date, args.settlement_window_days)
    finally:
        spark.stop()

    print_summary(counts, as_of_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
