"""Print a quick overview of the Bronze and Silver tables and the audit logs. Read-only.

Usage:
  python -m payrecon.tools.show_tables
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payrecon.common.paths import default_paths
from payrecon.common.spark import get_spark
from payrecon.gold.reconciliation import GOLD_AUDIT_TABLE, RECON_TABLE, SUMMARY_TABLE
from payrecon.ingestion.bronze import AUDIT_TABLE, FILE_KINDS
from payrecon.transform.silver import ENTITIES, SILVER_AUDIT_TABLE


def load_or_none(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None


def main() -> None:
    paths = default_paths()
    spark = get_spark("payrecon-show")
    try:
        for kind in FILE_KINDS.values():
            print(f"\n=== bronze.{kind.bronze_table} ===")
            df = load_or_none(spark, paths.bronze(kind.bronze_table))
            if df is None:
                print("(table does not exist yet)")
                continue
            print(f"total rows: {df.count()}")
            df.groupBy("_business_date", "_file_psp_code").count().orderBy("_business_date", "_file_psp_code").show()

        for entity in ENTITIES:
            print(f"\n=== silver.{entity.silver_table} ===")
            df = load_or_none(spark, paths.silver(entity.silver_table))
            print("(table does not exist yet)" if df is None else f"total rows: {df.count()}")

            print(f"\n=== silver.{entity.quarantine_table}: rows per reason ===")
            q = load_or_none(spark, paths.silver(entity.quarantine_table))
            if q is None:
                print("(table does not exist yet)")
            else:
                q.groupBy("_reject_reason").count().orderBy(F.col("count").desc()).show(truncate=False)

        print("\n=== gold.reconciliation: keys per status ===")
        recon = load_or_none(spark, paths.gold(RECON_TABLE))
        if recon is None:
            print("(table does not exist yet)")
        else:
            recon.groupBy("recon_status").count().orderBy(F.col("count").desc()).show(truncate=False)

        print("\n=== gold.merchant_daily_summary (first 10 merchant-days) ===")
        summary = load_or_none(spark, paths.gold(SUMMARY_TABLE))
        if summary is None:
            print("(table does not exist yet)")
        else:
            summary.select(
                "merchant_id", "business_date", "currency", "transaction_count", "rejection_rate",
                "settled_net_amount", "matched_count", "break_count",
            ).orderBy("business_date", "merchant_id").show(10, truncate=False)

        for table in (AUDIT_TABLE, SILVER_AUDIT_TABLE, GOLD_AUDIT_TABLE):
            print(f"\n=== audit.{table} (latest 10) ===")
            audit = load_or_none(spark, paths.audit(table))
            if audit is None:
                print("(table does not exist yet)")
                continue
            order_col = "finished_at" if table == AUDIT_TABLE else "processed_at"  # audit tables name it differently
            audit.orderBy(F.col(order_col).desc()).show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
