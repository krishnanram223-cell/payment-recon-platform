"""Print a quick overview of the Bronze tables and the ingestion audit log.

Usage:
  python -m payrecon.tools.show_tables
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from payrecon.common.paths import default_paths
from payrecon.common.spark import get_spark
from payrecon.ingestion.bronze import AUDIT_TABLE, FILE_KINDS


def main() -> None:
    paths = default_paths()
    spark = get_spark("payrecon-show")
    try:
        for kind in FILE_KINDS.values():
            path = paths.bronze(kind.bronze_table)
            print(f"\n=== bronze.{kind.bronze_table} ===")
            if not DeltaTable.isDeltaTable(spark, path):
                print("(table does not exist yet)")
                continue
            df = spark.read.format("delta").load(path)
            print(f"total rows: {df.count()}")
            df.groupBy("_business_date", "_file_psp_code").count().orderBy("_business_date", "_file_psp_code").show()

        audit_path = paths.audit(AUDIT_TABLE)
        print("\n=== audit.processed_files (latest 15) ===")
        if DeltaTable.isDeltaTable(spark, audit_path):
            (
                spark.read.format("delta")
                .load(audit_path)
                .orderBy(F.col("finished_at").desc())
                .select("file_name", "status", "reason", "expected_records", "ingested_records", "run_id")
                .show(15, truncate=False)
            )
        else:
            print("(table does not exist yet)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
