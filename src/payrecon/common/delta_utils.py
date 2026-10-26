"""Small Delta Lake helpers shared by several jobs."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def merge_insert(df: DataFrame, path: str, keys: list[str], partition_by: str | None = None) -> int:
    """Insert rows whose key is not in the table yet; never update, never duplicate.

    Returns how many rows were actually inserted, read from the Delta transaction log.
    Running it twice with the same rows inserts nothing the second time (idempotent).
    """
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return spark.read.format("delta").load(path).count()

    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    table = DeltaTable.forPath(spark, path)
    table.alias("t").merge(df.alias("s"), condition).whenNotMatchedInsertAll().execute()
    # Every Delta commit records metrics; the latest one is this MERGE.
    metrics = table.history(1).select("operationMetrics").first()[0] or {}
    return int(metrics.get("numTargetRowsInserted", 0))
