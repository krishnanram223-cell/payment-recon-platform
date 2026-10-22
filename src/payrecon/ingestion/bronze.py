"""Bronze ingestion for PSP clearing and settlement files.

For every file in data/landing/{transactions,settlements}/ this job:

  1. checks the file name  (PSP01_TXN_20261020_001.csv)
  2. checks the control file: it exists, sha256 matches, record count matches
  3. skips files already ingested successfully (replay guard, keyed on sha256)
  4. checks the CSV header against the data contract
  5. reads every column as a STRING (Bronze keeps raw values so that invalid
     data can be quarantined with a reason in Silver, instead of silently
     becoming null here)
  6. adds audit columns and writes to a Delta table, atomically replacing any
     rows from an earlier interrupted attempt for the same file
  7. records the outcome in the audit table lakehouse/audit/processed_files

Usage:
  python -m payrecon.ingestion.bronze            # uses ./data
  python -m payrecon.ingestion.bronze --data-dir /some/other/dir
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.schemas.contracts import SETTLEMENT_FIELDS, TRANSACTION_FIELDS, field_names

log = logging.getLogger("payrecon.bronze")

FILE_NAME_PATTERN = re.compile(
    r"^(?P<psp>PSP\d{2})_(?P<kind>TXN|STL)_(?P<date>\d{8})_(?P<seq>\d{3})\.csv$"
)

SUCCESS = "SUCCESS"
SKIPPED = "SKIPPED_ALREADY_PROCESSED"
REJECTED = "REJECTED"
FAILED = "FAILED"


@dataclass(frozen=True)
class FileKind:
    code: str
    landing_subdir: str
    bronze_table: str
    columns: tuple[str, ...]


FILE_KINDS = {
    "TXN": FileKind("TXN", "transactions", "clearing_transactions", tuple(field_names(TRANSACTION_FIELDS))),
    "STL": FileKind("STL", "settlements", "settlements", tuple(field_names(SETTLEMENT_FIELDS))),
}

AUDIT_TABLE = "processed_files"
AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("file_type", StringType(), False),
        StructField("file_sha256", StringType(), True),
        StructField("psp_code", StringType(), True),
        StructField("business_date", DateType(), True),
        StructField("expected_records", LongType(), True),
        StructField("ingested_records", LongType(), True),
        StructField("status", StringType(), False),
        StructField("reason", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
    ]
)


@dataclass
class FileResult:
    file_name: str
    file_type: str
    status: str = REJECTED
    reason: str | None = None
    psp_code: str | None = None
    business_date: date | None = None
    file_sha256: str | None = None
    expected_records: int | None = None
    ingested_records: int | None = None


# --------------------------------------------------------------------------- helpers
def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh), [])


def raw_schema(columns: tuple[str, ...]) -> StructType:
    """Every column as a nullable string: Bronze stores what the PSP sent."""
    return StructType([StructField(name, StringType(), True) for name in columns])


def already_processed(spark: SparkSession, audit_path: str, file_sha256: str) -> bool:
    if not DeltaTable.isDeltaTable(spark, audit_path):
        return False
    return (
        spark.read.format("delta")
        .load(audit_path)
        .filter((F.col("file_sha256") == file_sha256) & (F.col("status") == SUCCESS))
        .limit(1)
        .count()
        > 0
    )


def write_bronze(df: DataFrame, table_path: str, business_date: date, file_sha256: str) -> None:
    """Write one file's rows to Bronze in a single atomic Delta commit.

    replaceWhere replaces exactly the rows of *this* file. If an earlier attempt
    crashed after writing Bronze but before logging success, the retry overwrites
    those rows instead of duplicating them.
    """
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, table_path):
        df.write.format("delta").partitionBy("_business_date").save(table_path)
        return
    predicate = (
        f"_business_date = DATE'{business_date.isoformat()}' "
        f"AND _source_file_sha256 = '{file_sha256}'"
    )
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(table_path)


def log_result(spark: SparkSession, paths: Paths, run_id: str, result: FileResult, started_at: datetime) -> None:
    row = (
        run_id,
        result.file_name,
        result.file_type,
        result.file_sha256,
        result.psp_code,
        result.business_date,
        result.expected_records,
        result.ingested_records,
        result.status,
        result.reason,
        started_at,
        datetime.now(timezone.utc),
    )
    spark.createDataFrame([row], AUDIT_SCHEMA).write.format("delta").mode("append").save(
        paths.audit(AUDIT_TABLE)
    )


# --------------------------------------------------------------------------- core
def ingest_file(spark: SparkSession, paths: Paths, csv_path: Path, kind: FileKind, run_id: str) -> FileResult:
    result = FileResult(file_name=csv_path.name, file_type=kind.code)

    match = FILE_NAME_PATTERN.match(csv_path.name)
    if not match or match["kind"] != kind.code:
        result.reason = "INVALID_FILE_NAME"
        return result
    result.psp_code = match["psp"]
    result.business_date = datetime.strptime(match["date"], "%Y%m%d").date()

    ctl_path = csv_path.with_suffix(".ctl")
    if not ctl_path.exists():
        result.reason = "MISSING_CONTROL_FILE"
        return result
    try:
        control = json.loads(ctl_path.read_text(encoding="utf-8"))
        result.expected_records = int(control["record_count"])
        expected_sha = str(control["sha256"])
    except (ValueError, KeyError, TypeError):
        result.reason = "INVALID_CONTROL_FILE"
        return result

    result.file_sha256 = sha256_of(csv_path)
    if result.file_sha256 != expected_sha:
        result.reason = "CHECKSUM_MISMATCH"
        return result

    if already_processed(spark, paths.audit(AUDIT_TABLE), result.file_sha256):
        result.status = SKIPPED
        return result

    if read_header(csv_path) != list(kind.columns):
        result.reason = "INVALID_HEADER"
        return result

    raw_df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")  # a row with the wrong number of fields fails the file
        .schema(raw_schema(kind.columns))
        .csv(str(csv_path))
    )
    actual_records = raw_df.count()
    if actual_records != result.expected_records:
        result.reason = "RECORD_COUNT_MISMATCH"
        result.ingested_records = actual_records
        return result

    bronze_df = (
        raw_df.withColumn("_source_channel", F.lit("BATCH"))
        .withColumn("_source_file", F.lit(csv_path.name))
        .withColumn("_source_file_sha256", F.lit(result.file_sha256))
        .withColumn("_file_psp_code", F.lit(result.psp_code))
        .withColumn("_business_date", F.lit(result.business_date))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    write_bronze(bronze_df, paths.bronze(kind.bronze_table), result.business_date, result.file_sha256)

    result.status = SUCCESS
    result.ingested_records = actual_records
    return result


def run_bronze(spark: SparkSession, paths: Paths, run_id: str | None = None) -> list[FileResult]:
    run_id = run_id or uuid.uuid4().hex
    results: list[FileResult] = []
    for kind in FILE_KINDS.values():
        for csv_path in sorted((paths.landing / kind.landing_subdir).glob("*.csv")):
            started_at = datetime.now(timezone.utc)
            try:
                result = ingest_file(spark, paths, csv_path, kind, run_id)
            except Exception as exc:  # one bad file must not stop the others
                log.exception("Failed to ingest %s", csv_path.name)
                result = FileResult(csv_path.name, kind.code, FAILED, f"{type(exc).__name__}: {exc}"[:500])
            log_result(spark, paths, run_id, result, started_at)
            log.info("%-28s %-26s %s", result.file_name, result.status, result.reason or "")
            results.append(result)
    return results


def print_summary(results: list[FileResult]) -> None:
    print(f"\n{'FILE':<28} {'STATUS':<26} {'ROWS':>6}  REASON")
    for r in results:
        rows = "" if r.ingested_records is None else str(r.ingested_records)
        print(f"{r.file_name:<28} {r.status:<26} {rows:>6}  {r.reason or ''}")
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    print("\nTotals:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Ingest PSP files into Bronze Delta tables")
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()

    spark = get_spark("payrecon-bronze")
    try:
        results = run_bronze(spark, paths)
    finally:
        spark.stop()

    print_summary(results)
    # Non-zero exit code tells a scheduler (Databricks Workflows, cron, CI) that something needs attention.
    return 1 if any(r.status in (REJECTED, FAILED) for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
