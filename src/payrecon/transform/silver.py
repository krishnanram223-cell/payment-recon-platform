"""Silver layer: validate, type and deduplicate Bronze rows.

For every Bronze file not yet processed into Silver, this job:

  1. validates each row against business rules; the FIRST failing rule
     becomes the row's reject reason
  2. converts valid rows to real types (Decimal money, timestamps, dates)
  3. deduplicates on the business key:
       - extra copies inside the batch  -> DUPLICATE_IN_FILE or CROSS_FILE_DUPLICATE
       - key already in Silver from another file -> CROSS_FILE_DUPLICATE
       - key already in Silver from THIS file (a retry) -> ignored, already loaded
  4. MERGEs new rows into the Silver table (insert only, never duplicates)
  5. replaces this batch's rows in the quarantine table (idempotent)
  6. logs one audit row per file in lakehouse/audit/silver_processed_files

Usage:
  python -m payrecon.transform.silver
  python -m payrecon.transform.silver --data-dir /some/other/dir
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from payrecon.common.paths import Paths, default_paths
from payrecon.common.spark import get_spark
from payrecon.ingestion.bronze import FILE_KINDS
from payrecon.schemas.contracts import (
    APPROVED_RESPONSE_CODE,
    MASKED_PAN_PATTERN,
    SETTLEMENT_FIELDS,
    TRANSACTION_FIELDS,
    VALID_CARD_SCHEMES,
    VALID_CURRENCIES,
    VALID_ENTRY_MODES,
    VALID_TRANSACTION_TYPES,
    mandatory_field_names,
)

log = logging.getLogger("payrecon.silver")

SILVER_AUDIT_TABLE = "silver_processed_files"

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
POSITIVE_AMOUNT_PATTERN = r"^[0-9]+(\.[0-9]{1,2})?$"
SIGNED_AMOUNT_PATTERN = r"^-?[0-9]+(\.[0-9]{1,2})?$"
DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
RESPONSE_CODE_PATTERN = r"^[0-9]{2}$"

# Row outcomes
INSERT = "INSERT"
QUARANTINE = "QUARANTINE"
ALREADY_LOADED = "ALREADY_LOADED"

# Bronze audit columns carried into quarantine, so a rejected row stays traceable
BRONZE_AUDIT_COLUMNS = (
    "_source_channel",
    "_source_file",
    "_source_file_sha256",
    "_file_psp_code",
    "_business_date",
    "_run_id",
    "_ingest_ts",
)


# --------------------------------------------------------------------------- small helpers
def money(column: str) -> Column:
    """Text -> DECIMAL(18,2); NULL instead of an error when it cannot be converted."""
    return F.expr(f"try_cast({column} AS DECIMAL(18,2))")


def any_missing(columns: list[str]) -> Column:
    """True when at least one of the columns is NULL or only spaces."""
    checks = [F.col(c).isNull() | (F.trim(F.col(c)) == "") for c in columns]
    result = checks[0]
    for check in checks[1:]:
        result = result | check
    return result


def first_failing(rules: list[tuple[str, Column]]) -> Column:
    """CASE WHEN rule1 THEN reason1 WHEN rule2 THEN reason2 ... END (NULL = valid)."""
    expression = None
    for reason, condition in rules:
        expression = (
            F.when(condition, F.lit(reason))
            if expression is None
            else expression.when(condition, F.lit(reason))
        )
    return expression


def load_merchants(spark: SparkSession, paths: Paths) -> DataFrame:
    path = paths.reference / "merchants.csv"
    if not path.exists():
        raise FileNotFoundError(f"Merchant reference file not found: {path}")
    schema = StructType(
        [StructField(c, StringType(), True) for c in ("merchant_id", "merchant_name", "settlement_currency")]
    )
    return (
        spark.read.option("header", True)
        .schema(schema)
        .csv(str(path))
        .select("merchant_id")
        .dropDuplicates(["merchant_id"])
        .withColumn("_merchant_known", F.lit(True))
    )


# --------------------------------------------------------------------------- validation rules
def validate_transactions(df: DataFrame, merchants: DataFrame) -> DataFrame:
    """Add typed helper columns and _reject_reason (NULL when the row is valid)."""
    df = (
        df.withColumn("_t_amount", money("amount"))
        .withColumn("_t_ts", F.try_to_timestamp(F.col("transaction_timestamp"), F.lit(TIMESTAMP_FORMAT)))
        .join(merchants, on="merchant_id", how="left")
    )
    rules = [
        ("MISSING_MANDATORY_FIELD", any_missing(mandatory_field_names(TRANSACTION_FIELDS))),
        ("PSP_CODE_MISMATCH", F.col("psp_code") != F.col("_file_psp_code")),
        ("INVALID_TRANSACTION_TYPE", ~F.col("transaction_type").isin(*sorted(VALID_TRANSACTION_TYPES))),
        (
            "INVALID_AMOUNT",
            ~F.col("amount").rlike(POSITIVE_AMOUNT_PATTERN)
            | F.col("_t_amount").isNull()
            | (F.col("_t_amount") <= 0),
        ),
        ("INVALID_CURRENCY", ~F.col("currency").isin(*sorted(VALID_CURRENCIES))),
        ("INVALID_TIMESTAMP", F.col("_t_ts").isNull()),
        ("INVALID_CARD_SCHEME", ~F.col("card_scheme").isin(*sorted(VALID_CARD_SCHEMES))),
        ("INVALID_MASKED_PAN", ~F.col("masked_pan").rlike(MASKED_PAN_PATTERN)),
        ("INVALID_RESPONSE_CODE", ~F.col("response_code").rlike(RESPONSE_CODE_PATTERN)),
        ("INVALID_ENTRY_MODE", ~F.col("entry_mode").isin(*sorted(VALID_ENTRY_MODES))),
        ("UNKNOWN_MERCHANT", F.col("_merchant_known").isNull()),
    ]
    return df.withColumn("_reject_reason", first_failing(rules)).drop("_merchant_known")


def validate_settlements(df: DataFrame, merchants: DataFrame) -> DataFrame:  # noqa: ARG001 - same signature
    df = (
        df.withColumn("_t_gross", money("gross_amount"))
        .withColumn("_t_interchange", money("interchange_fee"))
        .withColumn("_t_scheme", money("scheme_fee"))
        .withColumn("_t_net", money("net_amount"))
        .withColumn("_t_date", F.expr("try_cast(settlement_date AS DATE)"))
    )
    rules = [
        ("MISSING_MANDATORY_FIELD", any_missing(mandatory_field_names(SETTLEMENT_FIELDS))),
        ("PSP_CODE_MISMATCH", F.col("psp_code") != F.col("_file_psp_code")),
        ("INVALID_SETTLEMENT_DATE", ~F.col("settlement_date").rlike(DATE_PATTERN) | F.col("_t_date").isNull()),
        (
            "INVALID_AMOUNT",
            ~F.col("gross_amount").rlike(SIGNED_AMOUNT_PATTERN)
            | ~F.col("interchange_fee").rlike(POSITIVE_AMOUNT_PATTERN)
            | ~F.col("scheme_fee").rlike(POSITIVE_AMOUNT_PATTERN)
            | ~F.col("net_amount").rlike(SIGNED_AMOUNT_PATTERN)
            | F.col("_t_gross").isNull()
            | F.col("_t_interchange").isNull()
            | F.col("_t_scheme").isNull()
            | F.col("_t_net").isNull(),
        ),
        ("INVALID_CURRENCY", ~F.col("currency").isin(*sorted(VALID_CURRENCIES))),
        (
            "NET_AMOUNT_MISMATCH",
            F.col("_t_net") != F.col("_t_gross") - F.col("_t_interchange") - F.col("_t_scheme"),
        ),
    ]
    return df.withColumn("_reject_reason", first_failing(rules))


# --------------------------------------------------------------------------- typed Silver rows
def to_silver_transactions(df: DataFrame, run_id: str) -> DataFrame:
    amount = F.col("_t_amount")
    return df.select(
        "psp_code",
        "psp_transaction_id",
        "merchant_id",
        "terminal_id",
        "transaction_type",
        F.col("_t_ts").alias("transaction_ts"),
        F.to_date("_t_ts").alias("transaction_date"),
        amount.alias("amount"),
        F.when(F.col("transaction_type") == "REFUND", -amount).otherwise(amount).alias("signed_amount"),
        "currency",
        "card_scheme",
        "masked_pan",
        "auth_code",
        "response_code",
        (F.col("response_code") == APPROVED_RESPONSE_CODE).alias("is_approved"),
        "entry_mode",
        (F.to_date("_t_ts") < F.col("_business_date")).alias("is_late_arrival"),
        "_business_date",
        "_source_channel",
        "_source_file",
        "_source_file_sha256",
        F.col("_run_id").alias("_bronze_run_id"),
        F.lit(run_id).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


def to_silver_settlements(df: DataFrame, run_id: str) -> DataFrame:
    return df.select(
        "settlement_id",
        "psp_code",
        "psp_transaction_id",
        "merchant_id",
        F.col("_t_date").alias("settlement_date"),
        F.col("_t_gross").alias("gross_amount"),
        F.col("_t_interchange").alias("interchange_fee"),
        F.col("_t_scheme").alias("scheme_fee"),
        F.col("_t_net").alias("net_amount"),
        "currency",
        "_business_date",
        "_source_channel",
        "_source_file",
        "_source_file_sha256",
        F.col("_run_id").alias("_bronze_run_id"),
        F.lit(run_id).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_processed_ts"),
    )


# --------------------------------------------------------------------------- entity configuration
@dataclass(frozen=True)
class Entity:
    code: str  # TXN or STL, same codes as Bronze
    silver_table: str
    quarantine_table: str
    key_columns: tuple[str, ...]
    validate: Callable[[DataFrame, DataFrame], DataFrame]
    to_silver: Callable[[DataFrame, str], DataFrame]


ENTITIES = (
    Entity(
        code="TXN",
        silver_table="transactions",
        quarantine_table="quarantine_transactions",
        key_columns=("psp_code", "psp_transaction_id"),
        validate=validate_transactions,
        to_silver=to_silver_transactions,
    ),
    Entity(
        code="STL",
        silver_table="settlements",
        quarantine_table="quarantine_settlements",
        key_columns=("settlement_id",),
        validate=validate_settlements,
        to_silver=to_silver_settlements,
    ),
)


@dataclass
class FileSummary:
    entity: str
    file_name: str
    input_rows: int
    inserted_rows: int
    quarantined_rows: int
    already_loaded_rows: int


# --------------------------------------------------------------------------- core steps
def new_bronze_rows(spark: SparkSession, paths: Paths, entity: Entity) -> DataFrame | None:
    """Bronze rows from files that have not been processed into Silver yet."""
    bronze_path = paths.bronze(FILE_KINDS[entity.code].bronze_table)
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return None
    bronze = spark.read.format("delta").load(bronze_path)
    audit_path = paths.audit(SILVER_AUDIT_TABLE)
    if DeltaTable.isDeltaTable(spark, audit_path):
        done = (
            spark.read.format("delta")
            .load(audit_path)
            .filter(F.col("entity") == entity.code)
            .select(F.col("file_sha256").alias("_source_file_sha256"))
            .distinct()
        )
        bronze = bronze.join(done, on="_source_file_sha256", how="left_anti")
    return bronze


def classify(validated: DataFrame, entity: Entity, silver_path: str) -> DataFrame:
    """Give every row an _outcome (INSERT / QUARANTINE / ALREADY_LOADED) and, if rejected, a reason."""
    spark = validated.sparkSession
    keys = list(entity.key_columns)

    invalid = validated.filter(F.col("_reject_reason").isNotNull()).withColumn("_outcome", F.lit(QUARANTINE))
    valid = validated.filter(F.col("_reject_reason").isNull())

    # Rank copies of the same business key inside this batch: oldest file first.
    window = Window.partitionBy(*keys).orderBy("_business_date", "_source_file")
    ranked = valid.withColumn("_rn", F.row_number().over(window)).withColumn(
        "_first_file", F.first("_source_file").over(window)
    )

    # Which keys are already in Silver, and from which file?
    if DeltaTable.isDeltaTable(spark, silver_path):
        existing = (
            spark.read.format("delta")
            .load(silver_path)
            .select(*keys, F.col("_source_file_sha256").alias("_existing_sha"))
        )
        ranked = ranked.join(existing, on=keys, how="left")
    else:
        ranked = ranked.withColumn("_existing_sha", F.lit(None).cast("string"))

    is_extra_copy = F.col("_rn") > 1
    in_silver_from_other_file = F.col("_existing_sha").isNotNull() & (
        F.col("_existing_sha") != F.col("_source_file_sha256")
    )

    classified_valid = ranked.withColumn(
        "_outcome",
        F.when(is_extra_copy, F.lit(QUARANTINE))
        .when(F.col("_existing_sha").isNull(), F.lit(INSERT))
        .when(in_silver_from_other_file, F.lit(QUARANTINE))
        .otherwise(F.lit(ALREADY_LOADED)),
    ).withColumn(
        "_reject_reason",
        F.when(
            is_extra_copy,
            F.when(F.col("_source_file") == F.col("_first_file"), F.lit("DUPLICATE_IN_FILE")).otherwise(
                F.lit("CROSS_FILE_DUPLICATE")
            ),
        ).when(in_silver_from_other_file, F.lit("CROSS_FILE_DUPLICATE")),
    )

    return classified_valid.drop("_rn", "_first_file", "_existing_sha").unionByName(
        invalid, allowMissingColumns=True
    )


def merge_into_silver(df: DataFrame, path: str, keys: tuple[str, ...]) -> None:
    """Insert rows whose business key is not in the table yet. Never updates, never duplicates."""
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("_business_date").save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), condition).whenNotMatchedInsertAll().execute()


def replace_quarantine(df: DataFrame, path: str, file_shas: list[str]) -> None:
    """Replace the quarantine rows of exactly these files in one atomic commit."""
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("_business_date").save(path)
        return
    in_list = ", ".join(f"'{sha}'" for sha in file_shas)
    df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"_source_file_sha256 IN ({in_list})"
    ).save(path)


def process_entity(
    spark: SparkSession, paths: Paths, entity: Entity, merchants: DataFrame, run_id: str
) -> list[FileSummary]:
    new_rows = new_bronze_rows(spark, paths, entity)
    if new_rows is None or new_rows.isEmpty():
        log.info("%s: no new Bronze files", entity.code)
        return []

    file_shas = [row[0] for row in new_rows.select("_source_file_sha256").distinct().collect()]
    silver_path = paths.silver(entity.silver_table)

    classified = classify(entity.validate(new_rows, merchants), entity, silver_path)
    # Freeze the result now. Spark is lazy: without this, the steps below would
    # re-run classify() AFTER the MERGE and see their own new rows as duplicates.
    classified = classified.localCheckpoint()

    to_insert = entity.to_silver(classified.filter(F.col("_outcome") == INSERT), run_id)
    merge_into_silver(to_insert, silver_path, entity.key_columns)

    raw_columns = list(FILE_KINDS[entity.code].columns) + list(BRONZE_AUDIT_COLUMNS)
    quarantine = classified.filter(F.col("_outcome") == QUARANTINE).select(
        *raw_columns,
        "_reject_reason",
        F.lit(run_id).alias("_silver_run_id"),
        F.current_timestamp().alias("_quarantined_ts"),
    )
    replace_quarantine(quarantine, paths.silver(entity.quarantine_table), file_shas)

    def count_of(outcome: str) -> Column:
        return F.sum(F.when(F.col("_outcome") == outcome, 1).otherwise(0))

    audit = classified.groupBy("_source_file", "_source_file_sha256", "_business_date").agg(
        F.count("*").alias("input_rows"),
        count_of(INSERT).alias("inserted_rows"),
        count_of(QUARANTINE).alias("quarantined_rows"),
        count_of(ALREADY_LOADED).alias("already_loaded_rows"),
    ).select(
        F.lit(run_id).alias("run_id"),
        F.lit(entity.code).alias("entity"),
        F.col("_source_file").alias("file_name"),
        F.col("_source_file_sha256").alias("file_sha256"),
        F.col("_business_date").alias("business_date"),
        "input_rows",
        "inserted_rows",
        "quarantined_rows",
        "already_loaded_rows",
        F.current_timestamp().alias("processed_at"),
    )
    audit.write.format("delta").mode("append").save(paths.audit(SILVER_AUDIT_TABLE))

    return [
        FileSummary(
            entity.code,
            r["file_name"],
            r["input_rows"],
            r["inserted_rows"],
            r["quarantined_rows"],
            r["already_loaded_rows"],
        )
        for r in audit.orderBy("file_name").collect()
    ]


def run_silver(spark: SparkSession, paths: Paths, run_id: str | None = None) -> list[FileSummary]:
    run_id = run_id or uuid.uuid4().hex
    merchants = load_merchants(spark, paths)
    summaries: list[FileSummary] = []
    for entity in ENTITIES:
        summaries.extend(process_entity(spark, paths, entity, merchants, run_id))
    return summaries


def print_summary(summaries: list[FileSummary]) -> None:
    if not summaries:
        print("\nNo new Bronze files to process.")
        return
    print(f"\n{'FILE':<28} {'INPUT':>6} {'INSERTED':>9} {'QUARANTINED':>12} {'ALREADY':>8}")
    for s in summaries:
        print(
            f"{s.file_name:<28} {s.input_rows:>6} {s.inserted_rows:>9} "
            f"{s.quarantined_rows:>12} {s.already_loaded_rows:>8}"
        )
    print(
        f"\nTotals: input={sum(s.input_rows for s in summaries)}, "
        f"inserted={sum(s.inserted_rows for s in summaries)}, "
        f"quarantined={sum(s.quarantined_rows for s in summaries)}"
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Validate, type and deduplicate Bronze rows into Silver")
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    paths = Paths(args.data_dir.resolve()) if args.data_dir else default_paths()

    spark = get_spark("payrecon-silver")
    try:
        summaries = run_silver(spark, paths)
    finally:
        spark.stop()

    print_summary(summaries)
    # Quarantined rows are expected business outcomes, not job failures.
    # A real failure raises an exception, and Python then exits with code 1.
    return 0


if __name__ == "__main__":
    sys.exit(main())
