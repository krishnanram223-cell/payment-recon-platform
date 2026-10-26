import json
import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, TimestampType

from payrecon.common.paths import Paths
from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.ingestion.bronze import FILE_KINDS, raw_schema, run_bronze
from payrecon.transform.silver import (
    SILVER_AUDIT_TABLE,
    load_merchants,
    run_silver,
    validate_transactions,
)

# Reasons the generator plants that Silver must catch, row by row.
SILVER_DEFECTS = (
    "MISSING_MANDATORY_FIELD",
    "INVALID_CURRENCY",
    "INVALID_AMOUNT",
    "INVALID_TIMESTAMP",
    "DUPLICATE_IN_FILE",
    "CROSS_FILE_DUPLICATE",
)


def table(spark, path: str):
    return spark.read.format("delta").load(path)


def count_or_zero(spark, path: str) -> int:
    return table(spark, path).count() if DeltaTable.isDeltaTable(spark, path) else 0


def build_pipeline(spark, root: Path, days: int, txns_per_file: int):
    cfg = GeneratorConfig(start_date=date(2026, 10, 19), days=days, txns_per_file=txns_per_file, seed=21, out_dir=root)
    manifests = [json.loads(p.read_text()) for p in generate(cfg)]
    paths = Paths(root)
    run_bronze(spark, paths)
    run_silver(spark, paths)
    return paths, manifests


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    """Two days, 200 rows per file (2 rows per defect type), loaded through Bronze and Silver once."""
    return build_pipeline(spark, tmp_path_factory.mktemp("silver"), days=2, txns_per_file=200)


def ids(df) -> set[str]:
    return {r[0] for r in df.select("psp_transaction_id").collect()}


# --------------------------------------------------------------------------- against the ground truth
def test_quarantine_matches_planted_defects_exactly(spark, pipeline):
    paths, manifests = pipeline
    quarantine = table(spark, paths.silver("quarantine_transactions"))

    expected_total = 0
    for manifest in manifests:
        for info in manifest["psps"].values():
            in_file = quarantine.filter(F.col("_source_file") == info["transaction_file"])
            for reason in SILVER_DEFECTS:
                expected = set(info["anomalies"].get(reason, []))
                expected_total += len(expected)
                actual = ids(in_file.filter(F.col("_reject_reason") == reason))
                assert actual == expected, f"{info['transaction_file']} {reason}"

    assert quarantine.count() == expected_total, "no row may be quarantined for an unplanned reason"


def test_silver_has_exactly_the_valid_unique_rows(spark, pipeline):
    paths, manifests = pipeline
    silver = table(spark, paths.silver("transactions"))

    expected = sum(info["expected_new_unique_valid"] for m in manifests for info in m["psps"].values())
    assert silver.count() == expected
    assert silver.select("psp_code", "psp_transaction_id").distinct().count() == expected


def test_late_arrivals_are_flagged_not_rejected(spark, pipeline):
    paths, manifests = pipeline
    silver = table(spark, paths.silver("transactions"))
    for manifest in manifests:
        for info in manifest["psps"].values():
            flagged = silver.filter(
                (F.col("_source_file") == info["transaction_file"]) & F.col("is_late_arrival")
            )
            assert ids(flagged) == set(info["anomalies"]["LATE_ARRIVAL"])


def test_silver_columns_are_typed_and_refunds_are_negative(spark, pipeline):
    paths, _ = pipeline
    silver = table(spark, paths.silver("transactions"))
    fields = {f.name: f.dataType for f in silver.schema.fields}
    assert fields["amount"] == DecimalType(18, 2)
    assert fields["signed_amount"] == DecimalType(18, 2)
    assert fields["transaction_ts"] == TimestampType()

    refunds = silver.filter(F.col("transaction_type") == "REFUND")
    purchases = silver.filter(F.col("transaction_type") == "PURCHASE")
    assert refunds.count() > 0
    assert refunds.filter(F.col("signed_amount") >= 0).count() == 0
    assert purchases.filter(F.col("signed_amount") <= 0).count() == 0


def test_all_settlements_are_valid_and_unique(spark, pipeline):
    paths, _ = pipeline
    bronze_rows = count_or_zero(spark, paths.bronze(FILE_KINDS["STL"].bronze_table))
    silver = table(spark, paths.silver("settlements"))
    assert silver.count() == bronze_rows
    assert silver.select("settlement_id").distinct().count() == bronze_rows
    assert count_or_zero(spark, paths.silver("quarantine_settlements")) == 0


def test_rerun_processes_nothing_and_changes_nothing(spark, pipeline):
    paths, _ = pipeline
    before = (
        count_or_zero(spark, paths.silver("transactions")),
        count_or_zero(spark, paths.silver("quarantine_transactions")),
    )

    summaries = run_silver(spark, paths)

    assert summaries == []
    after = (
        count_or_zero(spark, paths.silver("transactions")),
        count_or_zero(spark, paths.silver("quarantine_transactions")),
    )
    assert after == before


# --------------------------------------------------------------------------- failure recovery
def test_retry_after_crash_does_not_duplicate_anything(spark, tmp_path):
    paths, _ = build_pipeline(spark, tmp_path, days=1, txns_per_file=60)
    silver_before = count_or_zero(spark, paths.silver("transactions"))
    quarantine_before = count_or_zero(spark, paths.silver("quarantine_transactions"))

    # Simulate a crash after the MERGE but before the audit rows were written:
    # with no audit, Silver must believe it never processed these files.
    shutil.rmtree(paths.audit(SILVER_AUDIT_TABLE))
    summaries = run_silver(spark, paths)

    assert summaries, "the files must be processed again"
    assert sum(s.inserted_rows for s in summaries) == 0
    assert sum(s.already_loaded_rows for s in summaries) == silver_before + count_or_zero(
        spark, paths.silver("settlements")
    )
    assert count_or_zero(spark, paths.silver("transactions")) == silver_before
    assert count_or_zero(spark, paths.silver("quarantine_transactions")) == quarantine_before


# --------------------------------------------------------------------------- rule unit test
def test_each_transaction_rule_has_its_own_reason(spark, tmp_path):
    generate(GeneratorConfig(start_date=date(2026, 10, 19), days=1, txns_per_file=10, out_dir=tmp_path))
    merchants = load_merchants(spark, Paths(tmp_path))
    known_merchant = merchants.first()["merchant_id"]

    valid = {
        "psp_code": "PSP01",
        "psp_transaction_id": "PSP01-unit",
        "merchant_id": known_merchant,
        "terminal_id": "T0000101",
        "transaction_type": "PURCHASE",
        "transaction_timestamp": "2026-10-19T10:00:00Z",
        "amount": "12.50",
        "currency": "EUR",
        "card_scheme": "VISA",
        "masked_pan": "411111******1234",
        "auth_code": "123456",
        "response_code": "00",
        "entry_mode": "CHIP",
        "_file_psp_code": "PSP01",
    }
    cases = {
        None: {},
        "PSP_CODE_MISMATCH": {"_file_psp_code": "PSP02"},
        "INVALID_TRANSACTION_TYPE": {"transaction_type": "refund"},
        "INVALID_AMOUNT": {"amount": "99999999999999999999.00"},
        "INVALID_CARD_SCHEME": {"card_scheme": "DINERS"},
        "INVALID_MASKED_PAN": {"masked_pan": "4111111111111234"},
        "INVALID_RESPONSE_CODE": {"response_code": "A1"},
        "INVALID_ENTRY_MODE": {"entry_mode": "SWIPE"},
        "UNKNOWN_MERCHANT": {"merchant_id": "M99999"},
    }
    schema = raw_schema(FILE_KINDS["TXN"].columns).add(StructField("_file_psp_code", StringType(), True))
    rows = []
    for i, (reason, change) in enumerate(cases.items()):
        row = {**valid, **change, "psp_transaction_id": f"PSP01-unit-{i}"}
        rows.append(tuple(row[f.name] for f in schema.fields))

    result = validate_transactions(spark.createDataFrame(rows, schema), merchants)
    actual = {r["psp_transaction_id"]: r["_reject_reason"] for r in result.collect()}

    for i, reason in enumerate(cases):
        assert actual[f"PSP01-unit-{i}"] == reason, f"case {i}: expected {reason}"
    assert result.filter(F.col("_reject_reason").isNull()).first()["_t_amount"] == Decimal("12.50")
