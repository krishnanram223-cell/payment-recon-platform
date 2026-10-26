import hashlib
import json
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from payrecon.common.paths import Paths
from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.ingestion.bronze import AUDIT_TABLE, run_bronze, write_bronze

TXN_TABLE = "clearing_transactions"
STL_TABLE = "settlements"


def make_data(tmp_path: Path, days: int = 1) -> Paths:
    generate(GeneratorConfig(start_date=date(2026, 10, 19), days=days, txns_per_file=40, seed=11, out_dir=tmp_path))
    return Paths(tmp_path)


def bronze(spark, paths: Paths, table: str):
    return spark.read.format("delta").load(paths.bronze(table))


def control_total(paths: Paths, subdir: str) -> int:
    return sum(json.loads(p.read_text())["record_count"] for p in (paths.landing / subdir).glob("*.ctl"))


def rewrite_control(csv_path: Path, **overrides) -> None:
    """Make the .ctl consistent with the (modified) CSV, then apply overrides."""
    ctl = csv_path.with_suffix(".ctl")
    data = json.loads(ctl.read_text())
    data["sha256"] = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    data.update(overrides)
    ctl.write_text(json.dumps(data))


def test_ingests_all_files_with_audit_columns(spark, tmp_path):
    paths = make_data(tmp_path, days=2)
    results = run_bronze(spark, paths)

    assert len(results) == 12  # 3 PSPs x 2 days x (clearing + settlement)
    assert {r.status for r in results} == {"SUCCESS"}

    txn = bronze(spark, paths, TXN_TABLE)
    assert txn.count() == control_total(paths, "transactions")
    assert bronze(spark, paths, STL_TABLE).count() == control_total(paths, "settlements")

    for column in ("_source_file", "_source_file_sha256", "_business_date", "_run_id", "_ingest_ts"):
        assert txn.filter(F.col(column).isNull()).count() == 0, column
    assert [r[0] for r in txn.select("_source_channel").distinct().collect()] == ["BATCH"]

    audit = spark.read.format("delta").load(paths.audit(AUDIT_TABLE))
    assert audit.filter(F.col("status") == "SUCCESS").count() == 12


def test_rerun_is_skipped_and_does_not_duplicate(spark, tmp_path):
    paths = make_data(tmp_path)
    run_bronze(spark, paths)
    before = bronze(spark, paths, TXN_TABLE).count()

    second = run_bronze(spark, paths)

    assert {r.status for r in second} == {"SKIPPED_ALREADY_PROCESSED"}
    assert bronze(spark, paths, TXN_TABLE).count() == before


def test_tampered_file_is_rejected(spark, tmp_path):
    paths = make_data(tmp_path)
    target = next((paths.landing / "transactions").glob("PSP01_*.csv"))
    lines = target.read_text().splitlines()
    target.write_text("\n".join(lines + [lines[-1]]) + "\n")  # extra row, control file not updated

    results = {r.file_name: r for r in run_bronze(spark, paths)}

    assert results[target.name].status == "REJECTED"
    assert results[target.name].reason == "CHECKSUM_MISMATCH"
    assert bronze(spark, paths, TXN_TABLE).filter(F.col("_source_file") == target.name).count() == 0


def test_header_mismatch_is_rejected(spark, tmp_path):
    paths = make_data(tmp_path)
    target = next((paths.landing / "settlements").glob("PSP02_*.csv"))
    target.write_text(target.read_text().replace("gross_amount", "gross_amt", 1))
    rewrite_control(target)  # checksum is valid, only the header is wrong

    results = {r.file_name: r for r in run_bronze(spark, paths)}

    assert results[target.name].reason == "INVALID_HEADER"


def test_record_count_mismatch_is_rejected(spark, tmp_path):
    paths = make_data(tmp_path)
    target = next((paths.landing / "transactions").glob("PSP03_*.csv"))
    expected = json.loads(target.with_suffix(".ctl").read_text())["record_count"]
    rewrite_control(target, record_count=expected + 1)

    results = {r.file_name: r for r in run_bronze(spark, paths)}

    assert results[target.name].reason == "RECORD_COUNT_MISMATCH"


def test_bronze_write_is_idempotent_after_crash(spark, tmp_path):
    paths = make_data(tmp_path)
    run_bronze(spark, paths)
    table = paths.bronze(TXN_TABLE)
    total_before = spark.read.format("delta").load(table).count()

    one_file = spark.read.format("delta").load(table).filter(F.col("_source_file").startswith("PSP01_"))
    one_file = spark.createDataFrame(one_file.collect(), one_file.schema)  # materialise before rewriting
    first = one_file.first()

    # Simulate a retry after a crash: the same file's rows are written again.
    write_bronze(one_file, table, first["_business_date"], first["_source_file_sha256"])

    assert spark.read.format("delta").load(table).count() == total_before


def test_control_file_name_mismatch_is_rejected(spark, tmp_path):
    paths = make_data(tmp_path)
    target = next((paths.landing / "transactions").glob("PSP01_*.csv"))
    rewrite_control(target, file_name="PSP09_TXN_20991231_001.csv")

    results = {r.file_name: r for r in run_bronze(spark, paths)}

    assert results[target.name].status == "REJECTED"
    assert results[target.name].reason == "CONTROL_FILE_NAME_MISMATCH"
    assert (
        bronze(spark, paths, TXN_TABLE)
        .filter(F.col("_source_file") == target.name)
        .count()
        == 0
    )
