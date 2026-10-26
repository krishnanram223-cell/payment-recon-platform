import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from payrecon.common.paths import Paths
from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.gold.reconciliation import (
    AMOUNT_MISMATCH,
    DUPLICATE_SETTLEMENT,
    MATCHED,
    MISSING_SETTLEMENT,
    ORPHAN_SETTLEMENT,
    PENDING_SETTLEMENT,
    build_reconciliation,
    run_gold,
)
from payrecon.ingestion.bronze import run_bronze
from payrecon.transform.silver import run_silver

START = date(2026, 10, 19)
LONG_AFTER = date(2026, 12, 31)  # every settlement window has closed
PLANTED_BREAKS = (MISSING_SETTLEMENT, AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT, ORPHAN_SETTLEMENT)


def table(spark, path: str):
    return spark.read.format("delta").load(path)


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("gold")
    cfg = GeneratorConfig(start_date=START, days=2, txns_per_file=200, seed=33, out_dir=root)
    manifests = [json.loads(p.read_text()) for p in generate(cfg)]
    paths = Paths(root)
    run_bronze(spark, paths)
    run_silver(spark, paths)
    counts = run_gold(spark, paths, as_of_date=LONG_AFTER)
    return paths, manifests, counts


def planted(manifests, break_type: str) -> int:
    return sum(len(info["anomalies"].get(break_type, [])) for m in manifests for info in m["psps"].values())


# --------------------------------------------------------------------------- against the ground truth
def test_breaks_match_planted_defects_exactly(spark, pipeline):
    paths, manifests, _ = pipeline
    recon = table(spark, paths.gold("reconciliation"))
    for manifest in manifests:
        business_date = date.fromisoformat(manifest["business_date"])
        for psp, info in manifest["psps"].items():
            day = recon.filter((F.col("psp_code") == psp) & (F.col("business_date") == business_date))
            for break_type in PLANTED_BREAKS:
                actual = {r[0] for r in day.filter(F.col("recon_status") == break_type).select("psp_transaction_id").collect()}
                assert actual == set(info["anomalies"].get(break_type, [])), f"{psp} {business_date} {break_type}"


def test_every_other_approved_transaction_is_matched(spark, pipeline):
    paths, manifests, counts = pipeline
    approved = table(spark, paths.silver("transactions")).filter(F.col("is_approved")).count()
    breaks_on_transactions = sum(planted(manifests, b) for b in (MISSING_SETTLEMENT, AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT))

    assert counts[MATCHED] == approved - breaks_on_transactions
    assert set(counts) <= {MATCHED, *PLANTED_BREAKS}, f"unexpected statuses: {set(counts)}"


def test_refunds_match_negative_settlements(spark, pipeline):
    paths, _, _ = pipeline
    matched_refunds = table(spark, paths.gold("reconciliation")).filter(
        (F.col("recon_status") == MATCHED) & (F.col("transaction_type") == "REFUND")
    )
    assert matched_refunds.count() > 0
    assert matched_refunds.filter(F.col("transaction_amount") >= 0).count() == 0
    assert matched_refunds.filter(F.col("settled_gross_amount") != F.col("transaction_amount")).count() == 0


def test_break_amounts_are_explained(spark, pipeline):
    paths, _, _ = pipeline
    recon = table(spark, paths.gold("reconciliation"))

    mismatches = recon.filter(F.col("recon_status") == AMOUNT_MISMATCH).collect()
    assert mismatches
    assert all(Decimal("0.01") <= abs(r["amount_difference"]) <= Decimal("5.00") for r in mismatches)

    duplicates = recon.filter(F.col("recon_status") == DUPLICATE_SETTLEMENT).collect()
    assert duplicates
    assert all(r["settlement_count"] == 2 for r in duplicates)
    assert all(r["amount_difference"] == r["transaction_amount"] for r in duplicates), "paid twice = one extra amount"


def test_missing_becomes_pending_inside_the_settlement_window(spark, pipeline):
    paths, manifests, _ = pipeline
    recon = build_reconciliation(
        table(spark, paths.silver("transactions")),
        table(spark, paths.silver("settlements")),
        as_of_date=START,
        window_days=2,
        run_id="unit",
    )
    statuses = {r["recon_status"]: r["count"] for r in recon.groupBy("recon_status").count().collect()}
    assert MISSING_SETTLEMENT not in statuses
    assert statuses[PENDING_SETTLEMENT] == planted(manifests, MISSING_SETTLEMENT)


# --------------------------------------------------------------------------- merchant summary
def test_merchant_summary_adds_up_to_its_sources(spark, pipeline):
    paths, manifests, counts = pipeline
    summary = table(spark, paths.gold("merchant_daily_summary"))
    totals = summary.agg(
        F.sum("transaction_count").alias("transactions"),
        F.sum("matched_count").alias("matched"),
        F.sum("orphan_settlement_count").alias("orphans"),
        F.sum("rejected_row_count").alias("rejected"),
        F.sum("settled_net_amount").alias("settled_net"),
        F.sum("break_count").alias("breaks"),
    ).first()

    silver_txn = table(spark, paths.silver("transactions"))
    silver_net = table(spark, paths.silver("settlements")).agg(F.sum("net_amount")).first()[0]
    # Rejected rows that still carry a merchant id (rows with an empty merchant id can't be attributed).
    attributable = sum(planted(manifests, r) for r in ("INVALID_CURRENCY", "INVALID_AMOUNT", "INVALID_TIMESTAMP"))

    assert totals["transactions"] == silver_txn.count()
    assert totals["matched"] == counts[MATCHED]
    assert totals["orphans"] == planted(manifests, ORPHAN_SETTLEMENT)
    assert totals["rejected"] == attributable
    assert totals["settled_net"] == silver_net
    assert totals["breaks"] == sum(planted(manifests, b) for b in PLANTED_BREAKS)
    assert summary.filter(F.col("merchant_name").isNull()).count() == 0


def test_rerun_rebuilds_gold_without_duplicating(spark, pipeline):
    paths, _, counts = pipeline
    rows_before = table(spark, paths.gold("reconciliation")).count()

    again = run_gold(spark, paths, as_of_date=LONG_AFTER)

    assert again == counts
    assert table(spark, paths.gold("reconciliation")).count() == rows_before
