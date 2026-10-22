import csv
import hashlib
import json
import re
from collections import Counter
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest

from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.schemas.contracts import (
    MASKED_PAN_PATTERN,
    SETTLEMENT_FIELDS,
    TRANSACTION_FIELDS,
    VALID_CURRENCIES,
    field_names,
)


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return reader.fieldnames, list(reader)


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("data")
    cfg = GeneratorConfig(start_date=date(2026, 10, 19), days=2, txns_per_file=200, seed=7, out_dir=out)
    manifests = [json.loads(p.read_text()) for p in generate(cfg)]
    return out, manifests


def test_headers_match_contract(generated):
    out, _ = generated
    for f in (out / "landing" / "transactions").glob("*.csv"):
        assert read_csv(f)[0] == field_names(TRANSACTION_FIELDS)
    for f in (out / "landing" / "settlements").glob("*.csv"):
        assert read_csv(f)[0] == field_names(SETTLEMENT_FIELDS)


def test_control_files_match(generated):
    out, _ = generated
    for f in (out / "landing").rglob("*.csv"):
        ctl = json.loads(f.with_suffix(".ctl").read_text())
        assert ctl["record_count"] == len(read_csv(f)[1])
        assert ctl["sha256"] == hashlib.sha256(f.read_bytes()).hexdigest()


def test_no_unmasked_card_numbers(generated):
    out, _ = generated
    for f in (out / "landing" / "transactions").glob("*.csv"):
        for row in read_csv(f)[1]:
            assert re.match(MASKED_PAN_PATTERN, row["masked_pan"])


def test_planted_quarantine_defects_are_really_invalid(generated):
    out, manifests = generated
    for m in manifests:
        for psp, info in m["psps"].items():
            rows = {r["psp_transaction_id"]: r for r in read_csv(out / "landing" / "transactions" / info["transaction_file"])[1]}
            for tid in info["anomalies"]["MISSING_MANDATORY_FIELD"]:
                assert rows[tid]["merchant_id"] == ""
            for tid in info["anomalies"]["INVALID_CURRENCY"]:
                assert rows[tid]["currency"] not in VALID_CURRENCIES
            for tid in info["anomalies"]["INVALID_AMOUNT"]:
                try:
                    assert Decimal(rows[tid]["amount"]) <= 0
                except InvalidOperation:
                    pass  # unparseable is also invalid


def test_duplicates_and_settlement_breaks(generated):
    out, manifests = generated
    all_txn_ids = set()
    for f in (out / "landing" / "transactions").glob("*.csv"):
        all_txn_ids |= {r["psp_transaction_id"] for r in read_csv(f)[1]}

    for m in manifests:
        for psp, info in m["psps"].items():
            txn_counts = Counter(r["psp_transaction_id"] for r in read_csv(out / "landing" / "transactions" / info["transaction_file"])[1])
            stl_counts = Counter(r["psp_transaction_id"] for r in read_csv(out / "landing" / "settlements" / info["settlement_file"])[1])
            a = info["anomalies"]
            assert all(txn_counts[t] == 2 for t in a["DUPLICATE_IN_FILE"])
            assert all(t not in stl_counts for t in a["MISSING_SETTLEMENT"])
            assert all(stl_counts[t] == 2 for t in a["DUPLICATE_SETTLEMENT"])
            assert all(t not in all_txn_ids for t in a["ORPHAN_SETTLEMENT"])
            quarantine_ids = {t for k in ("MISSING_MANDATORY_FIELD", "INVALID_CURRENCY", "INVALID_AMOUNT", "INVALID_TIMESTAMP") for t in a[k]}
            assert not quarantine_ids & set(stl_counts), "invalid rows must never be settled"


def test_cross_file_duplicates_only_after_first_day(generated):
    _, manifests = generated
    assert all("CROSS_FILE_DUPLICATE" not in i["anomalies"] for i in manifests[0]["psps"].values())
    assert all(i["anomalies"]["CROSS_FILE_DUPLICATE"] for i in manifests[1]["psps"].values())


def test_generation_is_deterministic(tmp_path):
    def run(sub):
        cfg = GeneratorConfig(start_date=date(2026, 10, 19), days=1, txns_per_file=50, seed=1, out_dir=tmp_path / sub)
        generate(cfg)
        return {p.name: p.read_bytes() for p in (tmp_path / sub / "landing").rglob("*.csv")}

    assert run("a") == run("b")
