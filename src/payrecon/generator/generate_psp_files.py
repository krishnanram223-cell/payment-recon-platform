"""Synthetic PSP transaction and settlement file generator.

Produces, for each PSP and business date:
  data/landing/transactions/{PSP}_TXN_{YYYYMMDD}_001.csv  (+ .ctl)
  data/landing/settlements/{PSP}_STL_{YYYYMMDD}_001.csv   (+ .ctl)
and a ground-truth manifest of every defect it injected:
  data/ground_truth/expected_{YYYYMMDD}.json

The manifest lets later milestones *prove* that validation and reconciliation
catch exactly what was planted. The pipeline must never read ground_truth/.

All data is synthetic. Card numbers are masked test BINs, never real PANs.

Usage:
  python -m payrecon.generator.generate_psp_files --start-date 2026-10-19 --days 3
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from payrecon.schemas.contracts import (
    APPROVED_RESPONSE_CODE,
    SETTLEMENT_FIELDS,
    TRANSACTION_FIELDS,
    field_names,
)

PSPS = ("PSP01", "PSP02", "PSP03")
# Well-known public *test* BIN prefixes; output is always masked.
SCHEME_BINS = {"VISA": "411111", "MASTERCARD": "555555", "AMEX": "378282"}
MERCHANT_CURRENCIES = ("EUR", "SEK", "PLN", "GBP")
ENTRY_MODES = ("CHIP", "CONTACTLESS", "ECOM", "MAGSTRIPE")
DECLINE_CODES = ("05", "51", "54")

INTERCHANGE_RATE = Decimal("0.0030")
SCHEME_RATE = Decimal("0.0010")
CENT = Decimal("0.01")

# Defect types (also used as anomaly labels in the manifest)
MISSING_MANDATORY_FIELD = "MISSING_MANDATORY_FIELD"
INVALID_CURRENCY = "INVALID_CURRENCY"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
DUPLICATE_IN_FILE = "DUPLICATE_IN_FILE"
CROSS_FILE_DUPLICATE = "CROSS_FILE_DUPLICATE"
LATE_ARRIVAL = "LATE_ARRIVAL"
MISSING_SETTLEMENT = "MISSING_SETTLEMENT"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
DUPLICATE_SETTLEMENT = "DUPLICATE_SETTLEMENT"
ORPHAN_SETTLEMENT = "ORPHAN_SETTLEMENT"

QUARANTINE_DEFECTS = (MISSING_MANDATORY_FIELD, INVALID_CURRENCY, INVALID_AMOUNT, INVALID_TIMESTAMP)


@dataclass
class GeneratorConfig:
    start_date: date
    days: int = 3
    txns_per_file: int = 500
    merchants: int = 25
    defect_rate: float = 0.01  # share of rows per defect type (min 1 each)
    seed: int = 42
    out_dir: Path = Path("data")


@dataclass
class PspDayResult:
    transaction_file: str
    settlement_file: str
    txn_rows_in_file: int
    settlement_rows_in_file: int
    expected_quarantined: int
    expected_new_unique_valid: int
    anomalies: dict[str, list[str]] = field(default_factory=dict)
    valid_rows: list[dict] = field(default_factory=list)  # carried to next day, not serialised


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def iso_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_merchants(rng: random.Random, n: int) -> dict[str, str]:
    return {f"M{i:05d}": rng.choice(MERCHANT_CURRENCIES) for i in range(1, n + 1)}


def make_transaction(rng: random.Random, psp: str, business_date: date, merchants: dict[str, str]) -> dict:
    merchant_id = rng.choice(sorted(merchants))
    scheme = rng.choices(list(SCHEME_BINS), weights=(55, 35, 10))[0]
    approved = rng.random() < 0.92
    ts = datetime.combine(business_date, time(0), tzinfo=timezone.utc) + timedelta(
        seconds=rng.randint(0, 86_399)
    )
    hidden = "*" * (5 if scheme == "AMEX" else 6)
    return {
        "psp_code": psp,
        "psp_transaction_id": f"{psp}-{rng.getrandbits(48):012x}",
        "merchant_id": merchant_id,
        "terminal_id": f"T{merchant_id[1:]}{rng.randint(1, 3):02d}",
        "transaction_type": "REFUND" if rng.random() < 0.05 else "PURCHASE",
        "transaction_timestamp": iso_ts(ts),
        "amount": str(money(Decimal(rng.randint(100, 50_000)) / 100)),
        "currency": merchants[merchant_id],
        "card_scheme": scheme,
        "masked_pan": f"{SCHEME_BINS[scheme]}{hidden}{rng.randint(0, 9999):04d}",
        "auth_code": f"{rng.randint(0, 999_999):06d}" if approved else "",
        "response_code": APPROVED_RESPONSE_CODE if approved else rng.choice(DECLINE_CODES),
        "entry_mode": rng.choice(ENTRY_MODES),
    }


def make_settlement(rng: random.Random, txn: dict, settlement_date: date) -> dict:
    amount = Decimal(txn["amount"])
    if txn["transaction_type"] == "REFUND":
        gross, interchange, scheme_fee = -amount, Decimal("0.00"), Decimal("0.00")
    else:
        gross = amount
        interchange = money(amount * INTERCHANGE_RATE)
        scheme_fee = money(amount * SCHEME_RATE)
    return {
        "settlement_id": f"STL-{txn['psp_code']}-{rng.getrandbits(48):012x}",
        "psp_code": txn["psp_code"],
        "psp_transaction_id": txn["psp_transaction_id"],
        "merchant_id": txn["merchant_id"],
        "settlement_date": settlement_date.isoformat(),
        "gross_amount": str(money(gross)),
        "interchange_fee": str(interchange),
        "scheme_fee": str(scheme_fee),
        "net_amount": str(money(gross - interchange - scheme_fee)),
        "currency": txn["currency"],
    }


def write_csv_with_control(path: Path, header: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    control = {
        "file_name": path.name,
        "record_count": len(rows),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    path.with_suffix(".ctl").write_text(json.dumps(control, indent=2) + "\n", encoding="utf-8")


def generate_psp_day(
    rng: random.Random,
    cfg: GeneratorConfig,
    psp: str,
    business_date: date,
    merchants: dict[str, str],
    previous_valid_rows: list[dict],
) -> PspDayResult:
    n = cfg.txns_per_file
    k = max(1, round(cfg.defect_rate * n))
    rows = [make_transaction(rng, psp, business_date, merchants) for _ in range(n)]
    anomalies: dict[str, list[str]] = {}

    pool = list(range(n))
    rng.shuffle(pool)

    def take(count: int, predicate=lambda i: True) -> list[int]:
        picked = [i for i in pool if predicate(i)][:count]
        for i in picked:
            pool.remove(i)
        return picked

    # 1) Row-level validation failures -> must end up in quarantine, never settled.
    quarantined: set[int] = set()
    mutations = {
        MISSING_MANDATORY_FIELD: lambda r: r.update(merchant_id=""),
        INVALID_CURRENCY: lambda r: r.update(currency="XXX"),
        INVALID_AMOUNT: lambda r: r.update(amount=rng.choice(["0.00", "-12.50", "12,50"])),
        INVALID_TIMESTAMP: lambda r: r.update(transaction_timestamp="2026-13-45T25:61:00Z"),
    }
    for defect, mutate in mutations.items():
        for i in take(k):
            mutate(rows[i])
            quarantined.add(i)
            anomalies.setdefault(defect, []).append(rows[i]["psp_transaction_id"])

    # 2) Late arrivals: valid rows whose event time belongs to the previous day.
    for i in take(k):
        ts = datetime.strptime(rows[i]["transaction_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        rows[i]["transaction_timestamp"] = iso_ts(ts - timedelta(days=1))
        anomalies.setdefault(LATE_ARRIVAL, []).append(rows[i]["psp_transaction_id"])

    valid_rows = [rows[i] for i in range(n) if i not in quarantined]
    approved = lambda i: rows[i]["response_code"] == APPROVED_RESPONSE_CODE  # noqa: E731

    # 3) Settlement-side defects, only on valid approved rows.
    missing_settlement = {rows[i]["psp_transaction_id"] for i in take(k, approved)}
    amount_mismatch = {rows[i]["psp_transaction_id"] for i in take(k, approved)}
    duplicate_settlement = {rows[i]["psp_transaction_id"] for i in take(k, approved)}
    anomalies[MISSING_SETTLEMENT] = sorted(missing_settlement)
    anomalies[AMOUNT_MISMATCH] = sorted(amount_mismatch)
    anomalies[DUPLICATE_SETTLEMENT] = sorted(duplicate_settlement)

    # 4) Duplicates. Copies are appended after settlement selection so they are never settled twice.-- subasri oru mental
    
    file_rows = list(rows)
    for i in take(k):
        file_rows.append(dict(rows[i]))
        anomalies.setdefault(DUPLICATE_IN_FILE, []).append(rows[i]["psp_transaction_id"])
    if previous_valid_rows:
        for prev in rng.sample(previous_valid_rows, min(k, len(previous_valid_rows))):
            file_rows.append(dict(prev))
            anomalies.setdefault(CROSS_FILE_DUPLICATE, []).append(prev["psp_transaction_id"])

    file_rows.sort(key=lambda r: r["transaction_timestamp"])

    # 5) Settlement file: settles valid, approved, first-seen transactions of this file.
    settlement_date = business_date + timedelta(days=1)
    settlements: list[dict] = []
    for row in valid_rows:
        txn_id = row["psp_transaction_id"]
        if row["response_code"] != APPROVED_RESPONSE_CODE or txn_id in missing_settlement:
            continue
        stl = make_settlement(rng, row, settlement_date)
        if txn_id in amount_mismatch:
            delta = money(Decimal(rng.randint(1, 500)) / 100)
            stl["gross_amount"] = str(money(Decimal(stl["gross_amount"]) + delta))
            stl["net_amount"] = str(
                money(Decimal(stl["gross_amount"]) - Decimal(stl["interchange_fee"]) - Decimal(stl["scheme_fee"]))
            )
        settlements.append(stl)
        if txn_id in duplicate_settlement:
            dup = dict(stl)
            dup["settlement_id"] = f"STL-{psp}-{rng.getrandbits(48):012x}"
            settlements.append(dup)

    for _ in range(k):
        ghost = make_transaction(rng, psp, business_date, merchants)
        ghost["response_code"] = APPROVED_RESPONSE_CODE
        settlements.append(make_settlement(rng, ghost, settlement_date))
        anomalies.setdefault(ORPHAN_SETTLEMENT, []).append(ghost["psp_transaction_id"])

    rng.shuffle(settlements)

    stamp = business_date.strftime("%Y%m%d")
    txn_path = cfg.out_dir / "landing" / "transactions" / f"{psp}_TXN_{stamp}_001.csv"
    stl_path = cfg.out_dir / "landing" / "settlements" / f"{psp}_STL_{stamp}_001.csv"
    write_csv_with_control(txn_path, field_names(TRANSACTION_FIELDS), file_rows)
    write_csv_with_control(stl_path, field_names(SETTLEMENT_FIELDS), settlements)

    return PspDayResult(
        transaction_file=txn_path.name,
        settlement_file=stl_path.name,
        txn_rows_in_file=len(file_rows),
        settlement_rows_in_file=len(settlements),
        expected_quarantined=len(quarantined),
        expected_new_unique_valid=len(valid_rows),
        anomalies={key: sorted(v) for key, v in sorted(anomalies.items())},
        valid_rows=valid_rows,
    )


def generate(cfg: GeneratorConfig) -> list[Path]:
    rng = random.Random(cfg.seed)
    merchants = build_merchants(rng, cfg.merchants)
    previous: dict[str, list[dict]] = {psp: [] for psp in PSPS}
    manifests: list[Path] = []

    ref_path = cfg.out_dir / "reference" / "merchants.csv"
    ref_path.parent.mkdir(parents=True, exist_ok=True)
    with ref_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(["merchant_id", "merchant_name", "settlement_currency"])
        for mid, ccy in sorted(merchants.items()):
            writer.writerow([mid, f"Demo Merchant {mid[1:]}", ccy])

    for offset in range(cfg.days):
        business_date = cfg.start_date + timedelta(days=offset)
        day_manifest = {"business_date": business_date.isoformat(), "seed": cfg.seed, "psps": {}}
        for psp in PSPS:
            result = generate_psp_day(rng, cfg, psp, business_date, merchants, previous[psp])
            previous[psp] = result.valid_rows
            entry = result.__dict__.copy()
            entry.pop("valid_rows")
            day_manifest["psps"][psp] = entry
        out = cfg.out_dir / "ground_truth" / f"expected_{business_date:%Y%m%d}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(day_manifest, indent=2) + "\n", encoding="utf-8")
        manifests.append(out)
    return manifests


def parse_args(argv: list[str] | None = None) -> GeneratorConfig:
    p = argparse.ArgumentParser(description="Generate synthetic PSP files")
    p.add_argument("--start-date", type=date.fromisoformat, required=True)
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--txns-per-file", type=int, default=500)
    p.add_argument("--merchants", type=int, default=25)
    p.add_argument("--defect-rate", type=float, default=0.01)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=Path, default=Path("data"))
    a = p.parse_args(argv)
    return GeneratorConfig(a.start_date, a.days, a.txns_per_file, a.merchants, a.defect_rate, a.seed, a.out_dir)


def main(argv: list[str] | None = None) -> None:
    cfg = parse_args(argv)
    for manifest in generate(cfg):
        data = json.loads(manifest.read_text())
        print(f"{data['business_date']}:")
        for psp, info in data["psps"].items():
            print(
                f"  {psp}: {info['txn_rows_in_file']} txn rows "
                f"({info['expected_quarantined']} invalid), "
                f"{info['settlement_rows_in_file']} settlement rows"
            )
    print(f"Files written under {cfg.out_dir.resolve()}")


if __name__ == "__main__":
    main()
