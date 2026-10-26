"""Synthetic real-time payment events, derived from the generated clearing files.

The gateway sees each payment the moment it happens; the clearing file lists the
same payments at the end of the day. So events are built FROM the clearing rows,
which is what later lets us reconcile stream vs clearing (Phase 3).

For every business date with a clearing manifest this writes:
  data/stream/events_{YYYYMMDD}.jsonl                one JSON event per line, in SEND order
  data/ground_truth/stream_expected_{YYYYMMDD}.json  every stream defect that was planted

Stream defects (per PSP per day, ~1% each, never two on the same transaction):
  MISSING_IN_STREAM    approved payment in the clearing file, but no event was ever sent
  MISSING_IN_CLEARING  event for a payment that never appears in any clearing file
  LATE_EVENT           event sent 2-6 hours after it happened (arrives out of order)
  DUPLICATE_DELIVERY   the same event (same event_id) delivered twice

Must run AFTER generate_psp_files. Uses its own random generator, so the batch
files stay byte-identical whether or not events are generated.

Usage:
  python -m payrecon.generator.generate_events
  python -m payrecon.generator.generate_events --data-dir data --seed 42
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from payrecon.generator.generate_psp_files import QUARANTINE_DEFECTS, make_transaction
from payrecon.schemas.contracts import APPROVED_RESPONSE_CODE, EVENT_SCHEMA_VERSION

MISSING_IN_STREAM = "MISSING_IN_STREAM"
MISSING_IN_CLEARING = "MISSING_IN_CLEARING"
LATE_EVENT = "LATE_EVENT"
DUPLICATE_DELIVERY = "DUPLICATE_DELIVERY"

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class EventConfig:
    data_dir: Path = Path("data")
    seed: int = 42
    defect_rate: float = 0.01


def iso_ms(dt: datetime) -> str:
    """2026-10-20T14:03:09.123Z"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def event_type(row: dict) -> str:
    if row["response_code"] != APPROVED_RESPONSE_CODE:
        return "DECLINED"
    return "REFUNDED" if row["transaction_type"] == "REFUND" else "AUTHORISED"


def to_event(rng: random.Random, row: dict, delay: timedelta) -> dict:
    happened = datetime.strptime(row["transaction_timestamp"], TS_FORMAT).replace(tzinfo=timezone.utc)
    return {
        "event_id": f"{rng.getrandbits(128):032x}",
        "event_type": event_type(row),
        "event_timestamp": row["transaction_timestamp"],
        "produced_at": iso_ms(happened + delay),
        "psp_code": row["psp_code"],
        "psp_transaction_id": row["psp_transaction_id"],
        "merchant_id": row["merchant_id"],
        "terminal_id": row["terminal_id"],
        "transaction_type": row["transaction_type"],
        "amount": row["amount"],
        "currency": row["currency"],
        "card_scheme": row["card_scheme"],
        "masked_pan": row["masked_pan"],
        "entry_mode": row["entry_mode"],
        "response_code": row["response_code"],
        "schema_version": EVENT_SCHEMA_VERSION,
    }


def normal_delay(rng: random.Random) -> timedelta:
    return timedelta(milliseconds=rng.randint(50, 3000))


def clearing_rows_for_events(clearing_file: Path, psp_info: dict) -> list[dict]:
    """The payments the gateway really saw: valid rows, each transaction once.

    Invalid rows (quarantined in Silver) and duplicate copies are file problems,
    not real payments, so they never become events.
    """
    excluded = set()
    for defect in (*QUARANTINE_DEFECTS, "CROSS_FILE_DUPLICATE"):
        excluded.update(psp_info["anomalies"].get(defect, []))
    seen: set[str] = set()
    rows = []
    with clearing_file.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            txn_id = row["psp_transaction_id"]
            if txn_id in excluded or txn_id in seen:
                continue
            seen.add(txn_id)
            rows.append(row)
    rows.sort(key=lambda r: r["psp_transaction_id"])  # stable order, independent of file order
    return rows


def events_for_psp_day(
    rng: random.Random, rows: list[dict], psp: str, business_date: date, defect_rate: float, merchants: dict[str, str]
) -> tuple[list[dict], dict[str, list[str]]]:
    k = max(1, round(defect_rate * len(rows)))
    pool = list(range(len(rows)))
    rng.shuffle(pool)

    def take(count: int, predicate=lambda i: True) -> list[int]:
        picked = [i for i in pool if predicate(i)][:count]
        for i in picked:
            pool.remove(i)
        return picked

    approved = lambda i: rows[i]["response_code"] == APPROVED_RESPONSE_CODE  # noqa: E731
    missing = set(take(k, approved))
    late = set(take(k))
    duplicated = set(take(k))

    events: list[dict] = []
    for i, row in enumerate(rows):
        if i in missing:
            continue
        delay = timedelta(hours=rng.randint(2, 5), minutes=rng.randint(0, 59)) if i in late else normal_delay(rng)
        event = to_event(rng, row, delay)
        events.append(event)
        if i in duplicated:
            resend = dict(event)  # same event_id: a retry by the sender, not a new payment
            sent = datetime.strptime(event["produced_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            resend["produced_at"] = iso_ms(sent + timedelta(seconds=rng.randint(1, 30)))
            events.append(resend)

    ghosts = []
    for _ in range(k):
        ghost = make_transaction(rng, psp, business_date, merchants)
        ghost["response_code"] = APPROVED_RESPONSE_CODE
        ghost["auth_code"] = f"{rng.randint(0, 999_999):06d}"
        events.append(to_event(rng, ghost, normal_delay(rng)))
        ghosts.append(ghost["psp_transaction_id"])

    anomalies = {
        MISSING_IN_STREAM: sorted(rows[i]["psp_transaction_id"] for i in missing),
        LATE_EVENT: sorted(rows[i]["psp_transaction_id"] for i in late),
        DUPLICATE_DELIVERY: sorted(rows[i]["psp_transaction_id"] for i in duplicated),
        MISSING_IN_CLEARING: sorted(ghosts),
    }
    return events, anomalies


def load_merchants(data_dir: Path) -> dict[str, str]:
    with (data_dir / "reference" / "merchants.csv").open(newline="", encoding="utf-8") as fh:
        return {r["merchant_id"]: r["settlement_currency"] for r in csv.DictReader(fh)}


def generate_events(cfg: EventConfig) -> list[Path]:
    rng = random.Random(cfg.seed * 1_000 + 7)  # separate stream of randomness from the batch generator
    merchants = load_merchants(cfg.data_dir)
    stream_dir = cfg.data_dir / "stream"
    stream_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for manifest_path in sorted((cfg.data_dir / "ground_truth").glob("expected_*.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        business_date = date.fromisoformat(manifest["business_date"])
        day_events: list[dict] = []
        stream_manifest = {"business_date": manifest["business_date"], "seed": cfg.seed, "psps": {}}

        for psp, info in sorted(manifest["psps"].items()):
            clearing_file = cfg.data_dir / "landing" / "transactions" / info["transaction_file"]
            rows = clearing_rows_for_events(clearing_file, info)
            events, anomalies = events_for_psp_day(rng, rows, psp, business_date, cfg.defect_rate, merchants)
            day_events.extend(events)
            stream_manifest["psps"][psp] = {
                "event_messages": len(events),
                "unique_events": len({e["event_id"] for e in events}),
                "anomalies": anomalies,
            }

        # The file is in SEND order: late events land far from their event time.
        day_events.sort(key=lambda e: (e["produced_at"], e["event_id"]))
        stamp = business_date.strftime("%Y%m%d")
        events_path = stream_dir / f"events_{stamp}.jsonl"
        with events_path.open("w", encoding="utf-8") as fh:
            for event in day_events:
                fh.write(json.dumps(event, separators=(",", ":")) + "\n")
        stream_manifest["events_file"] = events_path.name
        stream_manifest["event_messages"] = len(day_events)
        out = cfg.data_dir / "ground_truth" / f"stream_expected_{stamp}.json"
        out.write_text(json.dumps(stream_manifest, indent=2) + "\n", encoding="utf-8")
        written.append(events_path)
    return written


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic payment events from the clearing files")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--defect-rate", type=float, default=0.01)
    args = parser.parse_args(argv)
    paths = generate_events(EventConfig(args.data_dir, args.seed, args.defect_rate))
    if not paths:
        print("No clearing manifests found. Run payrecon.generator.generate_psp_files first.")
    for path in paths:
        lines = sum(1 for _ in path.open(encoding="utf-8"))
        print(f"{path.name}: {lines} event messages")


if __name__ == "__main__":
    main()
