import csv
import json
import re
from collections import Counter
from datetime import date, datetime, timedelta

import pytest

from payrecon.generator.generate_events import (
    DUPLICATE_DELIVERY,
    LATE_EVENT,
    MISSING_IN_CLEARING,
    MISSING_IN_STREAM,
    EventConfig,
    generate_events,
)
from payrecon.generator.generate_psp_files import QUARANTINE_DEFECTS, GeneratorConfig, generate
from payrecon.schemas.contracts import (
    EVENT_SCHEMA_VERSION,
    MASKED_PAN_PATTERN,
    PAYMENT_EVENT_FIELDS,
    VALID_EVENT_TYPES,
    field_names,
)


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


@pytest.fixture(scope="module")
def stream(tmp_path_factory):
    root = tmp_path_factory.mktemp("events")
    generate(GeneratorConfig(start_date=date(2026, 10, 19), days=2, txns_per_file=200, seed=5, out_dir=root))
    files = generate_events(EventConfig(data_dir=root, seed=5))
    manifests = [json.loads(p.read_text()) for p in sorted((root / "ground_truth").glob("stream_expected_*.json"))]
    clearing = [json.loads(p.read_text()) for p in sorted((root / "ground_truth").glob("expected_*.json"))]
    return root, files, manifests, clearing


def test_every_event_follows_the_contract(stream):
    _, files, _, _ = stream
    for path in files:
        for event in read_events(path):
            assert list(event) == field_names(PAYMENT_EVENT_FIELDS)
            assert event["event_type"] in VALID_EVENT_TYPES
            assert isinstance(event["amount"], str), "money travels as a string, never a JSON number"
            assert re.match(r"^[0-9]+\.[0-9]{2}$", event["amount"])
            assert re.match(MASKED_PAN_PATTERN, event["masked_pan"])
            assert event["schema_version"] == EVENT_SCHEMA_VERSION


def test_files_are_in_send_order(stream):
    _, files, _, _ = stream
    for path in files:
        sent = [e["produced_at"] for e in read_events(path)]
        assert sent == sorted(sent)


def test_duplicate_deliveries_are_exact_resends(stream):
    _, files, manifests, _ = stream
    for path, manifest in zip(files, manifests):
        events = read_events(path)
        counts = Counter(e["event_id"] for e in events)
        resent_ids = {e["psp_transaction_id"] for e in events if counts[e["event_id"]] == 2}
        expected = {t for info in manifest["psps"].values() for t in info["anomalies"][DUPLICATE_DELIVERY]}
        assert resent_ids == expected
        assert max(counts.values()) == 2
        assert len(events) - len(counts) == len(expected)


def test_late_events_are_exactly_the_planted_ones(stream):
    _, files, manifests, _ = stream
    for path, manifest in zip(files, manifests):
        late = {
            e["psp_transaction_id"]
            for e in read_events(path)
            if ts(e["produced_at"]) - ts(e["event_timestamp"]) > timedelta(hours=1)
        }
        expected = {t for info in manifest["psps"].values() for t in info["anomalies"][LATE_EVENT]}
        assert late == expected


def test_stream_and_clearing_differ_only_by_planted_gaps(stream):
    root, files, manifests, clearing = stream
    for path, manifest, day in zip(files, manifests, clearing):
        streamed = {e["psp_transaction_id"] for e in read_events(path)}
        for psp, info in day["psps"].items():
            with (root / "landing" / "transactions" / info["transaction_file"]).open(newline="") as fh:
                ids_in_file = {r["psp_transaction_id"] for r in csv.DictReader(fh)}
            not_real = set()
            for defect in (*QUARANTINE_DEFECTS, "CROSS_FILE_DUPLICATE"):
                not_real.update(info["anomalies"].get(defect, []))
            real_payments = ids_in_file - not_real
            gaps = manifest["psps"][psp]["anomalies"]
            psp_streamed = {t for t in streamed if t.startswith(psp)}

            assert psp_streamed - real_payments == set(gaps[MISSING_IN_CLEARING])
            assert real_payments - psp_streamed == set(gaps[MISSING_IN_STREAM])
            assert not (psp_streamed & not_real), "invalid or re-sent file rows must never become events"


def test_event_generation_is_deterministic(tmp_path):
    def run(sub):
        root = tmp_path / sub
        generate(GeneratorConfig(start_date=date(2026, 10, 19), days=1, txns_per_file=50, seed=3, out_dir=root))
        return [p.read_bytes() for p in generate_events(EventConfig(data_dir=root, seed=3))]

    assert run("a") == run("b")
