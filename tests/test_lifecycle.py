"""Phase 3.1: lifecycle from both channels + three-way reconciliation, against the answer keys.

No Kafka needed: events are loaded from the generated JSONL file through the SAME
parse/validate/type code that the streaming job uses.
"""
import json
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from payrecon.common.delta_utils import merge_insert
from payrecon.common.paths import Paths
from payrecon.generator.generate_events import (
    MISSING_IN_CLEARING,
    MISSING_IN_STREAM,
    EventConfig,
    generate_events,
)
from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.gold.reconciliation import AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT, MISSING_SETTLEMENT, run_gold
from payrecon.gold.three_way import (
    AMOUNT_DIFF,
    PENDING_CLEARING,
    THREE_WAY_TABLE,
    build_three_way,
    run_three_way,
)
from payrecon.ingestion.bronze import run_bronze
from payrecon.streaming.stream_to_bronze import parse_and_validate, to_silver_events
from payrecon.transform.lifecycle import (
    KEY,
    LIFECYCLE_TABLE,
    clearing_side,
    merge_side,
    run_clearing_side,
    run_stream_side,
    stream_side,
)
from payrecon.transform.silver import run_silver

pytestmark = pytest.mark.slow  # streaming + full pipeline: run in the slow CI job

START = date(2026, 10, 19)
LONG_AFTER = date(2026, 12, 31)


def table(spark, path):
    return spark.read.format("delta").load(path)


def load_events_into_silver(spark, events_file: Path, paths: Paths) -> None:
    """What the streaming job does per micro-batch, minus Kafka."""
    raw = (
        spark.read.text(str(events_file))
        .withColumn("_kafka_topic", F.lit("file"))
        .withColumn("_kafka_partition", F.lit(0))
        .withColumn("_kafka_offset", F.monotonically_increasing_id())
        .withColumn("_kafka_timestamp", F.current_timestamp())
        .withColumn("_kafka_key", F.lit(None).cast("string"))
    )
    checked = parse_and_validate(raw)
    assert checked.filter(F.col("_reject_reason").isNotNull()).count() == 0
    events = to_silver_events(checked, batch_id=0).dropDuplicates(["event_id"])
    merge_insert(events, paths.silver("payment_events"), ["event_id"], "event_date")


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("lifecycle")
    generate(GeneratorConfig(start_date=START, days=2, txns_per_file=200, seed=17, out_dir=root))
    events_files = generate_events(EventConfig(data_dir=root, seed=17))
    clearing_manifests = [json.loads(p.read_text()) for p in sorted((root / "ground_truth").glob("expected_*.json"))]
    stream_manifests = [json.loads(p.read_text()) for p in sorted((root / "ground_truth").glob("stream_expected_*.json"))]

    paths = Paths(root)
    run_bronze(spark, paths)
    run_silver(spark, paths)
    for events_file in events_files:
        load_events_into_silver(spark, events_file, paths)
    run_clearing_side(spark, paths)
    run_stream_side(spark, paths, available_now=True)  # Delta -> Delta streaming
    run_gold(spark, paths, as_of_date=LONG_AFTER)
    run_three_way(spark, paths, as_of_date=LONG_AFTER)
    return paths, clearing_manifests, stream_manifests


def planted(manifests, defect: str) -> set[str]:
    return {t for m in manifests for info in m["psps"].values() for t in info["anomalies"].get(defect, [])}


def ids_with(df, column: str, value: str) -> set[str]:
    return {r[0] for r in df.filter(F.col(column) == value).select("psp_transaction_id").collect()}


# --------------------------------------------------------------------------- lifecycle
def test_one_lifecycle_row_per_payment_from_both_channels(spark, pipeline):
    paths, _, _ = pipeline
    lifecycle = table(spark, paths.silver(LIFECYCLE_TABLE))
    cleared = table(spark, paths.silver("transactions")).select(*KEY)
    streamed = table(spark, paths.silver("payment_events")).select(*KEY)
    expected = cleared.union(streamed).distinct().count()

    assert lifecycle.count() == expected
    assert lifecycle.select(*KEY).distinct().count() == expected
    assert lifecycle.filter(~F.col("seen_in_stream") & ~F.col("seen_in_clearing")).count() == 0


def test_result_is_the_same_whichever_channel_arrives_first(spark, pipeline, tmp_path):
    paths, _, _ = pipeline
    transactions = table(spark, paths.silver("transactions"))
    events = table(spark, paths.silver("payment_events"))
    stream_first, clearing_first = str(tmp_path / "a"), str(tmp_path / "b")

    merge_side(stream_side(events), stream_first, "stream")
    merge_side(clearing_side(transactions), stream_first, "clearing")
    merge_side(clearing_side(transactions), clearing_first, "clearing")
    merge_side(stream_side(events), clearing_first, "stream")

    compare = [*KEY, "seen_in_stream", "seen_in_clearing", "stream_event_id", "stream_amount", "clearing_amount", "clearing_business_date"]
    a = table(spark, stream_first).select(*compare)
    b = table(spark, clearing_first).select(*compare)
    assert a.count() == b.count()
    assert a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0


def test_rerunning_both_sides_changes_nothing(spark, pipeline):
    paths, _, _ = pipeline
    path = paths.silver(LIFECYCLE_TABLE)
    before = table(spark, path).agg(F.count("*"), F.max("last_updated_ts")).first()

    run_clearing_side(spark, paths)
    run_stream_side(spark, paths, available_now=True)

    after = table(spark, path).agg(F.count("*"), F.max("last_updated_ts")).first()
    assert after == before, "no row may be inserted or even touched by a re-run"


# --------------------------------------------------------------------------- three-way reconciliation
def test_stream_vs_clearing_breaks_match_the_stream_answer_key(spark, pipeline):
    paths, _, stream_manifests = pipeline
    three_way = table(spark, paths.gold(THREE_WAY_TABLE))

    assert ids_with(three_way, "stream_status", MISSING_IN_STREAM) == planted(stream_manifests, MISSING_IN_STREAM)
    assert ids_with(three_way, "stream_status", MISSING_IN_CLEARING) == planted(stream_manifests, MISSING_IN_CLEARING)
    assert three_way.filter(F.col("stream_status") == AMOUNT_DIFF).count() == 0
    statuses = {r[0] for r in three_way.select("stream_status").distinct().collect()}
    assert statuses <= {"MATCHED", MISSING_IN_STREAM, MISSING_IN_CLEARING}


def test_settlement_breaks_are_carried_into_the_three_way_view(spark, pipeline):
    paths, clearing_manifests, _ = pipeline
    three_way = table(spark, paths.gold(THREE_WAY_TABLE))
    for status in (MISSING_SETTLEMENT, AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT):
        assert ids_with(three_way, "settlement_status", status) == planted(clearing_manifests, status), status


def test_break_flag_covers_both_verdicts(spark, pipeline):
    paths, clearing_manifests, stream_manifests = pipeline
    three_way = table(spark, paths.gold(THREE_WAY_TABLE))
    expected = (
        planted(stream_manifests, MISSING_IN_STREAM)
        | planted(stream_manifests, MISSING_IN_CLEARING)
        | planted(clearing_manifests, MISSING_SETTLEMENT)
        | planted(clearing_manifests, AMOUNT_MISMATCH)
        | planted(clearing_manifests, DUPLICATE_SETTLEMENT)
    )
    assert ids_with(three_way, "is_break", True) == expected


def test_unmatched_authorisations_are_pending_inside_the_clearing_window(spark, pipeline):
    paths, _, stream_manifests = pipeline
    result = build_three_way(
        table(spark, paths.silver(LIFECYCLE_TABLE)),
        table(spark, paths.gold("reconciliation")),
        as_of_date=START,
        window_days=2,
        run_id="unit",
    )
    assert result.filter(F.col("stream_status") == MISSING_IN_CLEARING).count() == 0
    assert ids_with(result, "stream_status", PENDING_CLEARING) == planted(stream_manifests, MISSING_IN_CLEARING)
