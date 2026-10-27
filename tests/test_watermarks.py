"""Phase 3.2: watermarks, windowed streaming aggregation, and batch restatement.

No Kafka needed: the generated events file (already in SEND order) is cut into one file
per hour of send time, and Spark streams those files one per micro-batch. That replays
the day in the order the gateway sent it, so late events really do arrive late.
"""
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from payrecon.common.paths import Paths
from payrecon.generator.generate_events import LATE_EVENT, EventConfig, generate_events
from payrecon.generator.generate_psp_files import GeneratorConfig, generate
from payrecon.gold.volume_restatement import compare_volumes
from payrecon.streaming.merchant_volumes import (
    WINDOW_KEY,
    VolumeConfig,
    aggregate_windows,
    batch_volumes,
    run_available_now,
    valid_events,
)
from payrecon.streaming.stream_to_bronze import parse_and_validate

pytestmark = pytest.mark.slow  # streaming + full pipeline: run in the slow CI job

START = date(2026, 10, 19)


def file_events(spark, chunks_dir):
    stream = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(str(chunks_dir))
    return valid_events(parse_and_validate(stream))


def run_stream(spark, root, chunks_dir, watermark_minutes):
    cfg = VolumeConfig(paths=Paths(root / f"wm{watermark_minutes}"), watermark_minutes=watermark_minutes)
    summary = run_available_now(spark, file_events(spark, chunks_dir), cfg)
    table = spark.read.format("delta").load(cfg.paths.gold(cfg.table))
    return cfg, summary, table


@pytest.fixture(scope="module")
def replay(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("watermarks")
    generate(GeneratorConfig(start_date=START, days=1, txns_per_file=200, seed=17, out_dir=root))
    (events_file,) = generate_events(EventConfig(data_dir=root, seed=17))
    manifest = json.loads((root / "ground_truth" / f"stream_expected_{START:%Y%m%d}.json").read_text())
    late_ids = sorted({t for info in manifest["psps"].values() for t in info["anomalies"][LATE_EVENT]})

    # One file per hour of SEND time, with increasing modification times: Spark's file
    # source picks the oldest file first, so the batches follow the send order.
    chunks_dir = root / "chunks"
    chunks_dir.mkdir()
    by_hour: dict[str, list[str]] = {}
    for line in events_file.read_text().splitlines():
        by_hour.setdefault(json.loads(line)["produced_at"][:13], []).append(line)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    for i, hour in enumerate(sorted(by_hour)):
        chunk = chunks_dir / f"chunk_{i:03d}.jsonl"
        chunk.write_text("\n".join(by_hour[hour]) + "\n")
        os.utime(chunk, (base + i * 10, base + i * 10))

    checked = parse_and_validate(spark.read.text(str(events_file)))
    truth = batch_volumes(valid_events(checked), 15).persist()
    late_per_window = (
        batch_volumes(valid_events(checked.filter(F.col("psp_transaction_id").isin(late_ids))), 15)
        .select(*WINDOW_KEY, F.col("event_count").alias("late_in_window"))
        .persist()
    )
    assert late_per_window.agg(F.sum("late_in_window")).first()[0] == len(late_ids) > 0

    cfg10, summary10, stream10 = run_stream(spark, root, chunks_dir, 10)
    _, _, stream480 = run_stream(spark, root, chunks_dir, 480)
    return {
        "chunks_dir": chunks_dir,
        "truth": truth,
        "late_per_window": late_per_window,
        "cfg10": cfg10,
        "summary10": summary10,
        "stream10": stream10,
        "stream480": stream480,
    }


def with_late_counts(diff, late_per_window):
    return diff.join(late_per_window, on=WINDOW_KEY, how="left").fillna(0, subset=["late_in_window"])


# --------------------------------------------------------------------------- the aggregation itself
def test_window_measures_on_a_tiny_example(spark):
    rows = [
        ("e1", "2026-10-19 10:01:00", "M1", "EUR", "AUTHORISED", Decimal("10.00")),
        ("e2", "2026-10-19 10:14:59", "M1", "EUR", "REFUNDED", Decimal("4.00")),
        ("e3", "2026-10-19 10:05:00", "M1", "EUR", "DECLINED", Decimal("99.00")),
        ("e3", "2026-10-19 10:05:00", "M1", "EUR", "DECLINED", Decimal("99.00")),  # duplicate delivery
        ("e4", "2026-10-19 10:15:00", "M1", "EUR", "AUTHORISED", Decimal("1.00")),  # next window
    ]
    events = spark.createDataFrame(
        rows, "event_id string, ts string, merchant_id string, currency string, event_type string, amount decimal(18,2)"
    ).withColumn("event_ts", F.to_timestamp("ts"))
    result = {r["window_start"].strftime("%H:%M"): r for r in batch_volumes(events, 15).collect()}

    first = result["10:00"]
    assert (first["event_count"], first["approved_count"], first["declined_count"]) == (3, 2, 1)
    assert first["net_approved_amount"] == Decimal("6.00")  # 10.00 - 4.00 refund; declines move no money
    assert result["10:15"]["event_count"] == 1
    assert aggregate_windows(events, 15).filter(F.col("window_start") == first["window_start"]).first()["event_count"] == 4


# --------------------------------------------------------------------------- watermark behaviour
def test_stream_never_counts_more_than_the_truth(replay):
    """Duplicate deliveries are removed inside the watermark: no window is ever over-counted."""
    diff = compare_volumes(replay["stream10"], replay["truth"])
    assert diff.count() > 0, "the stream must have finalised some windows"
    assert diff.filter(F.col("late_events_missing") < 0).count() == 0


def test_only_late_events_are_dropped(replay):
    """Every difference between stream and truth is explained by planted LATE_EVENTs."""
    diff = with_late_counts(compare_volumes(replay["stream10"], replay["truth"]), replay["late_per_window"])
    assert diff.filter(F.col("late_events_missing") > F.col("late_in_window")).count() == 0
    on_time_windows = diff.filter(F.col("late_in_window") == 0)
    for m in ("event_count", "approved_count", "declined_count", "net_approved_amount"):
        assert on_time_windows.filter(F.col(f"stream_{m}") != F.col(f"batch_{m}")).count() == 0, m


def test_a_short_watermark_drops_late_events(replay):
    diff = compare_volumes(replay["stream10"], replay["truth"])
    missed = diff.agg(F.sum("late_events_missing")).first()[0]
    assert missed > 0, "events 2-6 hours late must be beyond a 10-minute watermark"
    print(f"10-minute watermark: {missed} late events missed; summary: {replay['summary10']}")


def test_a_long_watermark_is_complete_but_slower(replay):
    """Trade-off: an 8-hour watermark waits for every late event, but finalises fewer windows."""
    diff = compare_volumes(replay["stream480"], replay["truth"])
    assert diff.count() > 0
    assert diff.filter(F.col("late_events_missing") != 0).count() == 0
    assert replay["stream480"].count() < replay["stream10"].count()


def test_rerun_never_writes_a_window_twice(spark, replay):
    """The checkpoint remembers the files AND the open windows: each window is written once."""
    cfg = replay["cfg10"]
    path = cfg.paths.gold(cfg.table)
    before = spark.read.format("delta").load(path).count()
    run_available_now(spark, file_events(spark, replay["chunks_dir"]), cfg)
    after = spark.read.format("delta").load(path)
    assert after.count() >= before
    assert after.groupBy(*WINDOW_KEY).count().filter("count > 1").count() == 0
