# Design decisions and lessons learned

Notes on why the platform is built the way it is, what went wrong while building it, and
what I would change next. Numbers come from my own runs on the default data set.

## Decisions

### Bronze stores every column as a string
If Bronze typed the data, a value like `12,50` or `2026-13-45T25:61:00Z` would silently become
NULL and the evidence would be gone. Bronze keeps exactly what the PSP sent; Silver types it
with `try_cast` / `try_to_timestamp` and records *why* a row failed.

### Files are identified by their SHA-256, not their name
The replay guard skips a file whose hash already has a SUCCESS row in the audit table. The
name says nothing about the content: a corrected re-send can reuse the name, and the same
bytes can arrive under another name.

### Bronze writes with `replaceWhere`, not `append`
Bronze writes the data, then the audit row. A crash in between means the next run ingests the
file again. With `append` that doubles every row; `replaceWhere` on (business date, file hash)
replaces the earlier attempt instead.

### First failing rule wins in Silver
One clear reason per quarantined row is what an operations team can act on. Rule order is
deliberate: the missing-field check runs first so later rules never see NULLs
(`NULL.rlike(...)` is not TRUE in Spark).

### Silver deduplication: oldest file wins, retries are not duplicates
A window ranks copies of each business key by business date and file name. A key that is
already in Silver from the *same* file hash is a retry after a crash (`ALREADY_LOADED`), not a
duplicate. Without that branch a simple retry would quarantine hundreds of good rows.

### Settlements are aggregated before the reconciliation join
Joining one transaction to two settlement lines doubles every downstream sum. Grouping to one
row per key first turns a duplicate settlement into `settlement_count = 2`, a status I can
report.

### `as_of_date` is a parameter
Whether a missing settlement is PENDING or MISSING depends on "today". Passing the date in
makes every report reproducible and every test deterministic.

### Two keys for streaming duplicates
Bronze events are keyed on Kafka (topic, partition, offset), so a replayed micro-batch adds
nothing. Silver events are keyed on `event_id`, so a gateway re-send (new offset, same event)
is removed. Bronze still keeps both messages as proof the gateway sent twice.

### Exactly-once = replayable source + checkpoint + idempotent sink
`foreachBatch` is at-least-once: a batch can run twice after a crash. Every write inside it is
an idempotent MERGE, so the result is exactly-once. The dead-letter topic is a plain append
and therefore at-least-once, which is fine for an investigation queue.

### The lifecycle table is order-independent by construction
Each channel owns a disjoint set of columns and a `seen_in_<channel>` flag. The MERGE inserts
new keys, fills only its own columns while its flag is false, and otherwise does nothing.
Tests run both arrival orders and compare the tables with `exceptAll`; a re-run does not even
change `last_updated_ts`. Concurrent commits from the two jobs are retried, which is safe
because the MERGE is idempotent.

### Two verdicts per payment, not one
`stream_status` and `settlement_status` are separate columns. In my run one payment was both
missing from the stream and missing a settlement; a single status column would have hidden one
of the two problems.

### Real-time numbers are provisional
The windowed volumes use a watermark so state stays bounded. Late data is dropped from that
view, so Silver keeps every event and a batch restatement recomputes the same windows with the
same aggregation function. Any difference between the two tables can only be late data.

## Incidents

### The dev container would not build
Codespaces kept starting a minimal recovery container. The first ERROR in the creation log
showed `apt-get update` failing on an expired Yarn signing key that ships inside the base
image. Fix: a Dockerfile that removes that package source before installing Java. Lesson: read
the first error, not the last, and own the build environment.

### Lazy evaluation reported new rows as duplicates
Silver classifies rows by checking which keys already exist in the Silver table, then MERGEs,
then writes quarantine and audit rows. Because Spark is lazy, the later steps re-ran the
classification *after* the MERGE and saw the freshly inserted rows as existing. Fix:
`localCheckpoint()` right after classification. The answer-key test caught it.

### The test suite died halfway through
With 9 test files the suite failed with `RejectedExecutionException`, then every later test
with `ConnectionRefused`. There was no OOM kill and no crash file; each file passed on its own.
The single long-lived JVM was accumulating UI history, caches and streaming state. Fixes:
bounded Spark UI retention, and a fresh JVM per test file (stop the session, close the gateway
process). Result: 50 passed in one run.

### 884 on-time events dropped by a 10-minute watermark
Replaying the Kafka backlog into the windowed job dropped 884 events, although only 45 were
planted late. Event times at the same relative offset differed by 1.5 to 2 hours between the
three partitions: micro-batches split offsets by remaining lag, not by time, and the watermark
is global. On-time events in a lagging partition therefore looked late.

| Watermark | Events missed |
|---|---|
| 10 min | 884 |
| 120 min | 79 |
| 480 min | 20 |

Spark's `numRowsDroppedByWatermark` matched the restatement difference exactly (79 = 79,
20 = 20), so it is a reliable alert signal. Takeaways: rebuild backlogs in batch from Silver,
use a larger watermark only while catching up, and alert on dropped rows.

## Known gaps and next steps

- **Corrected re-sends.** A file re-sent with a fix under the same name gets a new hash, but
  Silver's insert-only MERGE keeps the first version. Corrections should carry a new sequence
  number and be upserted, or be rejected for review.
- **One definition of "break".** The reconciliation's `is_break` counts PENDING as a break; the
  three-way view does not. Dashboards should use one definition.
- **Incremental Gold.** Gold is rebuilt from Silver each run. At scale only open business dates
  should be recomputed, and the lifecycle clearing side should read Delta's change data feed.
- **Snowflake.** Next phase: a star schema (payments, settlements, breaks; merchant, date, PSP
  dimensions) loaded from Gold, modelled with dbt.
