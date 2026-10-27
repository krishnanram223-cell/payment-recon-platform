# Project progress — v2

Focus: real-time + batch processing, Snowflake DWH for analytics. (v1 AWS project kept separately.)
Development environment: GitHub Codespaces (Debian 12, Python 3.12, Java 17).

## Current phase: 1 — Batch foundation (local)

| Milestone | Status |
|---|---|
| 0 Development environment (Codespaces) | ✅ done |
| 1.1 Data contracts + synthetic PSP clearing/settlement files + tests | ✅ done |
| 1.2 PySpark + Delta; Bronze ingestion with audit columns, control-file and replay checks | ✅ done (gate: quiz 14/15, Task 1 passed; Task 2 control-file-name check included, verified by its test) |
| 1.3 Silver: validation, quarantine, MERGE dedup, verified against ground truth | ✅ done (22 tests passing) |
| 1.4 Clearing ↔ settlement reconciliation + Gold, verified against ground truth | ✅ done (7 Gold tests passing) |

## Current phase: 2 — Real-time (local)

| Milestone | Status |
|---|---|
| 2.1 Kafka in Docker, event contract + generator with stream defects, producer, consumer groups/offsets | ✅ done (4365 sent, 45 duplicates, 45 late; tests passing) |
| 2.2 Spark Structured Streaming: Kafka → Bronze events, checkpoints, dedup by event_id, DLQ | ✅ done (4320 unique events, 45 duplicates removed; all 37 tests passing) |

## Current phase: 3 — Batch + stream unified (local)

| Milestone | Status |
|---|---|
| 3.1 Transaction lifecycle from both channels (order-independent MERGE, Delta streaming source) + three-way reconciliation | ✅ done (4275 matched, 45 missing in stream, 45 missing in clearing; 44 tests passing) |
| 3.2 Watermarks + windowed streaming aggregations (near-real-time merchant volumes) + batch restatement | ✅ done (6 tests passing; Kafka replay showed partition skew: 884 / 79 / 20 events dropped at 10 / 120 / 480-min watermarks, drop metric = restatement difference) |

## Roadmap (all local and free until Phase 4)

| Phase | Content | Cost |
|---|---|---|
| 2 Real-time | Kafka in Docker, event producer (same transactions as the batch files), consumer basics, Structured Streaming → Bronze | free (Codespaces) |
| 3 Batch + stream unified | MERGE both channels into Silver, watermarks, late events, stream ↔ clearing recon | free (Codespaces) |
| 4 Snowflake DWH | star schema, stage + COPY INTO, MERGE loads, analytics SQL | 30-day trial — start only when DDL is ready |
| 5 Databricks deployment | run the pipelines on Databricks Free Edition, Workflows | free tier (quota-limited) |
| 6 Production engineering | CI/CD, data quality, monitoring, docs, interview prep | free (GitHub Actions) |
