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
| 1.4 Clearing ↔ settlement reconciliation + Gold, verified against ground truth | ⚠️ code delivered, test run not yet confirmed |

## Current phase: 2 — Real-time (local)

| Milestone | Status |
|---|---|
| 2.1 Kafka in Docker, event contract + generator with stream defects, producer, consumer groups/offsets | ✅ verified in codespace (4365 sent, 45 duplicates, 45 late); experiment 3 + full test run pending |
| 2.2 Spark Structured Streaming: Kafka → Bronze events, checkpoints, dedup by event_id, DLQ | 🔄 in progress |

## Roadmap (all local and free until Phase 4)

| Phase | Content | Cost |
|---|---|---|
| 2 Real-time | Kafka in Docker, event producer (same transactions as the batch files), consumer basics, Structured Streaming → Bronze | free (Codespaces) |
| 3 Batch + stream unified | MERGE both channels into Silver, watermarks, late events, stream ↔ clearing recon | free (Codespaces) |
| 4 Snowflake DWH | star schema, stage + COPY INTO, MERGE loads, analytics SQL | 30-day trial — start only when DDL is ready |
| 5 Databricks deployment | run the pipelines on Databricks Free Edition, Workflows | free tier (quota-limited) |
| 6 Production engineering | CI/CD, data quality, monitoring, docs, interview prep | free (GitHub Actions) |
