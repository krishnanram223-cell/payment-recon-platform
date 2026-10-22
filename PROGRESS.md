# Project progress — v2

Focus: real-time + batch processing, Snowflake DWH for analytics. (v1 AWS project kept separately.)

## Current phase: 1 — Batch foundation (local)

| Milestone | Status |
|---|---|
| 1.1 Data contracts + synthetic PSP clearing/settlement files + tests | ✅ code ready — waiting for your local run |
| 1.2 Local PySpark + Delta setup; Bronze ingestion with audit columns, control-file and replay checks | ⏳ next |
| 1.3 Silver: validation, quarantine, MERGE dedup | ⏳ |
| 1.4 Clearing ↔ settlement reconciliation + Gold, verified against ground truth | ⏳ |

## Roadmap (all local and free until Phase 4)

| Phase | Content | Cost |
|---|---|---|
| 2 Real-time | Kafka in Docker, event producer (same transactions as the batch files), consumer basics, Structured Streaming → Bronze | free (local) |
| 3 Batch + stream unified | MERGE both channels into Silver, watermarks, late events, stream ↔ clearing recon | free (local) |
| 4 Snowflake DWH | star schema, stage + COPY INTO, MERGE loads, analytics SQL | 30-day trial — start only when DDL is ready |
| 5 Databricks deployment | run the pipelines on Databricks Free Edition, Workflows | free tier (quota-limited) |
| 6 Production engineering | CI/CD, data quality, monitoring, docs, interview prep | free (GitHub Actions) |
