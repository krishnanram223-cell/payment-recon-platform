# Real-Time Payment Acquiring, Processing and Reconciliation Platform

Portfolio project simulating how a payment processor ingests PSP transaction and
settlement files (and later real-time events), validates them, reconciles them,
and reports to merchants.

See [docs/architecture.md](docs/architecture.md) for design and data contracts and
[PROGRESS.md](PROGRESS.md) for status.

## Quick start (milestone 1.1)

### Option 1 — GitHub Codespaces (no local install)

Open the repo on GitHub → **Code → Codespaces → Create codespace on main**.
The dev container (`.devcontainer/devcontainer.json`) installs Python 3.12, Java 17,
Docker and the project dependencies automatically. Then:

```bash
python -m payrecon.generator.generate_psp_files --start-date 2026-10-19 --days 3
pytest -v
```

### Option 2 — local Linux / WSL2

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pip install -e .

python -m payrecon.generator.generate_psp_files --start-date 2026-10-19 --days 3
pytest -v
```

## Bronze ingestion (milestone 1.2)

```bash
python -m payrecon.ingestion.bronze      # landing files -> Bronze Delta tables + audit log
python -m payrecon.tools.show_tables     # row counts per day/PSP and the audit log
```

## Silver layer (milestone 1.3)

```bash
python -m payrecon.transform.silver      # validate, type, deduplicate -> Silver + quarantine
python -m payrecon.tools.show_tables     # now also shows Silver, quarantine reasons, Silver audit
```

## Reconciliation and Gold (milestone 1.4)

```bash
python -m payrecon.gold.reconciliation --as-of-date 2026-12-31   # match clearing vs settlement, build Gold
python -m payrecon.tools.show_tables                              # recon statuses + merchant daily summary
```

## Real-time events with Kafka (milestone 2.1)

```bash
docker compose up -d                                  # Kafka 4.1 (KRaft) + Kafbat UI on port 8080
python -m payrecon.generator.generate_events          # data/stream/events_*.jsonl from the clearing files
python -m payrecon.streaming.admin create             # topics: payment-events (3 partitions), payment-events-dlq
python -m payrecon.streaming.producer --rate 0        # send all events
python -m payrecon.streaming.admin describe           # messages per partition
python -m payrecon.streaming.consumer_demo            # read them as consumer group "demo"
docker compose down                                   # stop Kafka when done (data kept)
```

## Spark Structured Streaming (milestone 2.2)

```bash
python -m payrecon.streaming.stream_to_bronze --available-now   # everything waiting in Kafka, then stop
python -m payrecon.streaming.stream_to_bronze                   # continuous, a micro-batch every 10 s
```

Kafka → `bronze/payment_events` (every message exactly once, keyed by topic/partition/offset)
→ `silver/payment_events` (one row per `event_id`) and invalid messages → `payment-events-dlq`.
Progress lives in `data/lakehouse/_checkpoints/payment_events`. If you delete the Kafka topic
(`docker compose down -v`), delete that checkpoint folder too.

`--as-of-date` is "today" for the settlement window: an approved transaction with no
settlement is `PENDING_SETTLEMENT` while `as_of_date <= business_date + window`
(default 2 days) and `MISSING_SETTLEMENT` after.

## Repository layout

```
payment-recon-platform/
├── docs/architecture.md          # design, AWS→Azure mapping, contracts, rules
├── src/payrecon/
│   ├── schemas/contracts.py      # single source of truth for columns & reference values
│   ├── generator/                # synthetic PSP files + ground-truth manifest
│   ├── ingestion/                # (1.2) Bronze
│   ├── transform/                # (1.3) Silver + quarantine
│   └── reconciliation/           # (1.4) recon + Gold
├── sql/                          # (later) analytical SQL, Snowflake DDL
├── tests/
└── data/                         # generated, git-ignored
    ├── landing/{transactions,settlements}/
    ├── reference/merchants.csv
    └── ground_truth/             # test oracle only — the pipeline never reads it
```

All data is synthetic; card numbers are masked test BINs.
