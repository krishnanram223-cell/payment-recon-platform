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
