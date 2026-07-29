# Steam Game Price Tracker

An automated **ELT pipeline** that builds daily price history for Steam games — turning point-in-time prices from Steam's Storefront API into a queryable time-series in BigQuery, transformed and tested with dbt, and orchestrated with Airflow.

Steam exposes only the *current* price of a game; it has no price-history endpoint. This pipeline creates that historical dataset itself by capturing a snapshot every day.

```
┌─────────────┐   ┌───────────┐   ┌──────────┐   ┌────────────────────┐
│  Steam API  │──▶│ extract.py│──▶│  JSONL   │──▶│  BigQuery (raw)     │
│ (Storefront)│   │  (Python) │   │  (local) │   │  append-only        │
└─────────────┘   └───────────┘   └──────────┘   └─────────┬──────────┘
                                                            │
                                                            ▼
                                            ┌──────────────────────────────┐
                                            │  dbt                          │
                                            │  staging → marts + 13 tests   │
                                            │  (dedupe, unit conversion)    │
                                            └──────────────┬───────────────┘
                                                           │
        Apache Airflow (Astro CLI) orchestrates daily:     ▼
        extract ▶ load ▶ dbt run ▶ dbt test
```

---

## Why this project

- **Steam has no price-history API** — the pipeline *generates* a historical dataset that doesn't exist at the source. Value compounds over time.
- **Time-series pricing** connects naturally to fintech use cases (tracking, discount behaviour, market comparison across regions).
- **Domain familiarity** — tracking games I actually follow makes it easy to spot anomalies in the data.

A working hypothesis the dataset is designed to explore: *do "trending" games discount more often or more deeply than a curated set of personal picks?*

---

## Tech stack

| Layer | Tool | Role |
|-------|------|------|
| Extraction / Loading | **Python** (`requests`, `google-cloud-bigquery`) | Fetch prices, batch-load to BigQuery |
| Warehouse | **Google BigQuery** (sandbox) | Store raw snapshots + transformed models |
| Transformation | **dbt** (`dbt-bigquery`) | staging → marts, plus data-quality tests |
| Orchestration | **Apache Airflow** (via Astro CLI / Docker) | Schedule and run the pipeline daily |
| Tooling | **uv**, Application Default Credentials | Python/env management, keyless GCP auth |

---

## How it works

**1. Extract** (`src/extract.py`) — Reads a curated list of 50 games from `config/games.csv`, calls the Steam Storefront `appdetails` endpoint for two regions (`th`, `us`), and writes one JSONL file per day. Every game/region produces a row — including a row flagged `api_success = false` when a call fails, so gaps are recorded rather than silently dropped.

**2. Load** (`src/load.py`) — Batch-loads the JSONL file into the BigQuery `raw_price_snapshots` table using an **append-only** load job (the sandbox blocks streaming/DML, so batch loading is the deliberate choice).

**3. Transform** (`steam_dbt/`) — dbt builds two layers:
- `stg_price_snapshots` — deduplicates by `(steam_appid, country_code, snapshot_date)`, keeping the latest ingestion; converts prices from minor units to a NUMERIC currency value.
- `mart_price_history` — joins snapshots with game metadata (category, developer, publisher) for analysis.

**4. Orchestrate** (`airflow/`) — An Airflow DAG runs `extract ▶ load ▶ dbt run ▶ dbt test` daily. If any task fails, downstream tasks don't run — preventing bad data from flowing forward.

---

## Data model

`raw_price_snapshots` — grain: **one game, one region, one day** (11 columns).

| Column | Type | Notes |
|--------|------|-------|
| `steam_appid` | INTEGER (REQUIRED) | Join key |
| `country_code` | STRING (REQUIRED) | Region requested (`th` / `us`) |
| `snapshot_date` | DATE (REQUIRED) | Logical date of the snapshot |
| `initial_price` / `final_price` | INTEGER | Price in **minor units** (satang/cents) |
| `discount_percent` | INTEGER | |
| `currency` | STRING | e.g. `THB`, `USD` |
| `is_free` | BOOLEAN | |
| `api_success` | BOOLEAN (REQUIRED) | Distinguishes "no price" from "call failed" |
| `ingested_at` | TIMESTAMP (REQUIRED) | Physical write time (UTC) |
| `game_name` | STRING | Name as returned by the API |

---

## Design decisions

**ELT, not ETL — the raw layer stays "dumb."** Raw data is loaded unchanged and append-only; all cleaning and deduplication happens in dbt. If transformation logic is ever wrong, it can be fixed and re-run against untouched raw data — nothing is lost.

**Deduplication in dbt, not at load.** The sandbox blocks `MERGE`/`DELETE`, and re-running the pipeline appends duplicates by design (this is normal and traceable). A dbt `ROW_NUMBER()` model keeps the latest row per grain, so the layer analysts query is always clean — the pipeline is idempotent at the output.

**Money as integers, then NUMERIC — never float.** Prices are stored in minor units (integers) in raw and converted to exact `NUMERIC` in staging. Floating-point rounding has no place in financial data.

**Explicit schema over autodetect.** The BigQuery table schema is defined explicitly, with five `REQUIRED` columns forming a first line of data-quality defense at load time — before dbt tests even run. Autodetect would infer types from each day's file, breaking when a day happens to contain only nulls.

**LEFT JOIN preserves price history.** The mart uses a LEFT JOIN from snapshots to game metadata, so a game's price history survives even if it's later removed from the tracked list.

**Configuration separated from logic.** The tracked-games list lives in `config/games.csv`, decoupled from extraction code and reused as a dbt seed — the same file documents *what* is tracked and serves as a queryable dimension.

---

## Data quality (13 dbt tests)

- **`unique`** on a surrogate key (`steam_appid|country_code|snapshot_date`) — proves deduplication works. A composite key can't be tested with the built-in `unique`, so a concatenated key is generated in staging.
- **`not_null`** on identity, key, and audit columns.
- **`accepted_values`** on `country_code` (`th`/`us`) and `category` (`personal`/`trending`) — catches unexpected values.

Tests are placed at the layer where each check is most meaningful: dedupe uniqueness at staging (where dedupe happens), category values at the mart (where category is joined in).

---

## Known limitations

- **Currency conversion assumes 2 decimal places** (`/100`). This holds for THB and USD but not for zero-decimal currencies like JPY — a future multi-region expansion would need currency-aware conversion (the `currency` column is retained for exactly this).
- **Backfill dates the snapshot, not the price.** Because Steam only returns the *current* price, a backfilled run records the correct `snapshot_date` but the price is as-of the run time — a limitation of the source, not the pipeline. The `ingested_at` vs `snapshot_date` split makes this visible.
- **The seed is a copy of the config file** and must be kept in sync manually.
- **Limited history so far.** With only a few days of data, early figures (e.g. average discount by category) are directional, not conclusive — the value grows as snapshots accumulate across sale events.

---

## Project structure

```
steam-price-tracker/
├── config/
│   ├── games.csv                    # tracked games (also a dbt seed)
│   └── schema/raw_price_snapshots.json
├── src/
│   ├── extract.py                   # Steam API → JSONL
│   └── load.py                      # JSONL → BigQuery
├── steam_dbt/
│   ├── models/staging/              # stg_price_snapshots + tests
│   ├── models/marts/                # mart_price_history + tests
│   ├── seeds/games.csv
│   └── profiles.yml                 # oauth (ADC) — no secrets
├── airflow/
│   ├── dags/steam_price_pipeline.py
│   └── docker-compose.override.yml  # mounts code + credentials
├── requirements.txt
├── .env.example
└── .python-version
```

---

## Setup

**Prerequisites:** Python 3.11 (via `uv`), Google Cloud SDK, Docker Desktop, a GCP project with BigQuery enabled.

```bash
# 1. Clone and set up the environment
git clone https://github.com/Jckappa/steam-price-tracker.git
cd steam-price-tracker
uv venv venv --python 3.11
source venv/Scripts/activate          # Git Bash on Windows
uv pip install -r requirements.txt

# 2. Configure environment variables
cp .env.example .env                  # then fill in GCP_PROJECT_ID, BQ_DATASET

# 3. Authenticate to GCP (keyless, via ADC)
gcloud auth application-default login
gcloud config set project <your-project-id>

# 4. Create the raw table
bq --location=asia-southeast1 mk --dataset <dataset>
bq mk --table <project>:<dataset>.raw_price_snapshots config/schema/raw_price_snapshots.json

# 5. Run the pipeline manually
python src/extract.py                 # writes data/snapshots_<date>.jsonl
python src/load.py                    # loads into BigQuery
cd steam_dbt && dbt seed && dbt run && dbt test

# 6. Or run it orchestrated with Airflow
cd airflow && astro dev start         # UI at http://localhost:8080
```

---

## Future improvements

- **Player counts** via the official Web API (`GetNumberOfCurrentPlayers`) to measure popularity quantitatively — turning the category hypothesis into a testable correlation.
- **More regions** — the long-format schema makes this a config change, not a schema change.
- **Partitioning & clustering** on `snapshot_date` / `steam_appid` once the dataset grows large enough for it to matter.
- **Dynamic trending discovery** — refresh the tracked list from Steam's most-played chart automatically.