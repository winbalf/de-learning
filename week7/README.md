# Chicago Food Inspections — End-to-End Data Engineering Pipeline

A production-style data pipeline built on the City of Chicago's public food inspection dataset. Demonstrates the full modern DE stack: ingestion, transformation, orchestration, and large-scale processing.

---

## Dataset

**Source:** [City of Chicago Data Portal](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)
**Size used:** 20,000 inspections (2010–2024)
**Key columns:** business name, facility type, risk level, inspection result, violation descriptions, date, coordinates

---

## Architecture

```
inspections_sample.csv (20k rows)
          │
          ▼ Python (ingest.py)
┌─────────────────────────┐
│  PostgreSQL              │
│  public.raw_inspections  │
└────────┬────────────────┘
         │ dbt (inspections_dbt/)
         ▼
┌─────────────────────────────────────────────────┐
│  dbt models (inspections_dbt schema)             │
│  stg_inspections (view)                          │
│  int_inspections_enriched (view)                 │
│  fct_inspection_summary (table)                  │
│  fct_yearly_trends (table)                       │
│  fct_top_failing_businesses (table)              │
└────────┬────────────────────────────────────────┘
         │ Apache Airflow (chicago_inspections_pipeline DAG)
         ▼
┌─────────────────────────────────────────────────┐
│  Orchestration: weekly schedule                  │
│  source check → DQ → branch → dbt → audit log   │
└────────┬────────────────────────────────────────┘
         │ PySpark (spark_inspections.py)
         ▼
┌─────────────────────────────────────────────────┐
│  Medallion Architecture (Parquet)                │
│  Bronze (raw, 9.9 MB)                            │
│  Silver (clean+DQ, 10.1 MB, partitioned by year) │
│  Gold (aggregated, 1.6 KB)                       │
└─────────────────────────────────────────────────┘
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + pandas | CSV → PostgreSQL with column normalisation |
| Storage | PostgreSQL 15 (Docker) | Raw and transformed data |
| Transform | dbt 1.8 | SQL models with tests and documentation |
| Orchestration | Apache Airflow 2.9 | Scheduling, retries, branching, audit log |
| Scale | PySpark 3.5 | Medallion pipeline, partitioned Parquet |
| Infrastructure | Docker Compose | Reproducible multi-service stack |

---

## Key findings

**Pass rates improved significantly over 14 years:**
- 2011: 48.4% pass rate (worst year)
- 2024: 73.4% pass rate (best year)
- 2017 was the lowest point — 46.1% pass rate, rising violation counts

**Risk level vs pass rate (counterintuitive):**
- High risk: 63.4% pass rate
- Medium risk: 62.4% pass rate
- Low risk: 58.9% pass rate

High-risk establishments face more frequent inspections and are more likely to be compliant as a result.

**Most failures by category:**
- SUBWAY: 29 failures out of 141 High Risk inspections
- DUNKIN DONUTS: 24 failures out of 89 Medium Risk inspections
- PARSON'S CHICKEN & FISH: 0% pass rate (7 failures, 7 inspections)

**Violation patterns:**
- Average violations per inspection increased from 3.5 (2010) to 9.0 (2019) then declined
- The 2019 spike may reflect stricter enforcement before plateauing

---

## Running the pipeline

### Prerequisites
```bash
# Start the data stack
cd ~/de-learning/week3 && docker compose up -d
cd ~/de-learning/week4 && docker compose up -d

# Activate Python environment
cd ~/de-learning && source .venv/bin/activate
```

### 1. Ingest raw data
```bash
cd week7
python ingest.py
# Loads 20,000 rows into public.raw_inspections
```

### 2. Run dbt transformations
```bash
cd week7/inspections_dbt
dbt run           # build all 5 models
dbt test          # run 9 tests — all should pass
dbt docs generate # builds manifest.json + catalog.json (needs DB)
dbt docs serve --port 8082  # view lineage graph
# If port is in use: use --port 8083, or free it with:
#   lsof -i :8082   # then kill <pid>
```

### 3. Trigger Airflow DAG
```bash
docker exec -it --user airflow airflow-scheduler \
    airflow dags trigger chicago_inspections_pipeline
# Weekly schedule: source check → DQ → dbt → audit log
```

### 4. Run PySpark medallion pipeline
```bash
cd week7
python spark_inspections.py
# Bronze → Silver (partitioned by year) → Gold (3 aggregation tables)
```

---

## dbt models

### stg_inspections (view)
Cleans raw data: standardises risk levels (High/Medium/Low), parses dates, counts violations from pipe-delimited text, normalises result values.

### int_inspections_enriched (view)
Adds derived fields: era bucket (2010-2014, 2015-2019, 2020+), facility category (Restaurant, School, Grocery, etc.), high-risk flag, multiple violations flag.

### fct_inspection_summary (table)
Pass/fail rates aggregated by facility category and risk level. 19 rows covering all combinations. Used for risk-based monitoring.

### fct_yearly_trends (table)
Annual pass rates, inspection volumes, and average violations from 2010–2024. Shows compliance trend over time.

### fct_top_failing_businesses (table)
Top 50 businesses by failure count, with minimum 3 inspections. Pass rate, avg violations, last inspection date. Input for risk monitoring dashboards.

---

## Tests

**9 automated dbt tests:**
- `unique` + `not_null` on `inspection_id`
- `accepted_values` on `result` (Pass/Fail/No Entry/Out of Business/Other)
- `accepted_values` on `risk_level` (High/Medium/Low/Unknown)
- `not_null` on `passed` and `risk_level`
- Singular: violation count non-negative
- Singular: pass rate bounds (0–100)

All 9 pass on every run.

---

## Airflow DAG: `chicago_inspections_pipeline`

```
start
  → check_source_data    (verify raw_inspections has rows, push count via XCom)
  → run_dq_checks        (null IDs, duplicates, null results threshold)
  → check_dbt_needed     (BranchPythonOperator — always rebuilds for now)
  → run_dbt_models       (BashOperator — runs dbt in production)
  → log_pipeline_completion (writes to pipeline_runs audit table)
  → end
```

Schedule: `@weekly` · Retries: 1 · Retry delay: 2 minutes

---

## PySpark medallion output

| Layer | Rows | Size | Details |
|---|---|---|---|
| Bronze | 20,000 | 9.9 MB | Raw CSV + metadata columns |
| Silver | 20,000 | 10.1 MB | Clean, typed, partitioned by `inspection_year` |
| Gold | 3–13 | 1.6 KB | Aggregated by risk, year, business |

Silver partition structure:
```
output/silver/
  inspection_year=2010/
  inspection_year=2011/
  ...
  inspection_year=2024/
```

Querying a single year reads only that folder — partition pruning at work.

---

## Project structure

```
week7/
├── ingest.py                    # CSV → PostgreSQL ingestion
├── inspections_sample.csv       # 20k row sample
├── spark_inspections.py         # PySpark medallion pipeline
├── output/
│   ├── bronze/                  # Raw Parquet
│   ├── silver/                  # Clean Parquet, partitioned by year
│   └── gold/                    # Aggregated Parquet
└── inspections_dbt/
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/
    │   │   ├── sources.yml
    │   │   ├── stg_inspections.sql
    │   │   └── stg_inspections.yml
    │   ├── intermediate/
    │   │   ├── int_inspections_enriched.sql
    │   │   └── int_inspections_enriched.yml
    │   └── marts/
    │       ├── fct_inspection_summary.sql
    │       ├── fct_yearly_trends.sql
    │       └── fct_top_failing_businesses.sql
    └── tests/
        ├── test_violation_count_non_negative.sql
        └── test_pass_rate_bounds.sql
```