# Data Engineering Learning Portfolio

A hands-on, end-to-end data engineering curriculum built from scratch on WSL2 Ubuntu.
Every week produces working code, tested pipelines, and documented outputs.

---

## Stack

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.9-red)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Spark](https://img.shields.io/badge/PySpark-3.5-yellow)

| Tool | Purpose |
|---|---|
| Python + pandas | Data ingestion, transformation, scripting |
| PostgreSQL | Relational storage, window functions, indexing |
| Docker Compose | Reproducible multi-service stacks |
| Apache Airflow | Pipeline orchestration, scheduling, monitoring |
| dbt | SQL transformations, testing, documentation |
| PySpark | Large-scale data processing, medallion architecture |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                            │
│              CSV files / Public APIs                        │
└──────────────────────┬──────────────────────────────────────┘
                       │ Extract + Load (Python)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   PostgreSQL (Docker)                       │
│         Raw tables · Pipeline audit log · DQ log           │
└──────┬────────────────────┬────────────────────────────────┘
       │ Transform (dbt)    │ Orchestrate (Airflow)
       ▼                    ▼
┌─────────────┐    ┌────────────────────┐
│  dbt models │    │   Airflow DAGs     │
│  staging    │    │   Daily schedule   │
│  marts      │    │   Retries + alerts │
└─────────────┘    └────────────────────┘
       │
       │ Scale (PySpark)
       ▼
┌─────────────────────────────────────────────────────────────┐
│              Medallion Architecture (Parquet)               │
│         Bronze (raw) → Silver (clean) → Gold (agg)         │
└─────────────────────────────────────────────────────────────┘
```

---

## Weekly Progress

### ✅ Week 1 — Python, pandas, PostgreSQL, Shell
- Set up Python 3.11 + pyenv + virtual environments on WSL2
- Built an ETL pipeline: CSV → pandas → Parquet → PostgreSQL
- Benchmarked Parquet vs CSV (column pruning, compression)
- Connected Python to PostgreSQL via SQLAlchemy
- Wrote bash pipeline scripts with logging and error handling

**Key files:** `week1/explore.py` · `week1/db_pipeline.py` · `week1/run_pipeline.sh`

---

### ✅ Week 2 — Advanced SQL & DB Patterns
- Window functions: ROW_NUMBER, RANK, LAG/LEAD, running totals, moving averages
- Recursive CTEs for hierarchy traversal
- EXPLAIN ANALYZE: query plans, index creation, 21x speedup
- Production DB patterns: connection pooling, context managers, atomic transactions
- Automated bash pipeline with log rotation and cron scheduling

**Key files:** `week2/window_functions.py` · `week2/db_utils.py` · `week2/auto_pipeline.sh`

---

### ✅ Week 3 — Docker Compose & Pipeline Observability
- Docker Compose stack: Postgres + pgAdmin + custom pipeline container
- Named volumes — data persists across container restarts
- Init SQL scripts — schema bootstrapped automatically on first boot
- Dockerfile with layer caching and environment variable injection
- Pipeline observability: every run tracked in `pipeline_runs` + `data_quality_log`

**Key files:** `week3/docker-compose.yml` · `week3/Dockerfile` · `week3/pipeline_runner.py`

---

### ✅ Week 4 — Apache Airflow
- Airflow running in Docker Compose (webserver + scheduler + metadata DB)
- DAGs with task dependencies, retries, and scheduling
- PythonOperator, BashOperator, BranchPythonOperator, PythonSensor
- XComs for passing data between tasks
- Full titanic pipeline DAG: source check → DQ → summary → audit log

**Key files:** `week4/dags/titanic_pipeline.py` · `week4/dags/branching_demo.py` · `week4/dags/sensor_demo.py`

---

### ✅ Week 5 — dbt
- Three-layer architecture: staging → intermediate → marts
- `stg_titanic`: PascalCase → snake_case, null handling, derived columns
- `int_passengers_enriched`: age groups, fare tiers, solo traveller flag
- `fct_survival_by_class` + `fct_survival_by_demographics`: final analytical tables
- 26 automated tests: unique, not_null, accepted_values, 2 custom singular tests
- Generated documentation with lineage graph

**Key files:** `week5/titanic_dbt/models/` · `week5/titanic_dbt/tests/`

---

### ✅ Week 6 — PySpark & Medallion Architecture
- SparkSession setup with local execution and tuned shuffle partitions
- DataFrame transformations, aggregations, and Spark SQL
- Window functions in PySpark: rank, running totals, moving averages
- Partitioned Parquet: write by column, partition pruning on read
- Full medallion pipeline: Bronze (raw) → Silver (clean+DQ) → Gold (aggregated)
- Query plan analysis with EXPLAIN

**Key files:** `week6/spark_basics.py` · `week6/spark_sql.py` · `week6/medallion.py`

---

### ✅ Week 7 — Capstone & Portfolio
- End-to-end pipeline on a new public dataset
- Full stack: ingest → PostgreSQL → Airflow DAG → dbt models → PySpark → Parquet
- Interview preparation: technical questions, system design

**Key files:** `week7/`

---

## Running the stack

```bash
# Start the data stack (Postgres + pgAdmin)
cd week3 && docker compose up -d

# Start Airflow
cd week4 && docker compose up -d

# Run dbt models
cd week5/titanic_dbt && dbt run && dbt test

# Run PySpark medallion pipeline
cd week6 && source ../.venv/bin/activate && python medallion.py
```

---

## What I learned

The most important non-obvious lessons from building this:

1. **Parquet column pruning** — at scale, reading only the columns you need is more impactful than compression. A 200-column table queried for 3 columns loads 1.5% of the data.

2. **Atomic transactions matter** — week 2's rollback demo proved that partial writes leave your DB in an unknown state. Every pipeline write should be wrapped in a transaction.

3. **Docker networking** — `localhost` inside a container is the container itself, not your machine. Service names resolve as hostnames within a Compose network. This caused the most debugging time in week 4.

4. **dbt `ref()` is a superpower** — the build order is resolved automatically from dependency declarations. No makefile, no manual ordering, no forgetting to rebuild upstream.

5. **Medallion architecture separates concerns** — bronze never changes (audit trail), silver is where DQ happens (catch problems early), gold is optimised for consumption (analysts never touch raw data).

---

## Setup

**Requirements:** WSL2 Ubuntu · Python 3.11 · Docker Desktop · Java 11+

```bash
git clone https://github.com/winbalf/de-learning.git
cd de-learning
python -m venv .venv && source .venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary pyarrow dbt-core==1.8.2 dbt-postgres==1.8.2 pyspark==3.5.1
```