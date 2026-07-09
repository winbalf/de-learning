# Week 4 — Apache Airflow: DAGs, Operators, Sensors & XComs

**Theme:** Replace cron + bash scripts with production-grade orchestration. Every pipeline run tracked, retried, and visible in a UI.

> **Prerequisites:** [Week 3](../week3/README.md) complete · Docker Compose stack running · `de-postgres` container healthy

---

## Table of Contents
1. [What Airflow is and why it replaces cron](#1-what-airflow-is-and-why-it-replaces-cron)
2. [Folder structure](#2-folder-structure)
3. [Docker Compose stack](#3-docker-compose-stack)
4. [Airflow init — first-time setup](#4-airflow-init--first-time-setup)
5. [Your first DAG — titanic pipeline](#5-your-first-dag--titanic-pipeline)
6. [BashOperator](#6-bashoperator)
7. [BranchPythonOperator](#7-branchpythonoperator)
8. [PythonSensor](#8-pythonsensor)
9. [XComs — passing data between tasks](#9-xcoms--passing-data-between-tasks)
10. [Troubleshooting reference](#10-troubleshooting-reference)
11. [Key concepts](#11-key-concepts)
12. [Week 4 checklist](#12-week-4-checklist)

---

## 1. What Airflow is and why it replaces cron

| Feature | Cron | Airflow |
|---|---|---|
| Scheduling | ✅ | ✅ |
| Retry on failure | ❌ | ✅ automatic |
| Task dependencies | ❌ | ✅ DAG graph |
| Run history | ❌ | ✅ full UI |
| Alerting | ❌ | ✅ email/Slack |
| Backfilling | ❌ | ✅ built-in |
| Parallelism | ❌ | ✅ configurable |

A **DAG** (Directed Acyclic Graph) is a Python file that defines tasks and the order they run. Airflow reads every `.py` file in the `dags/` folder automatically.

---

## 2. Folder structure

```
week4/
├── docker-compose.yml        # Airflow stack — webserver, scheduler, metadata DB
├── .env                      # AIRFLOW_UID=50000
├── dags/
│   ├── titanic_pipeline.py   # Main ETL pipeline
│   ├── bash_operator_demo.py # BashOperator examples
│   ├── branching_demo.py     # BranchPythonOperator
│   └── sensor_demo.py        # PythonSensor
├── logs/                     # Task logs (mounted from container)
└── plugins/                  # Custom operators (empty for now)
```

---

## 3. Docker Compose stack

`docker-compose.yml`:

```yaml
services:

  airflow-db:
    image: postgres:15
    container_name: airflow-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      retries: 5
    restart: unless-stopped

  airflow-webserver:
    image: apache/airflow:2.9.1
    container_name: airflow-webserver
    depends_on:
      airflow-db:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    user: "${AIRFLOW_UID:-50000}:0"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    ports:
      - "8081:8080"
    command: webserver
    networks:
      - default
      - week3_default
    restart: unless-stopped

  airflow-scheduler:
    image: apache/airflow:2.9.1
    container_name: airflow-scheduler
    depends_on:
      airflow-db:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    user: "${AIRFLOW_UID:-50000}:0"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    command: scheduler
    networks:
      - default
      - week3_default
    restart: unless-stopped

networks:
  week3_default:
    external: true   # Bridges to week3 stack so DAGs can reach de-postgres

volumes:
  postgres_data:
```

**Critical networking lesson:** Airflow's metadata DB service must be named differently from your data DB service. Both stacks use a service called `postgres` internally — this causes hostname conflicts when both networks are attached. Solution: rename Airflow's DB service to `airflow-db`.

**Access the UI:**
```bash
wslview http://$(ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1):8081
# Login: admin / admin
```

---

## 4. Airflow init — first-time setup

Run once to migrate the DB and create the admin user:

```bash
# Set UID — use 50000 on WSL2 (avoids getpwuid errors)
echo "AIRFLOW_UID=50000" > .env

# Fix folder permissions
sudo chown -R $USER:$USER ~/de-learning/week4/dags
sudo chown -R 50000:0 ~/de-learning/week4/logs
sudo chmod -R 775 ~/de-learning/week4/logs

# Init
docker compose up airflow-init
# Wait for: "Admin user admin created" then Ctrl+C

# Start the stack
docker compose up -d airflow-db airflow-webserver airflow-scheduler
```

**Common WSL2 issues and fixes:**

| Error | Fix |
|---|---|
| `getpwuid(): uid not found` | Set `AIRFLOW_UID=50000` in `.env` |
| `Permission denied: logs/dag_processor_manager` | `sudo chown -R 50000:0 logs && sudo chmod -R 775 logs` |
| `No module named 'airflow.sensors.time'` | Use `PythonSensor` from `airflow.sensors.python` instead |
| DAG not found after trigger | Wait 15s for scheduler to parse — check `airflow dags list-import-errors` |
| Hostname clash between stacks | Rename Airflow's postgres service to `airflow-db` |

---

## 5. Your first DAG — titanic pipeline

`dags/titanic_pipeline.py`:

```python
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "de-learner",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

DB_CONF = {
    "host": "de-postgres",   # Docker service name — NOT localhost
    "port": 5432,
    "dbname": "delearning",
    "user": "deuser",
    "password": "depassword",
}

def get_conn():
    return psycopg2.connect(**DB_CONF)

def check_source_data(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM titanic')
    row_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    if row_count == 0:
        raise ValueError("titanic table is empty — aborting")
    print(f"Source check passed — {row_count} rows found")
    context["ti"].xcom_push(key="source_row_count", value=row_count)

def run_data_quality(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE "Name" IS NULL)             AS null_names,
            COUNT(*) FILTER (WHERE "Fare" < 0)                 AS negative_fares,
            COUNT(*) FILTER (WHERE "Age" < 0 OR "Age" > 120)  AS invalid_ages,
            COUNT(*) - COUNT(DISTINCT "PassengerId")           AS duplicate_ids
        FROM titanic
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    issues = {
        "null_names": row[0], "negative_fares": row[1],
        "invalid_ages": row[2], "duplicate_ids": row[3],
    }
    failed = {k: v for k, v in issues.items() if v > 0}
    if failed:
        raise ValueError(f"DQ checks failed: {failed}")
    print("All DQ checks passed")

def build_summary(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT "Pclass", COUNT(*), SUM("Survived"),
               ROUND(AVG("Fare")::numeric, 2),
               ROUND(100.0 * SUM("Survived") / COUNT(*)::numeric, 1),
               ROUND(AVG("Age")::numeric, 1)
        FROM titanic GROUP BY "Pclass" ORDER BY "Pclass"
    """)
    rows = cur.fetchall()
    cur.execute("DROP TABLE IF EXISTS titanic_summary")
    cur.execute("""CREATE TABLE titanic_summary (
        pclass INT, total INT, survived NUMERIC,
        avg_fare NUMERIC, survival_pct NUMERIC, avg_age NUMERIC)""")
    cur.executemany("INSERT INTO titanic_summary VALUES (%s,%s,%s,%s,%s,%s)", rows)
    conn.commit()
    cur.close()
    conn.close()
    ti = context["ti"]
    source_count = ti.xcom_pull(key="source_row_count", task_ids="check_source_data")
    print(f"Summary built from {source_count} source rows → {len(rows)} summary rows")
    ti.xcom_push(key="summary_rows", value=len(rows))

def log_pipeline_run(**context):
    conn = get_conn()
    cur = conn.cursor()
    summary_rows = context["ti"].xcom_pull(key="summary_rows", task_ids="build_summary")
    cur.execute("""
        INSERT INTO pipeline_runs (pipeline, status, rows_loaded, finished_at)
        VALUES (%s, 'success', %s, NOW())
    """, (f"titanic_dag_{context['ds']}", summary_rows))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Pipeline run logged — rows={summary_rows}")

with DAG(
    dag_id="titanic_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["titanic", "week4"],
) as dag:

    start = EmptyOperator(task_id="start")
    t1 = PythonOperator(task_id="check_source_data",  python_callable=check_source_data)
    t2 = PythonOperator(task_id="run_data_quality",   python_callable=run_data_quality)
    t3 = PythonOperator(task_id="build_summary",      python_callable=build_summary)
    t4 = PythonOperator(task_id="log_pipeline_run",   python_callable=log_pipeline_run)
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> t4 >> end
```

**Trigger and monitor:**
```bash
docker exec -it --user airflow airflow-scheduler airflow dags unpause titanic_pipeline
docker exec -it --user airflow airflow-scheduler airflow dags trigger titanic_pipeline

# Check task states
docker exec -it --user airflow airflow-scheduler \
    airflow tasks states-for-dag-run titanic_pipeline <run_id>

# Read task log
find ~/de-learning/week4/logs -name "*.log" | grep check_source_data | sort | tail -1 | xargs tail -20
```

---

## 6. BashOperator

Runs shell commands — used for dbt, scripts, health checks.

```python
from airflow.operators.bash import BashOperator

check_postgres = BashOperator(
    task_id="check_postgres_connection",
    bash_command="pg_isready -h de-postgres -p 5432 -U deuser && echo 'DB is ready'",
)

count_rows = BashOperator(
    task_id="count_titanic_rows",
    bash_command="""
        PGPASSWORD=depassword psql \
            -h de-postgres -p 5432 \
            -U deuser -d delearning \
            -c 'SELECT COUNT(*) FROM titanic;'
    """,
)

# Jinja templating — Airflow injects execution date automatically
log_date = BashOperator(
    task_id="log_execution_date",
    bash_command='echo "Running for date: {{ ds }} at {{ ts }}"',
)
```

---

## 7. BranchPythonOperator

Returns a `task_id` (or list) to run next. Skipped branches show pink in the UI.

```python
from airflow.operators.python import BranchPythonOperator

def check_data_volume(**context):
    # ... query row count ...
    if count == 0:
        return "handle_empty_table"
    elif count < 500:
        return "handle_small_dataset"
    else:
        return "handle_full_dataset"

branch = BranchPythonOperator(
    task_id="check_data_volume",
    python_callable=check_data_volume,
)

# Join task after branch — must use trigger_rule
report = PythonOperator(
    task_id="send_completion_report",
    python_callable=send_completion_report,
    trigger_rule="none_failed_min_one_success",
)

start >> branch >> [empty, small, full] >> report >> end
```

**Result with 891 rows:**
```
start → check_data_volume → handle_full_dataset → send_completion_report → end
                          ↛ handle_empty_table   (skipped)
                          ↛ handle_small_dataset (skipped)
```

**`trigger_rule` values:**

| Rule | Meaning |
|---|---|
| `all_success` (default) | Run only if all upstream tasks succeeded |
| `none_failed_min_one_success` | Run if at least one upstream succeeded, none failed — needed after branches |
| `all_done` | Run regardless of upstream state |
| `one_success` | Run if any upstream task succeeded |

---

## 8. PythonSensor

Polls a condition on a schedule until it returns `True`.

```python
from airflow.sensors.python import PythonSensor

def wait_for_titanic_data():
    # Returns True when ready, False to keep waiting
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM titanic")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count > 0

wait_for_data = PythonSensor(
    task_id="wait_for_titanic_data",
    python_callable=wait_for_titanic_data,
    poke_interval=30,    # check every 30 seconds
    timeout=300,         # fail after 5 minutes
    mode="poke",         # hold worker slot while waiting
)
```

**Sensor modes:**

| Mode | Behaviour | Use when |
|---|---|---|
| `poke` | Holds worker slot, checks repeatedly | Short waits (<5 min) |
| `reschedule` | Releases slot between checks | Long waits (hours) — saves resources |

> **Note:** `TimeDeltaSensor` import path varies by Airflow version. Use `PythonSensor` with a `time.sleep()` inside for reliable cross-version behaviour.

---

## 9. XComs — passing data between tasks

XComs (cross-communications) let tasks share small values. Stored in Airflow's metadata DB.

```python
# Push a value
context["ti"].xcom_push(key="source_row_count", value=891)

# Pull it in a downstream task
count = context["ti"].xcom_pull(key="source_row_count", task_ids="check_source_data")
```

**Rules:**
- XComs are for small values (row counts, filenames, flags) — not DataFrames
- Large data should be written to storage (S3, Postgres) and the path passed via XCom
- XComs are visible in the UI: DAG → task → XCom tab

---

## 10. Troubleshooting reference

```bash
# List all DAGs
docker exec -it --user airflow airflow-scheduler airflow dags list

# Check parse errors
docker exec -it --user airflow airflow-scheduler airflow dags list-import-errors

# Trigger a DAG
docker exec -it --user airflow airflow-scheduler airflow dags trigger <dag_id>

# Unpause a DAG
docker exec -it --user airflow airflow-scheduler airflow dags unpause <dag_id>

# Check task states for a run
docker exec -it --user airflow airflow-scheduler \
    airflow tasks states-for-dag-run <dag_id> <run_id>

# Read task log
find ~/de-learning/week4/logs -name "*.log" | grep <task_id> | sort | tail -1 | xargs tail -30

# Check if de-postgres is reachable from Airflow
docker exec -it airflow-scheduler bash -c "nc -zv de-postgres 5432"

# Restart stack cleanly
docker compose down
docker compose up -d airflow-db airflow-webserver airflow-scheduler
```

---

## 11. Key concepts

| Concept | Why it matters |
|---|---|
| DAG | Python file defining tasks and dependencies — Airflow's core unit |
| `default_args` | Applied to every task — owner, retries, retry_delay |
| `catchup=False` | Don't backfill historical runs — almost always what you want |
| `schedule="@daily"` | Cron shorthand — also: `@hourly`, `@weekly`, `"0 6 * * *"` |
| `PythonOperator` | Run any Python function as a task |
| `BashOperator` | Run shell commands — used for dbt, scripts, health checks |
| `BranchPythonOperator` | Return task_id to run next — skips other branches |
| `PythonSensor` | Poll until condition is True — waits for files, APIs, upstream DAGs |
| `trigger_rule` | Controls when a task runs based on upstream states |
| XComs | Pass small values between tasks via Airflow's metadata DB |
| `host: "de-postgres"` | Docker service names resolve as hostnames — never use localhost inside containers |
| `AIRFLOW_UID=50000` | Required on WSL2 — avoids getpwuid permission errors |
| Separate service names | Airflow's DB must not share a name with your data DB across networks |

---

## 12. Week 4 checklist

- [x] Airflow running in Docker Compose (webserver + scheduler + metadata DB)
- [x] WSL2 UID and permissions configured correctly
- [x] Docker network bridged to week3 stack — Airflow reaches `de-postgres`
- [x] Airflow metadata DB renamed to `airflow-db` to avoid hostname clash
- [x] `titanic_pipeline` DAG — full ETL with DQ checks and observability
- [x] PythonOperator — task functions with context and error handling
- [x] XComs — `source_row_count` and `summary_rows` passed between tasks
- [x] BashOperator — pg_isready, psql query, Jinja date templating
- [x] BranchPythonOperator — routed to `handle_full_dataset` (891 rows)
- [x] PythonSensor — fired on first poke, condition met immediately
- [x] `trigger_rule="none_failed_min_one_success"` for post-branch joins
- [x] Task logs readable from filesystem and Airflow UI
- [x] Git commit

---

**Next:** Week 5 — dbt · SQL transformations · models · tests · documentation