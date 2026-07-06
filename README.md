# Data Engineering Study Plan
**Month 1 — Foundations**

> **Setup:** WSL2 Ubuntu on Windows · Python 3.11 · 5–10 hrs/week target

---

## Table of Contents

**Week 1 — Environment, pandas, Parquet, PostgreSQL, shell**
1. [Environment Setup](#1-environment-setup)
2. [Pandas Fundamentals](#2-pandas-fundamentals)
3. [Parquet vs CSV](#3-parquet-vs-csv)
4. [PostgreSQL + SQLAlchemy Pipeline](#4-postgresql--sqlalchemy-pipeline)
5. [Linux Shell Basics](#5-linux-shell-basics)
6. [Bash Pipeline Script](#6-bash-pipeline-script)
7. [Git — Commit Your Work](#7-git--commit-your-work)
8. [Week 1 Key Concepts](#8-week-1-key-concepts)

**Week 2 — Advanced SQL, DB patterns, automation**
9. [Week 2 Prerequisites](#9-week-2-prerequisites)
10. [Window Functions](#10-window-functions)
11. [Recursive CTEs + Query Optimisation](#11-recursive-ctes--query-optimisation)
12. [Reusable DB Connection Patterns](#12-reusable-db-connection-patterns)
13. [Automated Pipeline + Cron Scheduling](#13-automated-pipeline--cron-scheduling)
14. [Week 2 Key Concepts](#14-week-2-key-concepts)

---

# Week 1 — Environment, pandas, Parquet, PostgreSQL, shell

---

## 1. Environment Setup

### 1.1 Update the system
```bash
sudo apt update && sudo apt upgrade -y
```

### 1.2 Install pyenv dependencies
```bash
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
libffi-dev liblzma-dev git
```

### 1.3 Install pyenv
```bash
curl https://pyenv.run | bash
```

Add pyenv to your shell:
```bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc
```

Verify:
```bash
pyenv --version
```

### 1.4 Install Python 3.11
```bash
pyenv install 3.11.9
pyenv global 3.11.9
python --version   # should print Python 3.11.9
pip install --upgrade pip
```

### 1.5 Create project folder and virtual environment
```bash
mkdir ~/de-learning
cd ~/de-learning
python -m venv .venv
source .venv/bin/activate
# (.venv) should appear in your terminal prompt
```

### 1.6 Install week 1 libraries
```bash
pip install pandas polars pyarrow jupyter ipykernel sqlalchemy psycopg2-binary
```

### 1.7 Connect VS Code to WSL2
1. Install the **WSL** extension in VS Code (Windows side)
2. In your WSL terminal run:
```bash
code .
```
3. Install the **Python** extension when VS Code prompts
4. Select interpreter: `Ctrl+Shift+P` → `Python: Select Interpreter` → choose `.venv`

### 1.8 Verify everything works

`first_script.py`:
```python
import pandas as pd

data = {
    "city": ["Sydney", "Melbourne", "Brisbane"],
    "population": [5_300_000, 5_100_000, 2_600_000],
    "country": ["AU", "AU", "AU"]
}

df = pd.DataFrame(data)
print(df)
print(f"\nTotal population: {df['population'].sum():,}")
```

```bash
python first_script.py
```

---

## 2. Pandas Fundamentals

### 2.1 Download the dataset
```bash
mkdir ~/de-learning/week1
cd ~/de-learning/week1
curl -O https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
```

### 2.2 Explore and transform — `explore.py`
```python
import pandas as pd

df = pd.read_csv("titanic.csv")

print("=== Shape ===")
print(df.shape)

print("\n=== Column types ===")
print(df.dtypes)

print("\n=== First 5 rows ===")
print(df.head())

print("\n=== Missing values ===")
print(df.isnull().sum())

# Transform
df["Age"] = df["Age"].fillna(df["Age"].median())
df["fare_per_age"] = (df["Fare"] / df["Age"]).round(2)

# Aggregate
print("\n=== Survival rate by passenger class ===")
summary = (
    df.groupby("Pclass")
    .agg(total=("Survived", "count"), survived=("Survived", "sum"))
    .assign(survival_rate=lambda x: (x["survived"] / x["total"] * 100).round(1))
)
print(summary)

# Save to Parquet
df.to_parquet("titanic_cleaned.parquet", index=False)
print("\nSaved to titanic_cleaned.parquet")

df_check = pd.read_parquet("titanic_cleaned.parquet")
print(f"Parquet rows: {len(df_check)}")
```

```bash
python explore.py
```

**Key pandas operations:**

| Operation | Method | DE use case |
|---|---|---|
| Load CSV | `pd.read_csv()` | Ingest raw source data |
| Check nulls | `df.isnull().sum()` | Data quality check |
| Fill nulls | `df.fillna()` | Data cleaning |
| Aggregate | `df.groupby().agg()` | Build summary tables |
| Save Parquet | `df.to_parquet()` | Write to data lake |

---

## 3. Parquet vs CSV

**Why Parquet matters:**
- Columnar storage — reads only the columns you need
- Compressed — smaller files on disk
- Schema embedded — data types preserved
- Industry standard — S3, GCS, Snowflake, BigQuery, Spark all use it

### Benchmark — `parquet_vs_csv.py`
```python
import pandas as pd
import time
import os

csv_size = os.path.getsize("titanic.csv") / 1024
parquet_size = os.path.getsize("titanic_cleaned.parquet") / 1024
print(f"CSV size:     {csv_size:.1f} KB")
print(f"Parquet size: {parquet_size:.1f} KB")

runs = 100

start = time.time()
for _ in range(runs):
    pd.read_csv("titanic.csv")
csv_time = (time.time() - start) / runs * 1000

start = time.time()
for _ in range(runs):
    pd.read_parquet("titanic_cleaned.parquet")
parquet_time = (time.time() - start) / runs * 1000

print(f"CSV avg read:     {csv_time:.2f} ms")
print(f"Parquet avg read: {parquet_time:.2f} ms")

# Column pruning — Parquet's killer feature
start = time.time()
for _ in range(runs):
    pd.read_parquet("titanic_cleaned.parquet", columns=["Survived", "Pclass", "Fare"])
pruned_time = (time.time() - start) / runs * 1000

print(f"Parquet (3 columns only): {pruned_time:.2f} ms")
print("CSV has no equivalent — it always reads all columns")
```

> **Interview answer:** "Parquet is columnar, compressed, and supports predicate pushdown and column pruning — that's why data lakes use it instead of CSV."

---

## 4. PostgreSQL + SQLAlchemy Pipeline

### 4.1 Start PostgreSQL in Docker
```bash
sudo apt install -y docker.io
sudo service docker start
sudo usermod -aG docker $USER
newgrp docker

docker run --name de-postgres \
  -e POSTGRES_USER=deuser \
  -e POSTGRES_PASSWORD=depassword \
  -e POSTGRES_DB=delearning \
  -p 5433:5432 \
  -d postgres:15

docker ps   # verify running
```

> **If the container stops and credentials fail:** remove and recreate it, then re-run `db_pipeline.py` to reload the data. Docker containers don't persist data by default — we fix this in Week 3 with volumes.

### 4.2 ETL pipeline — `db_pipeline.py`
```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

df = pd.read_parquet("titanic_cleaned.parquet")
df = df.drop(columns=["Cabin"])
df["Embarked"] = df["Embarked"].fillna("S")

print(f"Loading {len(df)} rows into PostgreSQL...")
df.to_sql("titanic", con=engine, if_exists="replace", index=False, chunksize=500)
print("Write complete.\n")

with engine.connect() as conn:
    result = pd.read_sql(
        text('SELECT "Pclass" as pclass, COUNT(*) as total, AVG("Fare") as avg_fare FROM titanic GROUP BY "Pclass" ORDER BY "Pclass"'),
        conn
    )
    print("=== Avg fare by class ===")
    print(result)

    result2 = pd.read_sql(text("""
        SELECT "Name" as name, "Pclass" as pclass, "Fare" as fare,
            ROUND(AVG("Fare") OVER (PARTITION BY "Pclass")::numeric, 2) as class_avg_fare,
            ROUND(("Fare" - AVG("Fare") OVER (PARTITION BY "Pclass"))::numeric, 2) as diff_from_avg
        FROM titanic
        ORDER BY diff_from_avg DESC
        LIMIT 10
    """), conn)
    print("\n=== Top 10 passengers who paid most above their class average ===")
    print(result2[["name", "pclass", "fare", "class_avg_fare", "diff_from_avg"]])

print("\nPipeline complete.")
```

**Full pipeline pattern:**
```
CSV → pandas (clean) → Parquet → PostgreSQL → SQL window functions → DataFrame
```

---

## 5. Linux Shell Basics

### 5.1 Create practice files
```bash
cd ~/de-learning/week1

cat > pipeline.log << 'EOF'
2024-01-15 08:00:01 INFO  Pipeline started
2024-01-15 08:00:02 INFO  Loading titanic.csv - 891 rows found
2024-01-15 08:00:04 WARNING Cabin column has 77% nulls - dropping column
2024-01-15 08:00:07 INFO  Write complete - 891 rows inserted
2024-01-15 08:00:08 ERROR Connection timeout on retry attempt 1
2024-01-15 08:00:10 INFO  Pipeline finished successfully
2024-01-15 08:01:02 ERROR Failed to connect to PostgreSQL - connection refused
2024-01-15 08:01:03 ERROR Pipeline aborted
EOF

mkdir -p data/raw data/processed
touch data/raw/sales_jan.csv data/raw/sales_feb.csv data/raw/sales_mar.csv
touch data/processed/sales_jan_clean.csv
touch notes.txt config.yaml
```

### 5.2 grep — search for patterns
```bash
grep "ERROR" pipeline.log                  # find all error lines
grep "INFO" pipeline.log | wc -l           # count info lines
grep -i "warning" pipeline.log             # case-insensitive
grep -v "INFO" pipeline.log                # everything EXCEPT INFO
grep "ERROR\|WARNING" pipeline.log         # match ERROR or WARNING
```

### 5.3 find — locate files
```bash
find . -name "*.csv"                       # all CSV files
find . -name "*.csv" -path "*/raw/*"       # CSVs inside raw/ only
find . -type f -name "*.log"               # files ending in .log
find . -type f -newer notes.txt            # files newer than notes.txt
```

### 5.4 pipes — chain commands
```bash
cat pipeline.log | grep "ERROR" | wc -l           # count errors
cat pipeline.log | grep "ERROR" | sort             # sort error lines
cat pipeline.log | awk '{print $3}' | sort | uniq -c  # count by log level
find . -name "*.csv" | xargs wc -l                # line count of all CSVs
```

---

## 6. Bash Pipeline Script

`run_pipeline.sh`:
```bash
#!/bin/bash
set -e

LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"
DATA_DIR="$HOME/de-learning/week1"
VENV="$HOME/de-learning/.venv"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

log "INFO  Pipeline started"
source "$VENV/bin/activate"

cd "$DATA_DIR"

if [ -f "titanic.csv" ]; then
  log "INFO  titanic.csv already exists — skipping download"
else
  log "INFO  Downloading titanic.csv"
  curl -s -O https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
fi

ROW_COUNT=$(tail -n +2 titanic.csv | wc -l)
log "INFO  titanic.csv has $ROW_COUNT data rows"

if [ "$ROW_COUNT" -lt 100 ]; then
  log "ERROR File has fewer than 100 rows — aborting"
  exit 1
fi

if python db_pipeline.py >> "$LOG_FILE" 2>&1; then
  log "INFO  ETL complete"
else
  log "ERROR ETL script failed"
  exit 1
fi

log "INFO  Pipeline finished successfully"
grep "ERROR" "$LOG_FILE" || echo "No errors found"
```

```bash
chmod +x run_pipeline.sh
bash run_pipeline.sh   # always use bash, not sh — source is a bash built-in
```

---

## 7. Git — Commit Your Work

```bash
cd ~/de-learning
git init

cat > .gitignore << 'EOF'
.venv/
__pycache__/
*.pyc
.env
*.parquet
*.log
logs/
EOF

git add .
git commit -m "week1: pandas ETL pipeline - titanic CSV to PostgreSQL"
```

---

## 8. Week 1 Key Concepts

| Concept | Why it matters |
|---|---|
| Virtual environments | Isolates dependencies per project — always activate before working |
| Parquet over CSV | Columnar, compressed, column pruning — standard in data lakes |
| SQLAlchemy engine | Standard Python↔DB connector — used by pandas, dbt, Airflow |
| `set -e` in bash | Stops script on first failure — prevents silent partial runs |
| `tee` in bash | Prints to terminal AND writes to log file simultaneously |
| Window functions | Core DE/analytics SQL — ROW_NUMBER, LAG, AVG OVER PARTITION BY |
| `grep + pipes` | How DEs debug logs and monitor pipelines from the command line |

**Week 1 checklist:**
- [x] Python 3.11 + pyenv + virtual environment
- [x] pandas — load, clean, transform, aggregate
- [x] Parquet read/write + column pruning
- [x] PostgreSQL via Docker + SQLAlchemy
- [x] Window functions from Python
- [x] Linux shell — grep, find, pipes, awk
- [x] Bash pipeline script with logging
- [x] Git commit

---

# Week 2 — Advanced SQL, DB patterns, automation

---

## 9. Week 2 Prerequisites

### 9.1 Start the environment
```bash
cd ~/de-learning
source .venv/bin/activate
docker start de-postgres
mkdir week2 && cd week2
```

### 9.2 Reload titanic data if container was recreated
```bash
cd ~/de-learning/week1
python db_pipeline.py
cd ~/de-learning/week2
```

### 9.3 Verify DB is reachable
```bash
docker exec -it de-postgres psql -U deuser -d delearning -c "SELECT COUNT(*) FROM titanic"
```

---

## 10. Window Functions

Window functions compute across related rows without collapsing them — unlike `GROUP BY` which returns one row per group. They are used daily in DE for deduplication, period-over-period comparisons, running totals, and rankings.

### Script — `window_functions.py`

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

with engine.connect() as conn:

    # ROW_NUMBER — first passenger per class (used for deduplication)
    r1 = pd.read_sql(text("""
        SELECT * FROM (
            SELECT "Name" as name, "Pclass" as pclass, "PassengerId" as passengerid,
                ROW_NUMBER() OVER (PARTITION BY "Pclass" ORDER BY "PassengerId") as row_num
            FROM titanic
        ) t WHERE row_num = 1
    """), conn)
    print("=== ROW_NUMBER ===")
    print(r1)

    # RANK vs DENSE_RANK — fare ranking within class
    r2 = pd.read_sql(text("""
        SELECT "Name" as name, "Pclass" as pclass, "Fare" as fare,
            RANK()       OVER (PARTITION BY "Pclass" ORDER BY "Fare" DESC) as rank,
            DENSE_RANK() OVER (PARTITION BY "Pclass" ORDER BY "Fare" DESC) as dense_rank
        FROM titanic WHERE "Pclass" = 1
        ORDER BY "Fare" DESC LIMIT 10
    """), conn)
    print("\n=== RANK vs DENSE_RANK ===")
    print(r2)

    # LAG / LEAD — compare each fare to previous/next passenger
    r3 = pd.read_sql(text("""
        SELECT "PassengerId" as passengerid, "Name" as name, "Fare" as fare,
            LAG("Fare")  OVER (ORDER BY "PassengerId") as prev_fare,
            LEAD("Fare") OVER (ORDER BY "PassengerId") as next_fare,
            ROUND(("Fare" - LAG("Fare") OVER (ORDER BY "PassengerId"))::numeric, 2) as diff_from_prev
        FROM titanic ORDER BY "PassengerId" LIMIT 10
    """), conn)
    print("\n=== LAG / LEAD ===")
    print(r3[["passengerid", "name", "fare", "prev_fare", "diff_from_prev"]])

    # Running total + 3-row moving average
    r4 = pd.read_sql(text("""
        SELECT "PassengerId" as passengerid, "Pclass" as pclass, "Fare" as fare,
            SUM("Fare") OVER (PARTITION BY "Pclass" ORDER BY "PassengerId") as running_total,
            ROUND(AVG("Fare") OVER (
                PARTITION BY "Pclass" ORDER BY "PassengerId"
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )::numeric, 2) as moving_avg_3
        FROM titanic WHERE "Pclass" = 1
        ORDER BY "PassengerId" LIMIT 10
    """), conn)
    print("\n=== Running total + moving average ===")
    print(r4)

    # NTILE — split into fare quartiles
    r5 = pd.read_sql(text("""
        SELECT "Name" as name, "Fare" as fare,
            NTILE(4) OVER (ORDER BY "Fare") as fare_quartile
        FROM titanic ORDER BY "Fare" DESC LIMIT 12
    """), conn)
    print("\n=== NTILE quartiles ===")
    print(r5)
```

**Window function reference:**

| Function | Real DE use case |
|---|---|
| `ROW_NUMBER` | Deduplicate — keep latest row per customer/ID |
| `RANK / DENSE_RANK` | Top-N per category, leaderboards |
| `LAG / LEAD` | Period-over-period change, gap detection |
| `SUM OVER` | Cumulative revenue, running event counts |
| `AVG OVER ROWS BETWEEN` | Rolling KPIs, trend smoothing |
| `NTILE` | Quartile/decile segmentation |

> **Interview answer:** "Window functions compute across a set of rows related to the current row without collapsing them — unlike GROUP BY which returns one row per group."

---

## 11. Recursive CTEs + Query Optimisation

### Script — `query_optimization.py`

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

with engine.connect() as conn:

    # Recursive CTE — generate sequence
    r1 = pd.read_sql(text("""
        WITH RECURSIVE counter(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM counter WHERE n < 10
        )
        SELECT n FROM counter
    """), conn)
    print("=== Recursive CTE — sequence ===")
    print(r1.T)

    # Recursive CTE — org chart traversal
    conn.execute(text("""
        DROP TABLE IF EXISTS employees;
        CREATE TABLE employees (id INT, name TEXT, role TEXT, manager_id INT);
        INSERT INTO employees VALUES
            (1,'Alice','CTO',NULL),(2,'Bob','Data Lead',1),
            (3,'Carol','Data Engineer',2),(4,'Dave','Data Engineer',2),
            (5,'Eve','Analytics Lead',1),(6,'Frank','Data Analyst',5),
            (7,'Grace','Data Analyst',5);
    """))
    conn.commit()

    r2 = pd.read_sql(text("""
        WITH RECURSIVE org_chart AS (
            SELECT id, name, role, manager_id, 0 AS depth, name::TEXT AS path
            FROM employees WHERE manager_id IS NULL
            UNION ALL
            SELECT e.id, e.name, e.role, e.manager_id, oc.depth + 1,
                   oc.path || ' → ' || e.name
            FROM employees e JOIN org_chart oc ON e.manager_id = oc.id
        )
        SELECT depth, name, role, path FROM org_chart ORDER BY path
    """), conn)
    print("\n=== Recursive CTE — org chart ===")
    print(r2.to_string(index=False))

    # EXPLAIN ANALYZE — before and after index
    print("\n=== EXPLAIN ANALYZE — without index (cold) ===")
    plan1 = pd.read_sql(text('EXPLAIN ANALYZE SELECT * FROM titanic WHERE "Fare" > 100'), conn)
    for row in plan1["QUERY PLAN"]:
        print(row)

    conn.execute(text('CREATE INDEX IF NOT EXISTS idx_titanic_fare ON titanic("Fare")'))
    conn.commit()

    print("\n=== EXPLAIN ANALYZE — with index (warm) ===")
    plan2 = pd.read_sql(text('EXPLAIN ANALYZE SELECT * FROM titanic WHERE "Fare" > 100'), conn)
    for row in plan2["QUERY PLAN"]:
        print(row)

    # Anti-pattern vs optimised
    bad = pd.read_sql(text("SELECT * FROM titanic"), conn)
    bad_result = bad[bad["Fare"] > 100][["Name", "Fare", "Pclass"]]
    print(f"\nAnti-pattern: loaded {len(bad)} rows, kept {len(bad_result)}")

    good = pd.read_sql(text("""
        SELECT "Name" as name, "Fare" as fare, "Pclass" as pclass
        FROM titanic WHERE "Fare" > 100 ORDER BY "Fare" DESC
    """), conn)
    print(f"Optimised:    loaded {len(good)} rows directly")
```

**What to look for in EXPLAIN ANALYZE:**

| Term | Meaning |
|---|---|
| `Seq Scan` | Full table read — slow at scale |
| `Bitmap Index Scan` | Uses index — fast for filtered queries |
| `actual time` | Real execution time in ms |
| `Planning Time` | Time to build the query plan — drops sharply after warm cache |

> **Interview answer:** "Always filter and project in SQL before pulling data into Python — the database is optimised for scanning and filtering; Python is not."

---

## 12. Reusable DB Connection Patterns

### `db_utils.py` — production-grade connector

```python
import logging
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text, pool
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_engine(host="localhost", port=5433, db="delearning",
               user="deuser", password="depassword",
               pool_size=5, max_overflow=2):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(
        url,
        poolclass=pool.QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True    # test connection before using it
    )
    log.info(f"Engine created — pool_size={pool_size}")
    return engine


@contextmanager
def get_connection(engine):
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
        log.info("Transaction committed")
    except SQLAlchemyError as e:
        conn.rollback()
        log.error(f"Transaction rolled back: {e}")
        raise
    finally:
        conn.close()
        log.info("Connection closed")


def run_query(engine, sql, params=None):
    with get_connection(engine) as conn:
        result = pd.read_sql(text(sql), conn, params=params)
        log.info(f"Query returned {len(result)} rows")
        return result


def load_dataframe(engine, df, table, if_exists="append", chunksize=1000):
    try:
        df.to_sql(table, con=engine, if_exists=if_exists, index=False, chunksize=chunksize)
        log.info(f"Loaded {len(df)} rows into {table}")
    except SQLAlchemyError as e:
        log.error(f"Failed to load into {table}: {e}")
        raise
```

**Key patterns and why they matter:**

| Pattern | Why it matters |
|---|---|
| Connection pool | Never open a new connection per query — expensive at scale |
| `pool_pre_ping=True` | Detects stale connections before using them |
| Context manager | Guarantees commit/rollback/close even if script crashes |
| Parameterised queries | Prevents SQL injection — never use f-strings to build SQL |
| Atomic rollback | Either all inserts succeed or none are committed |

---

## 13. Automated Pipeline + Cron Scheduling

### `auto_pipeline.sh` — scheduled pipeline runner with log rotation

```bash
#!/bin/bash
set -e

BASE_DIR="$HOME/de-learning"
WEEK2_DIR="$BASE_DIR/week2"
VENV="$BASE_DIR/.venv"
LOG_DIR="$WEEK2_DIR/logs"
MAX_LOGS=5

mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" | tee -a "$LOG_FILE"
}

rotate_logs() {
    local count
    count=$(ls "$LOG_DIR"/pipeline_*.log 2>/dev/null | wc -l)
    if [ "$count" -gt "$MAX_LOGS" ]; then
        ls -t "$LOG_DIR"/pipeline_*.log | tail -n +$((MAX_LOGS + 1)) | xargs rm -f
        log "INFO" "Log rotation: kept $MAX_LOGS most recent logs"
    fi
}

check_db() {
    docker exec de-postgres pg_isready -U deuser -d delearning > /dev/null 2>&1
}

log "INFO" "Pipeline started"
source "$VENV/bin/activate"
log "INFO" "Virtual environment activated"

if check_db; then
    log "INFO" "Database is ready"
else
    log "ERROR" "Database not reachable — aborting"
    exit 1
fi

log "INFO" "Running summary refresh..."
cd "$WEEK2_DIR"

python - << 'PYEOF'
import sys
sys.path.insert(0, '.')
from db_utils import get_engine, run_query, load_dataframe

engine = get_engine()
summary = run_query(engine, """
    SELECT "Pclass" as pclass,
           COUNT(*)                            AS total,
           SUM("Survived")                     AS survived,
           ROUND(AVG("Fare")::numeric, 2)      AS avg_fare,
           ROUND(100.0 * SUM("Survived") / COUNT(*)::numeric, 1) AS survival_pct,
           ROUND(AVG("Age")::numeric, 1)       AS avg_age
    FROM titanic GROUP BY "Pclass" ORDER BY "Pclass"
""")
print(summary.to_string(index=False))
load_dataframe(engine, summary, "titanic_summary", if_exists="replace")
engine.dispose()
PYEOF

log "INFO" "Summary refresh complete"
rotate_logs
log "INFO" "Pipeline finished successfully"
```

```bash
chmod +x auto_pipeline.sh
bash auto_pipeline.sh
```

### Cron scheduling

```bash
crontab -e   # open crontab (pick nano if prompted)
```

Add this line:
```
* * * * * bash /home/<your-username>/de-learning/week2/auto_pipeline.sh >> /home/<your-username>/de-learning/week2/logs/cron.log 2>&1
```

Check logs after 2 minutes:
```bash
ls -lh ~/de-learning/week2/logs/
cat ~/de-learning/week2/logs/cron.log
```

Remove the job when done:
```bash
crontab -e   # delete the line, save and exit
crontab -l   # verify it's gone
```

**Cron syntax reference:**
```
* * * * *  command
│ │ │ │ └── day of week (0=Sun)
│ │ │ └──── month (1-12)
│ │ └────── day of month (1-31)
│ └──────── hour (0-23)
└────────── minute (0-59)

0 6 * * *        every day at 6am
0 6 * * 1        every Monday at 6am
*/15 * * * *     every 15 minutes
0 0 1 * *        first day of every month at midnight
```

### Git — commit week 2

```bash
cd ~/de-learning
git add week2/
git commit -m "week2: window functions, query optimisation, db_utils, automated pipeline + cron"
```

---

## 14. Week 2 Key Concepts

| Concept | Why it matters |
|---|---|
| `ROW_NUMBER()` | Deduplication — keep one row per partition |
| `LAG() / LEAD()` | Period-over-period change, gap detection |
| `ROWS BETWEEN` frame | Control which rows the window sees — rolling averages |
| Recursive CTE | Traverse hierarchies without recursive Python loops |
| `EXPLAIN ANALYZE` | Read query plans before blaming slow pipelines on "the database" |
| Index on filter columns | Turns sequential scans into index scans on large tables |
| Pushdown to SQL | Filter and project in SQL — less data over the wire |
| Connection pool | Reuse connections — never create one per query |
| Context manager | Atomic transactions — commit on success, rollback on failure |
| Parameterised queries | SQL injection prevention — never f-string your SQL |
| Log rotation | Keep disk usage bounded in long-running pipelines |
| Cron scheduling | How pipelines run automatically in production |

**Week 2 checklist:**
- [x] Window functions — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- [x] Running totals and moving averages with ROWS BETWEEN
- [x] NTILE for bucket segmentation
- [x] Recursive CTEs — sequence and org chart traversal
- [x] EXPLAIN ANALYZE — query plans before and after indexing
- [x] Anti-pattern vs optimised SQL — pushdown vs Python filtering
- [x] Reusable DB engine with connection pooling
- [x] Context manager for atomic transactions + rollback demo
- [x] Parameterised queries
- [x] Automated bash pipeline with DB health check + log rotation
- [x] Cron job scheduling
- [x] Git commit

---

**Next:** Week 3 — Docker Compose, named volumes (persistent data), multi-service stacks