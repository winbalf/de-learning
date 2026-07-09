# Week 1 — Environment, pandas, Parquet, PostgreSQL & Shell

**Theme:** Set up your DE workstation and build your first end-to-end pipeline — CSV → pandas → Parquet → PostgreSQL.

> **Prerequisites:** WSL2 Ubuntu on Windows · No prior DE tooling required

---

## Table of Contents
1. [Folder structure](#1-folder-structure)
2. [Environment setup](#2-environment-setup)
3. [Pandas fundamentals](#3-pandas-fundamentals)
4. [Parquet vs CSV](#4-parquet-vs-csv)
5. [PostgreSQL + SQLAlchemy pipeline](#5-postgresql--sqlalchemy-pipeline)
6. [Linux shell basics](#6-linux-shell-basics)
7. [Bash pipeline script](#7-bash-pipeline-script)
8. [Git — commit your work](#8-git--commit-your-work)
9. [Key concepts](#9-key-concepts)
10. [Week 1 checklist](#10-week-1-checklist)

---

## 1. Folder structure

```
week1/
├── titanic.csv              # raw source data
├── explore.py               # load, clean, transform, aggregate
├── parquet_vs_csv.py        # size and read-speed benchmark
├── db_pipeline.py           # Parquet → PostgreSQL ETL
├── run_pipeline.sh          # bash orchestrator with logging
└── pipeline.log             # practice log file for grep exercises
```

---

## 2. Environment setup

### 2.1 Update the system
```bash
sudo apt update && sudo apt upgrade -y
```

### 2.2 Install pyenv dependencies
```bash
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
libffi-dev liblzma-dev git
```

### 2.3 Install pyenv
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

### 2.4 Install Python 3.11
```bash
pyenv install 3.11.9
pyenv global 3.11.9
python --version   # should print Python 3.11.9
pip install --upgrade pip
```

### 2.5 Create project folder and virtual environment
```bash
mkdir ~/de-learning
cd ~/de-learning
python -m venv .venv
source .venv/bin/activate
# (.venv) should appear in your terminal prompt
```

### 2.6 Install week 1 libraries
```bash
pip install pandas polars pyarrow jupyter ipykernel sqlalchemy psycopg2-binary
```

### 2.7 Connect VS Code to WSL2
1. Install the **WSL** extension in VS Code (Windows side)
2. In your WSL terminal run:
```bash
code .
```
3. Install the **Python** extension when VS Code prompts
4. Select interpreter: `Ctrl+Shift+P` → `Python: Select Interpreter` → choose `.venv`

### 2.8 Verify everything works

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

## 3. Pandas fundamentals

### 3.1 Download the dataset
```bash
mkdir ~/de-learning/week1
cd ~/de-learning/week1
curl -O https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
```

### 3.2 Explore and transform — `explore.py`
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

## 4. Parquet vs CSV

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

## 5. PostgreSQL + SQLAlchemy pipeline

### 5.1 Start PostgreSQL in Docker
```bash
sudo apt install -y docker.io
sudo service docker start
sudo usermod -aG docker $USER
newgrp docker

docker run --name de-postgres \
  -e POSTGRES_USER=deuser \
  -e POSTGRES_PASSWORD=depassword \
  -e POSTGRES_DB=delearning \
  -p 5432:5432 \
  -d postgres:15

docker ps   # verify running
```

> **If the container stops and credentials fail:** remove and recreate it, then re-run `db_pipeline.py` to reload the data. Docker containers don't persist data by default — fixed in [Week 3](../week3/README.md) with named volumes.

### 5.2 ETL pipeline — `db_pipeline.py`
```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5432/delearning"
)

df = pd.read_parquet("titanic_cleaned.parquet")
df = df.drop(columns=["Cabin"])
df["Embarked"] = df["Embarked"].fillna("S")

print(f"Loading {len(df)} rows into PostgreSQL...")
df.to_sql("titanic", con=engine, if_exists="replace", index=False, chunksize=500)
print("Write complete.\n")

with engine.connect() as conn:
    result = pd.read_sql(
        text("SELECT pclass, COUNT(*) as total, AVG(fare) as avg_fare FROM titanic GROUP BY pclass ORDER BY pclass"),
        conn
    )
    print("=== Avg fare by class ===")
    print(result)

    result2 = pd.read_sql(text("""
        SELECT name, pclass, fare,
            ROUND(AVG(fare) OVER (PARTITION BY pclass)::numeric, 2) as class_avg_fare,
            ROUND((fare - AVG(fare) OVER (PARTITION BY pclass))::numeric, 2) as diff_from_avg
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

## 6. Linux shell basics

### 6.1 Create practice files
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

### 6.2 grep — search for patterns
```bash
grep "ERROR" pipeline.log                  # find all error lines
grep "INFO" pipeline.log | wc -l           # count info lines
grep -i "warning" pipeline.log             # case-insensitive
grep -v "INFO" pipeline.log                # everything EXCEPT INFO
grep "ERROR\|WARNING" pipeline.log         # match ERROR or WARNING
```

### 6.3 find — locate files
```bash
find . -name "*.csv"                       # all CSV files
find . -name "*.csv" -path "*/raw/*"       # CSVs inside raw/ only
find . -type f -name "*.log"               # files ending in .log
find . -type f -newer notes.txt            # files newer than notes.txt
```

### 6.4 pipes — chain commands
```bash
cat pipeline.log | grep "ERROR" | wc -l           # count errors
cat pipeline.log | grep "ERROR" | sort             # sort error lines
cat pipeline.log | awk '{print $3}' | sort | uniq -c  # count by log level
find . -name "*.csv" | xargs wc -l                # line count of all CSVs
```

---

## 7. Bash pipeline script

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

## 8. Git — commit your work

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

## 9. Key concepts

| Concept | Why it matters |
|---|---|
| Virtual environments | Isolates dependencies per project — always activate before working |
| Parquet over CSV | Columnar, compressed, column pruning — standard in data lakes |
| SQLAlchemy engine | Standard Python↔DB connector — used by pandas, dbt, Airflow |
| `set -e` in bash | Stops script on first failure — prevents silent partial runs |
| `tee` in bash | Prints to terminal AND writes to log file simultaneously |
| Window functions | Core DE/analytics SQL — ROW_NUMBER, LAG, AVG OVER PARTITION BY |
| `grep + pipes` | How DEs debug logs and monitor pipelines from the command line |

---

## 10. Week 1 checklist

- [x] Python 3.11 + pyenv + virtual environment
- [x] pandas — load, clean, transform, aggregate
- [x] Parquet read/write + column pruning
- [x] PostgreSQL via Docker + SQLAlchemy
- [x] Window functions from Python
- [x] Linux shell — grep, find, pipes, awk
- [x] Bash pipeline script with logging
- [x] Git commit

---

**Next:** [Week 2 — Advanced SQL, DB patterns & automation](../week2/README.md)
