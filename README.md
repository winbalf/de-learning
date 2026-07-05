# Data Engineering Study Plan — Week 1

**Theme: Python environment, pandas, Parquet, PostgreSQL, Linux shell & bash scripting**

> **Setup:** WSL2 Ubuntu on Windows · Python 3.11 · 5–10 hrs/week target
>
> **Project setup guide:** see [SETUP.md](SETUP.md) for virtual environment activation, Docker/PostgreSQL, and troubleshooting.

---

## Table of Contents

1. [Environment Setup](#1-environment-setup)
2. [Pandas Fundamentals](#2-pandas-fundamentals)
3. [Parquet vs CSV](#3-parquet-vs-csv)
4. [PostgreSQL + SQLAlchemy Pipeline](#4-postgresql--sqlalchemy-pipeline)
5. [Linux Shell Basics](#5-linux-shell-basics)
6. [Bash Pipeline Script](#6-bash-pipeline-script)
7. [Git — Commit Your Work](#7-git--commit-your-work)
8. [Key Concepts to Remember](#8-key-concepts-to-remember)

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

1. Install the **Python** extension when VS Code prompts
2. Select interpreter: `Ctrl+Shift+P` → `Python: Select Interpreter` → choose `.venv`



### 1.8 Verify everything works

```bash
touch first_script.py
```

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

Expected output:

```
        city  population country
0     Sydney     5300000      AU
1  Melbourne     5100000      AU
2   Brisbane     2600000      AU

Total population: 13,000,000
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

# --- Load ---
df = pd.read_csv("titanic.csv")

# --- Explore ---
print("=== Shape ===")
print(df.shape)               # rows, columns

print("\n=== Column types ===")
print(df.dtypes)

print("\n=== First 5 rows ===")
print(df.head())

print("\n=== Missing values ===")
print(df.isnull().sum())

# --- Transform ---
df["Age"] = df["Age"].fillna(df["Age"].median())
df["fare_per_age"] = (df["Fare"] / df["Age"]).round(2)

# --- Aggregate ---
print("\n=== Survival rate by passenger class ===")
summary = (
    df.groupby("Pclass")
    .agg(
        total=("Survived", "count"),
        survived=("Survived", "sum"),
    )
    .assign(survival_rate=lambda x: (x["survived"] / x["total"] * 100).round(1))
)
print(summary)

# --- Save to Parquet ---
df.to_parquet("titanic_cleaned.parquet", index=False)
print("\nSaved to titanic_cleaned.parquet")

df_check = pd.read_parquet("titanic_cleaned.parquet")
print(f"Parquet rows: {len(df_check)}")
```

```bash
python explore.py
```

**Key pandas operations used:**


| Operation    | Method               | DE use case            |
| ------------ | -------------------- | ---------------------- |
| Load CSV     | `pd.read_csv()`      | Ingest raw source data |
| Check nulls  | `df.isnull().sum()`  | Data quality check     |
| Fill nulls   | `df.fillna()`        | Data cleaning          |
| Aggregate    | `df.groupby().agg()` | Build summary tables   |
| Save Parquet | `df.to_parquet()`    | Write to data lake     |


---



## 3. Parquet vs CSV



### Why Parquet matters

- **Columnar storage** — reads only the columns you need, skips the rest
- **Compressed** — smaller file sizes on disk
- **Schema embedded** — data types are preserved, no guessing
- **Industry standard** — used in S3, GCS, Snowflake, BigQuery, Spark



### Benchmark script — `parquet_vs_csv.py`

```python
import pandas as pd
import time
import os

csv_size = os.path.getsize("titanic.csv") / 1024
parquet_size = os.path.getsize("titanic_cleaned.parquet") / 1024
print(f"CSV size:     {csv_size:.1f} KB")
print(f"Parquet size: {parquet_size:.1f} KB")
print(f"Parquet is {csv_size / parquet_size:.1f}x smaller\n")

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



### 4.1 Start a PostgreSQL container

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

docker ps   # verify container is running
```



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

df.to_sql(
    name="titanic",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=500
)
print("Write complete.\n")

with engine.connect() as conn:
    result = pd.read_sql(
        text("SELECT pclass, COUNT(*) as total, AVG(fare) as avg_fare FROM titanic GROUP BY pclass ORDER BY pclass"),
        conn
    )
    print("=== Avg fare by class ===")
    print(result)

    result2 = pd.read_sql(text("""
        SELECT
            name,
            pclass,
            fare,
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

```bash
python db_pipeline.py
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
2024-01-15 08:00:03 INFO  Missing values detected: Age=177, Cabin=687, Embarked=2
2024-01-15 08:00:04 WARNING Cabin column has 77% nulls - dropping column
2024-01-15 08:00:05 INFO  Cleaning complete - 891 rows retained
2024-01-15 08:00:06 INFO  Writing to PostgreSQL table: titanic
2024-01-15 08:00:07 INFO  Write complete - 891 rows inserted
2024-01-15 08:00:08 ERROR Connection timeout on retry attempt 1
2024-01-15 08:00:09 INFO  Retry successful
2024-01-15 08:00:10 INFO  Pipeline finished successfully
2024-01-15 08:01:00 INFO  Pipeline started
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
grep -i "warning" pipeline.log             # case-insensitive search
grep -v "INFO" pipeline.log                # everything EXCEPT INFO
grep "ERROR\|WARNING" pipeline.log         # match ERROR or WARNING
```



### 5.3 find — locate files

```bash
find . -name "*.csv"                       # all CSV files
find . -name "*.csv" -path "*/raw/*"       # CSVs only inside raw/
find . -type f -name "*.log"               # only files ending in .log
find . -type f -newer notes.txt            # files newer than notes.txt
```



### 5.4 pipes — chain commands

```bash
cat pipeline.log | grep "ERROR" | wc -l           # count errors
cat pipeline.log | grep "ERROR" | sort             # sort error lines
cat pipeline.log | awk '{print $3}' | sort | uniq -c  # count by log level
ls -la | grep ".csv"                               # list only CSV files
find . -name "*.csv" | xargs wc -l                # line count of all CSVs
```

---



## 6. Bash Pipeline Script

`run_pipeline.sh`:

```bash
#!/bin/bash

set -e  # exit immediately if any command fails

LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"
DATA_DIR="$HOME/de-learning/week1"
VENV="$HOME/de-learning/.venv"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

log "INFO  Pipeline started"
log "INFO  Log file: $LOG_FILE"

log "INFO  Activating virtual environment"
source "$VENV/bin/activate"

cd "$DATA_DIR"

if [ -f "titanic.csv" ]; then
  log "INFO  titanic.csv already exists — skipping download"
else
  log "INFO  Downloading titanic.csv"
  curl -s -O https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
  log "INFO  Download complete"
fi

ROW_COUNT=$(tail -n +2 titanic.csv | wc -l)
log "INFO  titanic.csv has $ROW_COUNT data rows"

if [ "$ROW_COUNT" -lt 100 ]; then
  log "ERROR File has fewer than 100 rows — aborting"
  exit 1
fi

log "INFO  Running ETL pipeline"

if python db_pipeline.py >> "$LOG_FILE" 2>&1; then
  log "INFO  ETL complete"
else
  log "ERROR ETL script failed — check log for details"
  exit 1
fi

log "INFO  Pipeline finished successfully"
echo ""
echo "=== Error summary ==="
grep "ERROR" "$LOG_FILE" || echo "No errors found"
echo ""
echo "=== Log saved to: $LOG_FILE ==="
```

Make it executable and run with bash:

```bash
chmod +x run_pipeline.sh
bash run_pipeline.sh
```

> **Note:** Always run with `bash`, not `sh`. The `source` command is a bash built-in and will fail under `sh`.

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
EOF

git add .
git commit -m "week1: pandas ETL pipeline - titanic CSV to PostgreSQL"
```

---



## 8. Key Concepts to Remember


| Concept              | Why it matters                                                        |
| -------------------- | --------------------------------------------------------------------- |
| Virtual environments | Isolates dependencies per project — always activate before working    |
| Parquet over CSV     | Columnar, compressed, column pruning — standard in data lakes         |
| SQLAlchemy engine    | The standard Python↔database connector — used by pandas, dbt, Airflow |
| `set -e` in bash     | Stops script on first failure — prevents silent partial runs          |
| `tee` in bash        | Prints to terminal AND writes to log file simultaneously              |
| Window functions     | Core DE/analytics SQL — ROW_NUMBER, LAG, AVG OVER PARTITION BY        |
| `grep + pipes`       | How DEs debug logs and monitor pipelines from the command line        |


---



## Week 1 Checklist

- [x] Python 3.11 + pyenv + virtual environment
- [x] pandas — load, clean, transform, aggregate
- [x] Parquet read/write + column pruning
- [x] PostgreSQL via Docker + SQLAlchemy
- [x] Window functions from Python
- [x] Linux shell — grep, find, pipes, awk
- [x] Bash pipeline script with logging
- [x] Git commit

---

**Next:** Week 2 — Advanced SQL, Python↔DB patterns, shell scripting automation