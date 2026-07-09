# Week 2 — Advanced SQL, DB Patterns & Automation

**Theme:** Level up your SQL with window functions and query optimisation. Build reusable DB utilities and automate pipelines with cron.

> **Prerequisites:** [Week 1](../week1/README.md) complete · `de-postgres` container running · titanic data loaded

---

## Table of Contents
1. [Folder structure](#1-folder-structure)
2. [Prerequisites](#2-prerequisites)
3. [Window functions](#3-window-functions)
4. [Recursive CTEs + query optimisation](#4-recursive-ctes--query-optimisation)
5. [Reusable DB connection patterns](#5-reusable-db-connection-patterns)
6. [Automated pipeline + cron scheduling](#6-automated-pipeline--cron-scheduling)
7. [Key concepts](#7-key-concepts)
8. [Week 2 checklist](#8-week-2-checklist)

---

## 1. Folder structure

```
week2/
├── window_functions.py      # ROW_NUMBER, RANK, LAG, running totals, NTILE
├── query_optimization.py    # recursive CTEs, EXPLAIN ANALYZE, indexing
├── db_utils.py              # connection pool, context manager, helpers
├── auto_pipeline.sh         # scheduled runner with log rotation
└── logs/                    # pipeline run logs
```

---

## 2. Prerequisites

### 2.1 Start the environment
```bash
cd ~/de-learning
source .venv/bin/activate
docker start de-postgres
mkdir week2 && cd week2
```

### 2.2 Reload titanic data if container was recreated
```bash
cd ~/de-learning/week1
python db_pipeline.py
cd ~/de-learning/week2
```

### 2.3 Verify DB is reachable
```bash
docker exec -it de-postgres psql -U deuser -d delearning -c "SELECT COUNT(*) FROM titanic"
```

---

## 3. Window functions

Window functions compute across related rows without collapsing them — unlike `GROUP BY` which returns one row per group. Used daily in DE for deduplication, period-over-period comparisons, running totals, and rankings.

### Script — `window_functions.py`

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5432/delearning"
)

with engine.connect() as conn:

    # ROW_NUMBER — first passenger per class (used for deduplication)
    r1 = pd.read_sql(text("""
        SELECT * FROM (
            SELECT name, pclass, passengerid,
                ROW_NUMBER() OVER (PARTITION BY pclass ORDER BY passengerid) as row_num
            FROM titanic
        ) t WHERE row_num = 1
    """), conn)
    print("=== ROW_NUMBER ===")
    print(r1)

    # RANK vs DENSE_RANK — fare ranking within class
    r2 = pd.read_sql(text("""
        SELECT name, pclass, fare,
            RANK()       OVER (PARTITION BY pclass ORDER BY fare DESC) as rank,
            DENSE_RANK() OVER (PARTITION BY pclass ORDER BY fare DESC) as dense_rank
        FROM titanic WHERE pclass = 1
        ORDER BY fare DESC LIMIT 10
    """), conn)
    print("\n=== RANK vs DENSE_RANK ===")
    print(r2)

    # LAG / LEAD — compare each fare to previous/next passenger
    r3 = pd.read_sql(text("""
        SELECT passengerid, name, fare,
            LAG(fare)  OVER (ORDER BY passengerid) as prev_fare,
            LEAD(fare) OVER (ORDER BY passengerid) as next_fare,
            ROUND((fare - LAG(fare) OVER (ORDER BY passengerid))::numeric, 2) as diff_from_prev
        FROM titanic ORDER BY passengerid LIMIT 10
    """), conn)
    print("\n=== LAG / LEAD ===")
    print(r3[["passengerid", "name", "fare", "prev_fare", "diff_from_prev"]])

    # Running total + 3-row moving average
    r4 = pd.read_sql(text("""
        SELECT passengerid, pclass, fare,
            SUM(fare) OVER (PARTITION BY pclass ORDER BY passengerid) as running_total,
            ROUND(AVG(fare) OVER (
                PARTITION BY pclass ORDER BY passengerid
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )::numeric, 2) as moving_avg_3
        FROM titanic WHERE pclass = 1
        ORDER BY passengerid LIMIT 10
    """), conn)
    print("\n=== Running total + moving average ===")
    print(r4)

    # NTILE — split into fare quartiles
    r5 = pd.read_sql(text("""
        SELECT name, fare,
            NTILE(4) OVER (ORDER BY fare) as fare_quartile
        FROM titanic ORDER BY fare DESC LIMIT 12
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

## 4. Recursive CTEs + query optimisation

### Script — `query_optimization.py`

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5432/delearning"
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
    print("\n=== EXPLAIN ANALYZE — cold ===")
    plan1 = pd.read_sql(text("EXPLAIN ANALYZE SELECT * FROM titanic WHERE fare > 100"), conn)
    for row in plan1["QUERY PLAN"]:
        print(row)

    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_titanic_fare ON titanic(fare)"))
    conn.commit()

    print("\n=== EXPLAIN ANALYZE — with index ===")
    plan2 = pd.read_sql(text("EXPLAIN ANALYZE SELECT * FROM titanic WHERE fare > 100"), conn)
    for row in plan2["QUERY PLAN"]:
        print(row)

    # Anti-pattern vs optimised
    bad = pd.read_sql(text("SELECT * FROM titanic"), conn)
    bad_result = bad[bad["fare"] > 100][["name", "fare", "pclass"]]
    print(f"\nAnti-pattern: loaded {len(bad)} rows, kept {len(bad_result)}")

    good = pd.read_sql(text("""
        SELECT name, fare, pclass FROM titanic WHERE fare > 100 ORDER BY fare DESC
    """), conn)
    print(f"Optimised:    loaded {len(good)} rows directly")
```

**What to look for in EXPLAIN ANALYZE:**

| Term | Meaning |
|---|---|
| `Seq Scan` | Full table read — slow at scale |
| `Bitmap Index Scan` | Uses index — fast for filtered queries |
| `actual time` | Real execution time in ms |
| `Planning Time` | Drops sharply after warm cache |

> **Interview answer:** "Always filter and project in SQL before pulling data into Python — the database is optimised for scanning and filtering; Python is not."

---

## 5. Reusable DB connection patterns

### `db_utils.py` — production-grade connector

```python
import logging
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text, pool
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_engine(host="localhost", port=5432, db="delearning",
               user="deuser", password="depassword",
               pool_size=5, max_overflow=2):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(
        url,
        poolclass=pool.QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True
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

**Key patterns:**

| Pattern | Why it matters |
|---|---|
| Connection pool | Never open a new connection per query — expensive at scale |
| `pool_pre_ping=True` | Detects stale connections before using them |
| Context manager | Guarantees commit/rollback/close even if script crashes |
| Parameterised queries | Prevents SQL injection — never use f-strings to build SQL |
| Atomic rollback | Either all inserts succeed or none are committed |

---

## 6. Automated pipeline + cron scheduling

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

if check_db; then
    log "INFO" "Database is ready"
else
    log "ERROR" "Database not reachable — aborting"
    exit 1
fi

cd "$WEEK2_DIR"

python - << 'PYEOF'
import sys
sys.path.insert(0, '.')
from db_utils import get_engine, run_query, load_dataframe

engine = get_engine()
summary = run_query(engine, """
    SELECT pclass, COUNT(*) AS total, SUM(survived) AS survived,
           ROUND(AVG(fare)::numeric, 2) AS avg_fare,
           ROUND(100.0 * SUM(survived) / COUNT(*)::numeric, 1) AS survival_pct,
           ROUND(AVG(age)::numeric, 1) AS avg_age
    FROM titanic GROUP BY pclass ORDER BY pclass
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

### Cron syntax reference
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

## 7. Key concepts

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

---

## 8. Week 2 checklist

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

**Next:** [Week 3 — Docker Compose, named volumes & pipeline observability](../week3/README.md)
