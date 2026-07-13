# Week 6 — PySpark: DataFrames, Spark SQL & Medallion Architecture

**Theme:** Process data at scale using Apache Spark. Understand lazy evaluation, partitioning, and the bronze/silver/gold lakehouse pattern.

> **Prerequisites:** [Week 1](../week1/README.md) complete · titanic.csv in `~/de-learning/week1/` · Java 11+ installed · [Week 5](../week5/README.md) gold tables for comparison

**Hub:** [Root README](../README.md) · [Week 5](../week5/README.md)

---

## Scripts in this folder

| Script | What it covers | Run |
|---|---|---|
| [`spark_basics.py`](spark_basics.py) | SparkSession, DataFrames, transforms, aggregations, partitioned Parquet | `python spark_basics.py` |
| [`spark_sql.py`](spark_sql.py) | Temp views, Spark SQL, window functions, EXPLAIN | `python spark_sql.py` |
| [`medallion.py`](medallion.py) | Bronze → silver → gold lakehouse pipeline + DQ checks | `python medallion.py` |

Outputs land in `week6/output/` (gitignored — regenerate by running the scripts).

---

## Table of Contents
1. [The mental model shift from pandas to Spark](#1-the-mental-model-shift-from-pandas-to-spark)
2. [Installation](#2-installation)
3. [SparkSession](#3-sparksession)
4. [DataFrames — transformations and actions](#4-dataframes--transformations-and-actions)
5. [Spark SQL](#5-spark-sql)
6. [Window functions](#6-window-functions)
7. [Partitioned Parquet](#7-partitioned-parquet)
8. [Medallion architecture](#8-medallion-architecture)
9. [Query plans — EXPLAIN](#9-query-plans--explain)
10. [Key concepts](#10-key-concepts)
11. [Week 6 checklist](#11-week-6-checklist)

---

## 1. The mental model shift from pandas to Spark

**pandas** — loads everything into your machine's RAM:
```
CSV on disk → RAM (16GB limit) → transform → result
```

**Spark** — distributes data across partitions, processes in parallel:
```
Data on disk (unlimited size)
        ↓
Spark splits into partitions
        ↓
Worker processes each partition in parallel
        ↓
Results merged
```

On your laptop Spark runs with one "worker" (your machine). The code is identical to what runs on a 100-node cluster in AWS. You're learning the API, not the hardware.

**The key difference — lazy evaluation:**
- pandas executes immediately
- Spark builds a query plan and executes only when you call an **action**

```python
# These are TRANSFORMATIONS — nothing executes
df = spark.read.csv(...)
cleaned = df.withColumn(...).filter(...)
aggregated = cleaned.groupBy(...).agg(...)

# These are ACTIONS — trigger execution
aggregated.show()      # execute + print
aggregated.count()     # execute + return count
aggregated.write.parquet(...)  # execute + write
```

Spark sees the full transformation chain before executing — this lets it optimise (push filters down, skip columns, reorder joins).

---

## 2. Installation

```bash
pip install pyspark==3.5.1

# PySpark needs Java
sudo apt install -y default-jdk
java -version   # should show OpenJDK 11 or 17

# Verify
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 3. SparkSession

Entry point to everything in Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TitanicAnalysis") \
    .master("local[*]") \               # local[*] = use all CPU cores
    .config("spark.sql.shuffle.partitions", "4") \  # default 200 is too many locally
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # suppress verbose logs
```

Always stop the session when done:
```python
spark.stop()
```

---

## 4. DataFrames — transformations and actions

```python
from pyspark.sql import functions as F

# Read CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("titanic.csv")

df.printSchema()          # show column names and types
df.show(5)                # show first 5 rows (ACTION)
df.count()                # row count (ACTION)

# Transformations (lazy)
cleaned = df \
    .withColumnRenamed("PassengerId", "passenger_id") \
    .withColumn("age", F.coalesce(F.col("Age"), F.lit(28.0))) \
    .withColumn("fare_gbp", F.round(F.col("Fare"), 2)) \
    .withColumn("is_solo", F.when(F.col("SibSp") + F.col("Parch") == 0, True).otherwise(False)) \
    .withColumn("survival_status",
        F.when(F.col("Survived") == 1, "survived").otherwise("died")) \
    .drop("Cabin")

# Aggregations
survival_by_class = cleaned \
    .groupBy("Pclass") \
    .agg(
        F.count("*").alias("total"),
        F.sum("Survived").alias("survived"),
        F.round(F.avg("fare_gbp"), 2).alias("avg_fare"),
        F.round(F.sum("Survived") / F.count("*") * 100, 1).alias("survival_rate_pct")
    ) \
    .orderBy("Pclass")

survival_by_class.show()
```

**Common functions:**

| pandas | PySpark equivalent |
|---|---|
| `df["col"]` | `F.col("col")` |
| `df.fillna(28)` | `F.coalesce(F.col("age"), F.lit(28))` |
| `df["col"].apply(fn)` | `F.when(...).otherwise(...)` |
| `df.groupby().agg()` | `.groupBy().agg()` |
| `df.merge()` | `.join(other, on="key", how="left")` |
| `df.drop_duplicates()` | `.dropDuplicates(["col"])` |

---

## 5. Spark SQL

Register a DataFrame as a temporary view and run plain SQL:

```python
cleaned.createOrReplaceTempView("passengers")

result = spark.sql("""
    SELECT
        Pclass,
        COUNT(*)                                        AS total,
        SUM(Survived)                                   AS survived,
        ROUND(100.0 * SUM(Survived) / COUNT(*), 1)     AS survival_rate_pct,
        ROUND(AVG(fare_gbp), 2)                        AS avg_fare
    FROM passengers
    GROUP BY Pclass
    ORDER BY Pclass
""")

result.show()
```

SQL and DataFrame API produce identical results — pick whichever is cleaner for the task. Complex joins and window functions are often cleaner in SQL; simple transforms are cleaner in the API.

---

## 6. Window functions

Same concepts as SQL week 2 — different syntax:

```python
from pyspark.sql.window import Window

# Define the window
window = Window.partitionBy("passenger_class").orderBy(F.col("fare_gbp").desc())

# Apply window functions
ranked = df \
    .withColumn("fare_rank",    F.rank().over(window)) \
    .withColumn("dense_rank",   F.dense_rank().over(window)) \
    .withColumn("running_total",F.sum("fare_gbp").over(
        Window.partitionBy("passenger_class").orderBy("passenger_id")
    )) \
    .withColumn("moving_avg_3", F.avg("fare_gbp").over(
        Window.partitionBy("passenger_class")
              .orderBy("passenger_id")
              .rowsBetween(-2, 0)    # 2 preceding rows + current
    ))
```

Or use Spark SQL (cleaner for window functions):
```sql
SELECT passenger_class, passenger_id, fare_gbp,
    RANK() OVER (PARTITION BY passenger_class ORDER BY fare_gbp DESC) AS fare_rank,
    SUM(fare_gbp) OVER (PARTITION BY passenger_class ORDER BY passenger_id) AS running_total,
    AVG(fare_gbp) OVER (
        PARTITION BY passenger_class ORDER BY passenger_id
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3
FROM passengers
```

---

## 7. Partitioned Parquet

Partitioning splits data into folders by column value — Spark skips irrelevant folders entirely (partition pruning):

```python
# Write partitioned by passenger_class
df.write \
    .mode("overwrite") \
    .partitionBy("passenger_class") \
    .parquet("output/titanic_cleaned")

# Output structure:
# output/titanic_cleaned/
#   passenger_class=1/  ← 216 rows
#   passenger_class=2/  ← 184 rows
#   passenger_class=3/  ← 491 rows

# Read all partitions
df_all = spark.read.parquet("output/titanic_cleaned")

# Read one partition only — Spark skips the other folders
df_class1 = spark.read.parquet("output/titanic_cleaned/passenger_class=1")
# Only reads 216 rows — skips 675
```

**Partition column choice:** partition by columns you filter on frequently. Common choices: date (daily partitions), region, status. Don't partition by high-cardinality columns like passenger_id — you'd get thousands of tiny files.

---

## 8. Medallion architecture

The standard pattern for data lakehouses (Databricks, Delta Lake, AWS Glue):

```
Bronze  → raw data as-is, never modified, + metadata columns
Silver  → cleaned, validated, deduplicated, enriched
Gold    → aggregated, business-ready, one table per use case
```

```python
# BRONZE — raw ingestion, metadata only
bronze = spark.read.csv("titanic.csv", header=True, inferSchema=True) \
    .withColumn("_ingested_at", F.lit(run_ts)) \
    .withColumn("_source_file", F.lit("titanic.csv"))

bronze.write.mode("overwrite").parquet("output/bronze")

# SILVER — DQ checks then clean
dq = raw.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("PassengerId").isNull(), 1).otherwise(0)).alias("null_ids"),
    F.countDistinct("PassengerId").alias("unique_ids")
).collect()[0]

silver = raw \
    .withColumnRenamed("PassengerId", "passenger_id") \
    .withColumn("age", F.coalesce(F.col("Age"), F.lit(28.0))) \
    .withColumn("age_group",
        F.when(F.col("Age") < 13, "child")
         .when(F.col("Age") < 18, "teenager")
         .when(F.col("Age") < 60, "adult")
         .otherwise("senior")) \
    .dropDuplicates(["passenger_id"]) \
    .withColumn("_processed_at", F.lit(run_ts))

silver.write.mode("overwrite").partitionBy("passenger_class").parquet("output/silver")

# GOLD — aggregated, business-ready
silver_df.createOrReplaceTempView("silver")

gold = spark.sql("""
    SELECT passenger_class,
           COUNT(*) AS total_passengers,
           ROUND(100.0 * SUM(survived) / COUNT(*), 1) AS survival_rate_pct
    FROM silver GROUP BY passenger_class
""")

gold.write.mode("overwrite").parquet("output/gold/survival_by_class")
```

**Layer comparison from this week:**

| Layer | Rows | Size | Purpose |
|---|---|---|---|
| Bronze | 891 | 35.9 KB | Raw archive — never modified |
| Silver | 891 | 49.0 KB | Clean, validated, enriched |
| Gold | 3 | 2.7 KB | Analyst-ready aggregation |

**Interview answer:** "Bronze is raw and immutable — your safety net if something goes wrong upstream. Silver is clean and validated — one row per entity, no nulls, consistent types. Gold is aggregated and optimised for specific business questions. Analysts query gold, never bronze."

---

## 9. Query plans — EXPLAIN

Like `EXPLAIN ANALYZE` in Postgres but for distributed execution:

```python
df.explain(mode="simple")   # physical plan only
df.explain(mode="extended") # logical + physical plans
```

**Read bottom to top:**
```
FileScan parquet [fare_gbp, passenger_class]  ← column pruning: reads only needed columns
  HashAggregate (partial)                      ← each partition aggregates locally first
    Exchange hashpartitioning                  ← shuffle across partitions
      HashAggregate (final)                    ← merge partial results
```

Key optimisations to spot:
- **FileScan** only lists columns actually used — column pruning working
- **PartitionFilters** shows partition pruning — skipping folders
- **PushedFilters** shows predicates pushed to the file reader

---

## 10. Key concepts

| Concept | Why it matters |
|---|---|
| Lazy evaluation | Spark builds a plan before executing — enables optimisation |
| Transformation vs action | `.withColumn()` = lazy; `.show()`, `.count()`, `.write()` = triggers execution |
| Partitioning | Splits data into folders — queries skip irrelevant partitions |
| `local[*]` | Use all CPU cores locally — same code runs on a cluster |
| `shuffle.partitions=4` | Default 200 creates too many tiny partitions locally — always tune this |
| `inferSchema=true` | Spark scans data to detect types — use explicit schema in production |
| `dropDuplicates()` | Deduplication — always do this in silver, never in bronze |
| `_ingested_at` / `_processed_at` | Metadata columns — track when data arrived at each layer |
| Bronze/Silver/Gold | The medallion architecture — industry standard for data lakehouses |
| Spark SQL | Plain SQL on DataFrames via `createOrReplaceTempView()` |
| Column pruning | Spark reads only the columns referenced in the query |
| Partition pruning | Spark skips folders that don't match the filter |

---

## 11. Week 6 checklist

- [x] PySpark 3.5.1 installed + Java configured
- [x] SparkSession created with `local[*]` and tuned shuffle partitions
- [x] CSV → DataFrame with schema inference
- [x] Transformations: `withColumnRenamed`, `withColumn`, `coalesce`, `when/otherwise`
- [x] Aggregations: `groupBy().agg()` with multiple functions
- [x] Spark SQL: `createOrReplaceTempView()` + plain SQL queries
- [x] Window functions: `rank()`, `sum()`, `avg()` over partitions and row frames
- [x] Partitioned Parquet write and read with partition pruning
- [x] EXPLAIN — read physical query plans
- [x] Medallion architecture: bronze → silver → gold pipeline
- [x] DQ checks embedded in silver layer
- [x] Gold tables match dbt mart outputs exactly
- [x] Git commit

---

**Back:** [Week 5 — dbt](../week5/README.md) · [All weeks](../README.md)

**Next:** Week 7 — Portfolio project · end-to-end pipeline · GitHub · interview prep