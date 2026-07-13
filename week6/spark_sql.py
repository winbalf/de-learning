# spark_sql.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TitanicSQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Read partitioned Parquet — Spark reads partition columns automatically ────
print("=== Reading partitioned Parquet ===")
df = spark.read.parquet("output/titanic_cleaned")
print(f"Total rows: {df.count()}")
print(f"Partitions in memory: {df.rdd.getNumPartitions()}")

# Read only class 1 — Spark skips the other folders entirely (partition pruning)
print("\n=== Partition pruning — reading class 1 only ===")
class1 = spark.read \
    .parquet("output/titanic_cleaned/passenger_class=1")
print(f"Class 1 rows: {class1.count()}")

# ── Register as a temporary SQL view ─────────────────────────────────────────
df.createOrReplaceTempView("passengers")
print("\n=== Temporary view registered: passengers ===")

# ── Run plain SQL on the DataFrame ───────────────────────────────────────────
print("\n=== SQL: survival rate by class ===")
spark.sql("""
    SELECT
        passenger_class,
        COUNT(*)                                        AS total,
        SUM(survived)                                   AS survived,
        ROUND(100.0 * SUM(survived) / COUNT(*), 1)     AS survival_rate_pct,
        ROUND(AVG(fare_gbp), 2)                        AS avg_fare,
        ROUND(AVG(age), 1)                             AS avg_age
    FROM passengers
    GROUP BY passenger_class
    ORDER BY passenger_class
""").show()

print("\n=== SQL: window function — running total of fares by class ===")
spark.sql("""
    SELECT
        passenger_class,
        passenger_id,
        fare_gbp,
        ROUND(SUM(fare_gbp) OVER (
            PARTITION BY passenger_class
            ORDER BY passenger_id
        ), 2) AS running_total,
        ROUND(AVG(fare_gbp) OVER (
            PARTITION BY passenger_class
            ORDER BY passenger_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_3
    FROM passengers
    WHERE passenger_class = 1
    ORDER BY passenger_id
    LIMIT 8
""").show()

print("\n=== SQL: who survived vs died by embarkation port ===")
spark.sql("""
    SELECT
        embarkation_code,
        survival_status,
        COUNT(*)                                        AS count,
        ROUND(AVG(fare_gbp), 2)                        AS avg_fare
    FROM passengers
    WHERE embarkation_code IS NOT NULL
    GROUP BY embarkation_code, survival_status
    ORDER BY embarkation_code, survival_status
""").show()

# ── EXPLAIN — see Spark's query plan (like EXPLAIN ANALYZE in Postgres) ───────
print("\n=== Query plan for survival by class ===")
spark.sql("""
    SELECT passenger_class, COUNT(*) as total, ROUND(AVG(fare_gbp),2) as avg_fare
    FROM passengers GROUP BY passenger_class
""").explain(mode="simple")

# ── Write aggregated result to Parquet ───────────────────────────────────────
print("\n=== Writing summary to Parquet ===")
summary = spark.sql("""
    SELECT
        passenger_class,
        COUNT(*)                                        AS total,
        SUM(survived)                                   AS survived,
        ROUND(100.0 * SUM(survived) / COUNT(*), 1)     AS survival_rate_pct,
        ROUND(AVG(fare_gbp), 2)                        AS avg_fare,
        ROUND(AVG(age), 1)                             AS avg_age
    FROM passengers
    GROUP BY passenger_class
    ORDER BY passenger_class
""")

summary.write \
    .mode("overwrite") \
    .parquet("output/survival_summary")

print("Written to output/survival_summary/")
summary.show()

spark.stop()
print("Done.")