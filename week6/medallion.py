# medallion.py
# Implements bronze → silver → gold pipeline on the Titanic dataset
# This is the pattern used in Databricks, Delta Lake, and AWS Glue

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MedallionPipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

BRONZE = "output/bronze"
SILVER = "output/silver"
GOLD   = "output/gold"

run_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

# ══════════════════════════════════════════════════════════════════════════════
# BRONZE — Raw ingestion, no transformations
# Add metadata columns only: ingestion timestamp and source file
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("BRONZE — Raw ingestion")
print("="*60)

bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("../week1/titanic.csv") \
    .withColumn("_ingested_at", F.lit(run_ts)) \
    .withColumn("_source_file", F.lit("titanic.csv"))

print(f"Bronze rows: {bronze.count()}")
print(f"Bronze columns: {len(bronze.columns)}")
bronze.printSchema()

bronze.write \
    .mode("overwrite") \
    .parquet(BRONZE)

print(f"Bronze written to {BRONZE}/")

# ══════════════════════════════════════════════════════════════════════════════
# SILVER — Clean, validate, deduplicate, rename
# Same logic as your dbt staging model — but in PySpark
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("SILVER — Clean and validate")
print("="*60)

raw = spark.read.parquet(BRONZE)

# Data quality checks before cleaning
dq = raw.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("PassengerId").isNull(), 1).otherwise(0)).alias("null_ids"),
    F.sum(F.when(F.col("Fare") < 0, 1).otherwise(0)).alias("negative_fares"),
    F.countDistinct("PassengerId").alias("unique_ids")
).collect()[0]

print(f"DQ checks on bronze:")
print(f"  total rows:      {dq.total_rows}")
print(f"  null ids:        {dq.null_ids}   {'PASS' if dq.null_ids == 0 else 'FAIL'}")
print(f"  negative fares:  {dq.negative_fares}   {'PASS' if dq.negative_fares == 0 else 'FAIL'}")
print(f"  unique ids:      {dq.unique_ids} (should equal total rows) {'PASS' if dq.unique_ids == dq.total_rows else 'FAIL'}")

silver = raw \
    .withColumnRenamed("PassengerId", "passenger_id") \
    .withColumnRenamed("Survived",    "survived") \
    .withColumnRenamed("Pclass",      "passenger_class") \
    .withColumnRenamed("Name",        "full_name") \
    .withColumnRenamed("Sex",         "gender") \
    .withColumnRenamed("Age",         "age") \
    .withColumnRenamed("SibSp",       "siblings_spouses") \
    .withColumnRenamed("Parch",       "parents_children") \
    .withColumnRenamed("Ticket",      "ticket_number") \
    .withColumnRenamed("Fare",        "fare_gbp") \
    .withColumnRenamed("Embarked",    "embarkation_code") \
    .drop("Cabin") \
    .withColumn("age",              F.coalesce(F.col("age"), F.lit(28.0))) \
    .withColumn("fare_gbp",         F.round(F.col("fare_gbp"), 2)) \
    .withColumn("embarkation_code", F.coalesce(F.col("embarkation_code"), F.lit("S"))) \
    .withColumn("family_size",      F.col("siblings_spouses") + F.col("parents_children")) \
    .withColumn("is_solo",          F.col("family_size") == 0) \
    .withColumn("survival_status",
        F.when(F.col("survived") == 1, "survived").otherwise("died")) \
    .withColumn("age_group",
        F.when(F.col("age") < 13,  "child")
         .when(F.col("age") < 18,  "teenager")
         .when(F.col("age") < 60,  "adult")
         .otherwise("senior")) \
    .withColumn("fare_tier",
        F.when(F.col("fare_gbp") < 7.9,  "budget")
         .when(F.col("fare_gbp") < 14.5, "economy")
         .when(F.col("fare_gbp") < 31.0, "standard")
         .otherwise("premium")) \
    .withColumn("_processed_at", F.lit(run_ts)) \
    .dropDuplicates(["passenger_id"])

print(f"\nSilver rows: {silver.count()}")
silver.printSchema()

silver.write \
    .mode("overwrite") \
    .partitionBy("passenger_class") \
    .parquet(SILVER)

print(f"Silver written to {SILVER}/ (partitioned by passenger_class)")

# ══════════════════════════════════════════════════════════════════════════════
# GOLD — Aggregated, business-ready tables
# Multiple gold tables from one silver layer
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("GOLD — Business-ready aggregations")
print("="*60)

silver_df = spark.read.parquet(SILVER)
silver_df.createOrReplaceTempView("silver")

# Gold table 1: survival by class
gold_by_class = spark.sql("""
    SELECT
        passenger_class,
        COUNT(*)                                        AS total_passengers,
        SUM(survived)                                   AS total_survived,
        ROUND(100.0 * SUM(survived) / COUNT(*), 1)     AS survival_rate_pct,
        ROUND(AVG(fare_gbp), 2)                        AS avg_fare_gbp,
        ROUND(AVG(age), 1)                             AS avg_age,
        COUNT(*) FILTER (WHERE is_solo = true)         AS solo_travellers,
        COUNT(*) FILTER (WHERE gender = 'female')      AS female_count,
        COUNT(*) FILTER (WHERE gender = 'male')        AS male_count
    FROM silver
    GROUP BY passenger_class
    ORDER BY passenger_class
""")

print("\nGold — survival by class:")
gold_by_class.show()

gold_by_class.write.mode("overwrite").parquet(f"{GOLD}/survival_by_class")

# Gold table 2: survival by demographics
gold_demographics = spark.sql("""
    SELECT 'gender'     AS dimension, gender       AS segment,
           COUNT(*)     AS total, SUM(survived)    AS survived,
           ROUND(100.0 * SUM(survived) / COUNT(*), 1) AS survival_rate_pct
    FROM silver GROUP BY gender
    UNION ALL
    SELECT 'age_group'  AS dimension, age_group    AS segment,
           COUNT(*)     AS total, SUM(survived)    AS survived,
           ROUND(100.0 * SUM(survived) / COUNT(*), 1) AS survival_rate_pct
    FROM silver GROUP BY age_group
    UNION ALL
    SELECT 'fare_tier'  AS dimension, fare_tier    AS segment,
           COUNT(*)     AS total, SUM(survived)    AS survived,
           ROUND(100.0 * SUM(survived) / COUNT(*), 1) AS survival_rate_pct
    FROM silver GROUP BY fare_tier
    ORDER BY dimension, survival_rate_pct DESC
""")

print("\nGold — survival by demographics:")
gold_demographics.show()

gold_demographics.write.mode("overwrite").parquet(f"{GOLD}/survival_by_demographics")

# ── Final summary ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("PIPELINE COMPLETE — Layer summary")
print("="*60)

for layer, path in [("Bronze", BRONZE), ("Silver", SILVER), ("Gold", f"{GOLD}/survival_by_class")]:
    count = spark.read.parquet(path).count()
    size  = sum(
        os.path.getsize(os.path.join(root, f))
        for root, _, files in os.walk(path)
        for f in files if f.endswith(".parquet")
    )
    print(f"  {layer:8s}: {count:4d} rows  {size/1024:.1f} KB  → {path}/")

spark.stop()
print("\nMedallion pipeline complete.")