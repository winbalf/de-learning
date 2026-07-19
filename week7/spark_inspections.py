# spark_inspections.py — PySpark medallion pipeline for Chicago inspections

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ChicagoInspections") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

run_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
BRONZE = "output/bronze"
SILVER = "output/silver"
GOLD   = "output/gold"

# ── BRONZE ────────────────────────────────────────────────────────────────────
print("\n=== BRONZE — Raw ingestion ===")
# read the inspections_sample.csv file into a DataFrame and add the _ingested_at and _source_file columns
bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("inspections_sample.csv") \
    .withColumn("_ingested_at", F.lit(run_ts)) \
    .withColumn("_source_file", F.lit("inspections_sample.csv"))

print(f"Bronze rows: {bronze.count()}")
# write the DataFrame to the Bronze layer as parquet files and overwrite the existing files
bronze.write.mode("overwrite").parquet(BRONZE)

# ── SILVER ────────────────────────────────────────────────────────────────────
print("\n=== SILVER — Clean and validate ===")
# read the Bronze layer as a DataFrame and assign it to the raw variable
raw = spark.read.parquet(BRONZE)

# DQ checks
"""
    Data Quality Checks:
    - total: the total number of rows in the DataFrame
    - null_ids: the number of rows with a null inspection ID
    - unique_ids: the number of unique inspection IDs
    - null_results: the number of rows with a null result
"""
dq = raw.agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("Inspection ID").isNull(), 1).otherwise(0)).alias("null_ids"),
    F.countDistinct("Inspection ID").alias("unique_ids"),
    F.sum(F.when(F.col("Results").isNull(), 1).otherwise(0)).alias("null_results")
).collect()[0]

print(f"DQ: total={dq.total}, null_ids={dq.null_ids}, unique={dq.unique_ids}, null_results={dq.null_results}")

silver = raw \
    .withColumnRenamed("Inspection ID",   "inspection_id") \
    .withColumnRenamed("DBA Name",        "business_name") \
    .withColumnRenamed("Facility Type",   "facility_type") \
    .withColumnRenamed("Risk",            "risk_raw") \
    .withColumnRenamed("Results",         "result_raw") \
    .withColumnRenamed("Inspection Date", "inspection_date_str") \
    .withColumnRenamed("Violations",      "violations_raw") \
    .withColumnRenamed("Latitude",        "latitude") \
    .withColumnRenamed("Longitude",       "longitude") \
    .withColumn("inspection_id",   F.col("inspection_id").cast("integer")) \
    .withColumn("inspection_date", F.to_date(F.col("inspection_date_str"), "MM/dd/yyyy")) \
    .withColumn("inspection_year", F.year("inspection_date")) \
    .withColumn("risk_level",
        F.when(F.col("risk_raw").contains("1"), "High")
         .when(F.col("risk_raw").contains("2"), "Medium")
         .when(F.col("risk_raw").contains("3"), "Low")
         .otherwise("Unknown")) \
    .withColumn("result",
        F.when(F.lower(F.col("result_raw")).contains("pass"), "Pass")
         .when(F.lower(F.col("result_raw")).contains("fail"), "Fail")
         .otherwise("Other")) \
    .withColumn("passed", F.col("result") == "Pass") \
    .withColumn("violation_count",
        F.when(F.col("violations_raw").isNull(), F.lit(0))
         .otherwise(F.size(F.split(F.col("violations_raw"), r"\|")))) \
    .withColumn("latitude",  F.col("latitude").cast("double")) \
    .withColumn("longitude", F.col("longitude").cast("double")) \
    .withColumn("_processed_at", F.lit(run_ts)) \
    .dropDuplicates(["inspection_id"]) \
    .filter(F.col("inspection_id").isNotNull())

print(f"Silver rows: {silver.count()}")
# write the DataFrame to the Silver layer as parquet files and overwrite the existing files and partition by inspection_year
# this is to improve query performance by allowing Spark to read only the data for the year of interest
silver.write.mode("overwrite") \
    .partitionBy("inspection_year") \
    .parquet(SILVER)
print(f"Silver written — partitioned by inspection_year")

# ── GOLD ──────────────────────────────────────────────────────────────────────
print("\n=== GOLD — Business-ready aggregations ===")
# read the Silver layer as a DataFrame and assign it to the silver_df variable
# create a temporary view of the DataFrame to allow SQL queries to be executed on it
silver_df = spark.read.parquet(SILVER)
silver_df.createOrReplaceTempView("silver")

# Gold 1: pass rates by risk
# this query calculates the pass rate by risk level
# it filters the data to only include the pass and fail results
gold_risk = spark.sql("""
    SELECT risk_level,
           COUNT(*)                                         AS total,
           SUM(CAST(passed AS INT))                        AS passed,
           ROUND(100.0 * SUM(CAST(passed AS INT))
                 / COUNT(*), 1)                            AS pass_rate_pct,
           ROUND(AVG(violation_count), 2)                  AS avg_violations
    FROM silver
    WHERE result IN ('Pass','Fail')
    GROUP BY risk_level
    ORDER BY pass_rate_pct
""")
print("\nGold — pass rate by risk:")
gold_risk.show()
# write the DataFrame to the Gold layer as parquet files and overwrite the existing files
gold_risk.write.mode("overwrite").parquet(f"{GOLD}/pass_by_risk")

# Gold 2: yearly trends
"""
    Gold 2: yearly trends    
    this query calculates the yearly trends in pass rates and average violations
    - it filters the data to only include the years 2010 to 2024
    - it groups the data by inspection year
    - it calculates the total number of inspections, the pass rate percentage, and the average number of violations
    - it orders the data by inspection year
"""
gold_yearly = spark.sql("""
    SELECT inspection_year,
           COUNT(*)                                         AS total_inspections,
           ROUND(100.0 * SUM(CAST(passed AS INT))
                 / COUNT(*), 1)                            AS pass_rate_pct,
           ROUND(AVG(violation_count), 2)                  AS avg_violations
    FROM silver
    WHERE result IN ('Pass','Fail')
      AND inspection_year BETWEEN 2010 AND 2024
    GROUP BY inspection_year
    ORDER BY inspection_year
""")
print("\nGold — yearly trends:")
gold_yearly.show()
# write the DataFrame to the Gold layer as parquet files and overwrite the existing files
gold_yearly.write.mode("overwrite").parquet(f"{GOLD}/yearly_trends")

# Gold 3: window function — rank businesses by failures within risk level
"""
    Gold 3: window function — rank businesses by failures within risk level
    this query ranks the businesses by failures within risk level
    - it filters the data to only include the years 2015 and later
    - it groups the data by business name and risk level
    - it calculates the total number of inspections, the number of failures, and the average number of violations
    - it filters the data to only include businesses with at least 3 inspections
    - it creates a window function to rank the businesses by failures within risk level
    - it filters the data to only include the top 3 businesses by failures within risk level
"""
print("\nGold — top failing businesses by risk level (window function):")
business_stats = silver_df \
    .filter((F.col("result").isin(["Pass", "Fail"])) & (F.col("inspection_year") >= 2015)) \
    .groupBy("business_name", "risk_level") \
    .agg(
        F.count("*").alias("total"),
        F.sum((F.col("passed") == False).cast("integer")).alias("failures"),
        F.round(F.avg("violation_count"), 2).alias("avg_violations")
    ) \
    .filter(F.col("total") >= 3)

window = Window.partitionBy("risk_level").orderBy(F.col("failures").desc())

# create a window function to rank the businesses by failures within risk level
ranked = business_stats \
    .withColumn("rank_in_risk", F.rank().over(window)) \
    .filter(F.col("rank_in_risk") <= 3) \
    .orderBy("risk_level", "rank_in_risk")

ranked.show(20, truncate=False)
ranked.write.mode("overwrite").parquet(f"{GOLD}/top_failing_by_risk")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n=== Pipeline summary ===")
for layer, path in [("Bronze", BRONZE), ("Silver", SILVER), ("Gold", f"{GOLD}/pass_by_risk")]:
    df = spark.read.parquet(path)
    size = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, files in os.walk(path)
        for f in files if f.endswith(".parquet")
    )
    print(f"  {layer:8s}: {df.count():6d} rows  {size/1024:.1f} KB")

spark.stop()
print("\nDone.")
