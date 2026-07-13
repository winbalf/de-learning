# spark_basics.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ── Create a Spark session ────────────────────────────────────────────────────
# SparkSession is the entry point to everything in Spark
# In production this connects to a cluster — locally it runs on your machine
spark = SparkSession.builder \
    .appName("TitanicAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Suppress verbose logs — keep output readable
spark.sparkContext.setLogLevel("ERROR")

print("=== Spark session created ===")
print(f"Spark version: {spark.version}")
print(f"App name: {spark.sparkContext.appName}")

# ── Read CSV into a Spark DataFrame ──────────────────────────────────────────
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("../week1/titanic.csv")

print(f"\n=== Schema ===")
df.printSchema()

print(f"\n=== Row count ===")
print(f"Total rows: {df.count()}")   # .count() is an ACTION — triggers execution

print(f"\n=== First 5 rows ===")
df.show(5, truncate=True)

# ── Transformations — lazy, nothing executes yet ──────────────────────────────
# This just builds a query plan
cleaned = df \
    .withColumnRenamed("PassengerId", "passenger_id") \
    .withColumnRenamed("Survived", "survived") \
    .withColumnRenamed("Pclass", "passenger_class") \
    .withColumnRenamed("Name", "full_name") \
    .withColumnRenamed("Sex", "gender") \
    .withColumnRenamed("Age", "age") \
    .withColumnRenamed("Fare", "fare_gbp") \
    .withColumnRenamed("Embarked", "embarkation_code") \
    .withColumn("age", F.coalesce(F.col("age"), F.lit(28.0))) \
    .withColumn("fare_gbp", F.round(F.col("fare_gbp"), 2)) \
    .withColumn("embarkation_code", F.coalesce(F.col("embarkation_code"), F.lit("S"))) \
    .withColumn("family_size", F.col("SibSp") + F.col("Parch")) \
    .withColumn("is_solo", F.when(F.col("family_size") == 0, True).otherwise(False)) \
    .withColumn("survival_status",
        F.when(F.col("survived") == 1, "survived").otherwise("died")
    )

print(f"\n=== Cleaned schema ===")
cleaned.printSchema()

# ── Aggregations ──────────────────────────────────────────────────────────────
print(f"\n=== Survival rate by class ===")
survival_by_class = cleaned \
    .groupBy("passenger_class") \
    .agg(
        F.count("*").alias("total"),
        F.sum("survived").alias("survived"),
        F.round(F.avg("fare_gbp"), 2).alias("avg_fare"),
        F.round(F.avg("age"), 1).alias("avg_age"),
        F.round(
            F.sum("survived") / F.count("*") * 100, 1
        ).alias("survival_rate_pct")
    ) \
    .orderBy("passenger_class")

survival_by_class.show()

print(f"\n=== Survival rate by gender ===")
cleaned \
    .groupBy("gender", "survival_status") \
    .count() \
    .orderBy("gender", "survival_status") \
    .show()

# ── Window functions — same concepts as SQL week 2 ───────────────────────────
from pyspark.sql.window import Window

print(f"\n=== Fare ranking within class (window function) ===")
window = Window.partitionBy("passenger_class").orderBy(F.col("fare_gbp").desc())

ranked = cleaned \
    .withColumn("fare_rank", F.rank().over(window)) \
    .select("passenger_class", "full_name", "fare_gbp", "fare_rank") \
    .filter(F.col("fare_rank") <= 3) \
    .orderBy("passenger_class", "fare_rank")

ranked.show(10, truncate=False)

# ── Write to Parquet ──────────────────────────────────────────────────────────
print(f"\n=== Writing to Parquet ===")
cleaned.write \
    .mode("overwrite") \
    .partitionBy("passenger_class") \
    .parquet("output/titanic_cleaned")

print("Written to output/titanic_cleaned/")

# Read back and verify
df_parquet = spark.read.parquet("output/titanic_cleaned")
print(f"Parquet row count: {df_parquet.count()}")
print(f"Parquet partitions: {df_parquet.rdd.getNumPartitions()}")

# Show partition structure on disk
import os
for root, dirs, files in os.walk("output/titanic_cleaned"):
    level = root.replace("output/titanic_cleaned", "").count(os.sep)
    indent = " " * 2 * level
    print(f"{indent}{os.path.basename(root)}/")
    for file in files:
        print(f"{indent}  {file}")

spark.stop()
print("\nSpark session stopped.")