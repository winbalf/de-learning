import pandas as pd
import time
import os

# --- Compare file sizes ---
csv_size = os.path.getsize("titanic.csv") / 1024
parquet_size = os.path.getsize("titanic_cleaned.parquet") / 1024
print(f"CSV size:     {csv_size:.1f} KB")
print(f"Parquet size: {parquet_size:.1f} KB")
print(f"Parquet is {csv_size / parquet_size:.1f}x smaller\n")

# --- Compare read speed ---
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
print(f"Parquet is {csv_time / parquet_time:.1f}x faster to read\n")

# --- Parquet column pruning (killer feature) ---
# With CSV you always load ALL columns even if you need one
# With Parquet you can load only what you need
start = time.time()
for _ in range(runs):
    pd.read_parquet("titanic_cleaned.parquet", columns=["Survived", "Pclass", "Fare"])
pruned_time = (time.time() - start) / runs * 1000

print(f"Parquet (3 columns only): {pruned_time:.2f} ms")
print("CSV has no equivalent — it always reads all columns")