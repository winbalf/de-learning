# ingest.py — Bronze ingestion: CSV → PostgreSQL
import os
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

print("=== Loading Chicago Food Inspections ===")
df = pd.read_csv(
    "inspections_sample.csv",
    dtype=str,                    # load everything as string — let dbt handle types
    keep_default_na=False,        # don't auto-convert empty strings to NaN
    na_values=[""]                # only treat empty string as null
)

# Normalise column names — lowercase + underscores
df.columns = (
    df.columns
    .str.lower()
    .str.replace(" ", "_", regex=False) # replace spaces with underscores
    .str.replace(r"[^a-z0-9_]", "", regex=True) # remove any characters that are not lowercase letters, numbers, or underscores
    .str.replace(r"_+", "_", regex=True) # replace multiple underscores with a single underscore
    .str.strip("_") # remove leading and trailing underscores
)

print(f"Columns: {list(df.columns)}") # print the columns of the dataframe
print(f"Rows: {len(df)}") # print the number of rows in the dataframe
print(f"\nMissing values:")
print(df.isnull().sum()[df.isnull().sum() > 0]) # print the number of missing values in each column

# Add metadata
df["_ingested_at"] = pd.Timestamp.now().isoformat() # add a timestamp to the dataframe
df["_source_file"] = "inspections_sample.csv" # add the source file to the dataframe

# Load to PostgreSQL
print("\nLoading to PostgreSQL...")
df.to_sql(
    "raw_inspections", # the name of the table to load the dataframe into
    con=engine,
    schema="public",
    if_exists="replace", # replace the table if it already exists
    index=False, # do not include the index in the table
    chunksize=500 # load the dataframe in chunks of 500 rows
) # load the dataframe into the PostgreSQL database
print(f"Loaded {len(df)} rows into public.raw_inspections")

# Quick verify
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM raw_inspections")).scalar()
    sample = pd.read_sql(text("""
        SELECT inspection_id, dba_name, facility_type, risk, results, inspection_date
        FROM raw_inspections LIMIT 5
    """), conn) # read the first 5 rows of the table into a pandas dataframe
    print(f"\nVerification: {count} rows in DB")
    print(sample)

engine.dispose()
print("\nIngestion complete.")