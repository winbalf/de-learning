import pandas as pd
from sqlalchemy import create_engine, text

# --- Connect ---
engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

# --- Load cleaned parquet ---
df = pd.read_parquet("titanic_cleaned.parquet")

# Drop Cabin (too many nulls - real DE decision)
df = df.drop(columns=["Cabin"])

# Fill the 2 missing Embarked values
df["Embarked"] = df["Embarked"].fillna("S")

print(f"Loading {len(df)} rows into PostgreSQL...")

# --- Write to Postgres ---
df.to_sql(
    name="titanic",
    con=engine,
    if_exists="replace",   # replace table if it exists
    index=False,
    chunksize=500          # write in batches - good practice at scale
)
print("Write complete.\n")

# --- Read back with a SQL query ---
with engine.connect() as conn:

    # Basic query
    result = pd.read_sql(
        text('SELECT "Pclass" as pclass, COUNT(*) as total, AVG("Fare") as avg_fare FROM titanic GROUP BY "Pclass" ORDER BY "Pclass"'),
        conn
    )
    print("=== Avg fare by class ===")
    print(result)

    # Window function - your existing SQL strength
    result2 = pd.read_sql(text("""
        SELECT
            "Name" as name,
            "Pclass" as pclass,
            "Fare" as fare,
            ROUND(AVG("Fare") OVER (PARTITION BY "Pclass")::numeric, 2) as class_avg_fare,
            ROUND(("Fare" - AVG("Fare") OVER (PARTITION BY "Pclass"))::numeric, 2) as diff_from_avg
        FROM titanic
        ORDER BY diff_from_avg DESC
        LIMIT 10
    """), conn)
    print("\n=== Top 10 passengers who paid most above their class average ===")
    print(result2[["name", "pclass", "fare", "class_avg_fare", "diff_from_avg"]])

print("\nPipeline complete.")