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
# Fill missing Age with median
df["Age"] = df["Age"].fillna(df["Age"].median())

# Create a new column: fare per age
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
print("\n Saved to titanic_cleaned.parquet")

# --- Read it back to verify ---
df_check = pd.read_parquet("titanic_cleaned.parquet")
print(f"Parquet rows: {len(df_check)}")