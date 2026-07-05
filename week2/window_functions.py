import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

with engine.connect() as conn:

    # --- ROW_NUMBER: assign a unique rank per group ---
    print("=== ROW_NUMBER — first ticket purchased per class ===")
    r1 = pd.read_sql(text("""
        SELECT name, pclass, passengerid,
            ROW_NUMBER() OVER (PARTITION BY pclass ORDER BY passengerid) as row_num
        FROM titanic
        QUALIFY row_num = 1  -- first passenger per class
    """), conn) if False else pd.read_sql(text("""
        SELECT * FROM (
            SELECT "Name" as name, "Pclass" as pclass, "PassengerId" as passengerid,
                ROW_NUMBER() OVER (PARTITION BY "Pclass" ORDER BY "PassengerId") as row_num
            FROM titanic
        ) t WHERE row_num = 1
    """), conn)
    print(r1)

    # --- RANK vs DENSE_RANK ---
    print("\n=== RANK vs DENSE_RANK — fare ranking within class ===")
    r2 = pd.read_sql(text("""
        SELECT "Name" as name, "Pclass" as pclass, "Fare" as fare,
            RANK()       OVER (PARTITION BY "Pclass" ORDER BY "Fare" DESC) as rank,
            DENSE_RANK() OVER (PARTITION BY "Pclass" ORDER BY "Fare" DESC) as dense_rank
        FROM titanic
        WHERE "Pclass" = 1
        ORDER BY "Fare" DESC
        LIMIT 10
    """), conn)
    print(r2)

    # --- LAG / LEAD: compare a row to previous/next row ---
    print("\n=== LAG — compare each passenger fare to previous passenger ===")
    r3 = pd.read_sql(text("""
        SELECT "PassengerId" as passengerid, "Name" as name, "Fare" as fare,
            LAG("Fare")  OVER (ORDER BY "PassengerId") as prev_fare,
            LEAD("Fare") OVER (ORDER BY "PassengerId") as next_fare,
            ROUND(("Fare" - LAG("Fare") OVER (ORDER BY "PassengerId"))::numeric, 2) as diff_from_prev
        FROM titanic
        ORDER BY "PassengerId"
        LIMIT 10
    """), conn)
    print(r3[["passengerid", "name", "fare", "prev_fare", "diff_from_prev"]])

    # --- Running totals and moving averages ---
    print("\n=== Running total of fares by class ===")
    r4 = pd.read_sql(text("""
        SELECT "PassengerId" as passengerid, "Pclass" as pclass, "Fare" as fare,
            SUM("Fare")  OVER (PARTITION BY "Pclass" ORDER BY "PassengerId") as running_total,
            ROUND(AVG("Fare") OVER (
                PARTITION BY "Pclass"
                ORDER BY "PassengerId"
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )::numeric, 2) as moving_avg_3
        FROM titanic
        WHERE "Pclass" = 1
        ORDER BY "PassengerId"
        LIMIT 10
    """), conn)
    print(r4)

    # --- NTILE: split into buckets ---
    print("\n=== NTILE — split passengers into 4 fare quartiles ===")
    r5 = pd.read_sql(text("""
        SELECT "Name" as name, "Fare" as fare,
            NTILE(4) OVER (ORDER BY "Fare") as fare_quartile
        FROM titanic
        ORDER BY "Fare" DESC
        LIMIT 12
    """), conn)
    print(r5)

print("\nAll window function examples complete.")
