import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://deuser:depassword@localhost:5433/delearning"
)

with engine.connect() as conn:

    # --- Recursive CTE: generate a sequence (classic use case) ---
    print("=== Recursive CTE — generate numbers 1 to 10 ===")
    r1 = pd.read_sql(text("""
        WITH RECURSIVE counter(n) AS (
            SELECT 1                          -- base case
            UNION ALL
            SELECT n + 1 FROM counter WHERE n < 10  -- recursive case
        )
        SELECT n FROM counter
    """), conn)
    print(r1.T)  # print transposed so it fits on one line

    # --- Recursive CTE: real DE use case — employee hierarchy ---
    # First create a small hierarchy table
    conn.execute(text("""
        DROP TABLE IF EXISTS employees;
        CREATE TABLE employees (
            id INT, name TEXT, role TEXT, manager_id INT
        );
        INSERT INTO employees VALUES
            (1, 'Alice',   'CTO',              NULL),
            (2, 'Bob',     'Data Lead',        1),
            (3, 'Carol',   'Data Engineer',    2),
            (4, 'Dave',    'Data Engineer',    2),
            (5, 'Eve',     'Analytics Lead',   1),
            (6, 'Frank',   'Data Analyst',     5),
            (7, 'Grace',   'Data Analyst',     5);
    """))
    conn.commit()

    print("\n=== Recursive CTE — org chart traversal ===")
    r2 = pd.read_sql(text("""
        WITH RECURSIVE org_chart AS (
            -- Base: start from the top (no manager)
            SELECT id, name, role, manager_id, 0 AS depth,
                   name::TEXT AS path
            FROM employees
            WHERE manager_id IS NULL

            UNION ALL

            -- Recursive: find each person's reports
            SELECT e.id, e.name, e.role, e.manager_id,
                   oc.depth + 1,
                   oc.path || ' → ' || e.name
            FROM employees e
            JOIN org_chart oc ON e.manager_id = oc.id
        )
        SELECT depth, name, role, path
        FROM org_chart
        ORDER BY path
    """), conn)
    print(r2.to_string(index=False))

    # --- EXPLAIN ANALYZE: understand query performance ---
    print("\n=== EXPLAIN ANALYZE — query without index ===")
    plan1 = pd.read_sql(text("""
        EXPLAIN ANALYZE
        SELECT * FROM titanic WHERE "Fare" > 100
    """), conn)
    for row in plan1["QUERY PLAN"]:
        print(row)

    # --- Create an index and compare ---
    conn.execute(text('CREATE INDEX IF NOT EXISTS idx_titanic_fare ON titanic("Fare")'))
    conn.commit()

    print("\n=== EXPLAIN ANALYZE — query WITH index ===")
    plan2 = pd.read_sql(text("""
        EXPLAIN ANALYZE
        SELECT * FROM titanic WHERE "Fare" > 100
    """), conn)
    for row in plan2["QUERY PLAN"]:
        print(row)

    # --- Common DE anti-pattern: SELECT * in production ---
    print("\n=== Anti-pattern vs optimised query ===")

    # Bad: reads all columns, all rows, then filters in Python
    bad = pd.read_sql(text("SELECT * FROM titanic"), conn)
    bad_result = bad[bad["Fare"] > 100][["Name", "Fare", "Pclass"]]
    print(f"Anti-pattern: loaded {len(bad)} rows, kept {len(bad_result)}")

    # Good: filter and project in SQL, only transfer what you need
    good = pd.read_sql(text("""
        SELECT "Name" as name, "Fare" as fare, "Pclass" as pclass
        FROM titanic
        WHERE "Fare" > 100
        ORDER BY "Fare" DESC
    """), conn)
    print(f"Optimised:    loaded {len(good)} rows directly")
    print(good.head())

print("\nQuery optimisation examples complete.")