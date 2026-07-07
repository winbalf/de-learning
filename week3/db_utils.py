import logging
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine, text, pool
from sqlalchemy.exc import SQLAlchemyError

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# --- Connection pool ---
# In production you never create a new connection per query.
# A pool keeps N connections open and reuses them.
def get_engine(
    host="localhost",
    port=5433,
    db="delearning",
    user="deuser",
    password="depassword",
    pool_size=5,        # keep 5 connections open
    max_overflow=2      # allow 2 extra under load
):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(
        url,
        poolclass=pool.QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True   # test connection before using it
    )
    log.info(f"Engine created — pool_size={pool_size}")
    return engine


# --- Context manager: safe connection handling ---
# Automatically commits on success, rolls back on error, always closes
@contextmanager
def get_connection(engine):
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
        log.info("Transaction committed")
    except SQLAlchemyError as e:
        conn.rollback()
        log.error(f"Transaction rolled back: {e}")
        raise
    finally:
        conn.close()
        log.info("Connection closed")


# --- Reusable query runner ---
def run_query(engine, sql, params=None):
    with get_connection(engine) as conn:
        result = pd.read_sql(text(sql), conn, params=params)
        log.info(f"Query returned {len(result)} rows")
        return result


# --- Reusable bulk loader ---
def load_dataframe(engine, df, table, if_exists="append", chunksize=1000):
    try:
        df.to_sql(table, con=engine, if_exists=if_exists,
                  index=False, chunksize=chunksize)
        log.info(f"Loaded {len(df)} rows into {table}")
    except SQLAlchemyError as e:
        log.error(f"Failed to load into {table}: {e}")
        raise


# -------------------------------------------------------
# Test all patterns
# -------------------------------------------------------
if __name__ == "__main__":

    engine = get_engine()

    # 1. Basic parameterised query (safe from SQL injection)
    print("\n=== Parameterised query ===")
    result = run_query(
        engine,
        """SELECT "Name" as name, "Pclass" as pclass, "Fare" as fare
           FROM titanic
           WHERE "Pclass" = :cls AND "Fare" > :min_fare
           ORDER BY "Fare" DESC LIMIT 5""",
        params={"cls": 1, "min_fare": 200}
    )
    print(result)

    # 2. Load a new DataFrame into a new table
    print("\n=== Load new data into DB ===")
    summary_df = run_query(engine, """
        SELECT "Pclass" as pclass,
               COUNT(*)                              AS total,
               SUM("Survived")                       AS survived,
               ROUND(AVG("Fare")::numeric, 2)        AS avg_fare,
               ROUND(AVG("Age")::numeric, 1)         AS avg_age
        FROM titanic
        GROUP BY "Pclass"
        ORDER BY "Pclass"
    """)
    print(summary_df)
    load_dataframe(engine, summary_df, "titanic_summary", if_exists="replace")

    # 3. Simulate an error + rollback
    print("\n=== Rollback on error ===")
    try:
        with get_connection(engine) as conn:
            conn.execute(text("INSERT INTO titanic_summary VALUES (99, 999, 999, 999, 999)"))
            conn.execute(text("INSERT INTO nonexistent_table VALUES (1)"))  # will fail
    except Exception as e:
        print(f"Caught expected error: {type(e).__name__}")
        print("Rollback fired — first INSERT was undone")

    # 4. Verify rollback worked
    check = run_query(engine, "SELECT * FROM titanic_summary WHERE pclass = 99")
    print(f"\nRows with pclass=99 after rollback: {len(check)} (should be 0)")

    engine.dispose()
    log.info("Engine disposed — all connections returned to pool")