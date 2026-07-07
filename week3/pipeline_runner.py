import os
import sys
import logging
import pandas as pd
from sqlalchemy import text


from db_utils import get_engine, get_connection, run_query, load_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def log_pipeline_start(engine, pipeline_name):
    with get_connection(engine) as conn:
        result = conn.execute(text("""
            INSERT INTO pipeline_runs (pipeline, status, started_at)
            VALUES (:name, 'running', NOW())
            RETURNING id
        """), {"name": pipeline_name})
        run_id = result.fetchone()[0]
    log.info(f"Pipeline run started — id={run_id}")
    return run_id


def log_pipeline_end(engine, run_id, status, rows_loaded=None):
    with get_connection(engine) as conn:
        conn.execute(text("""
            UPDATE pipeline_runs
            SET status = :status,
                rows_loaded = :rows,
                finished_at = NOW()
            WHERE id = :id
        """), {"status": status, "rows": rows_loaded, "id": run_id})
    log.info(f"Pipeline run {run_id} finished — status={status}, rows={rows_loaded}")


def run_data_quality_checks(engine, table):
    checks = run_query(engine, f"""
        SELECT
            COUNT(*)                                          AS total_rows,
            COUNT(*) FILTER (WHERE "Name" IS NULL)            AS null_names,
            COUNT(*) FILTER (WHERE "Fare" < 0)                AS negative_fares,
            COUNT(*) FILTER (WHERE "Age" < 0 OR "Age" > 120) AS invalid_ages,
            COUNT(DISTINCT "PassengerId")                     AS unique_passengers
        FROM {table}
    """)
    row = checks.iloc[0]
    results = [
        ("no_null_names",     int(row.null_names) == 0,     f"{int(row.null_names)} nulls found"),
        ("no_negative_fares", int(row.negative_fares) == 0, f"{int(row.negative_fares)} negative fares"),
        ("valid_ages",        int(row.invalid_ages) == 0,   f"{int(row.invalid_ages)} invalid ages"),
        ("unique_passengers", int(row.unique_passengers) == int(row.total_rows),
         f"{int(row.unique_passengers)} unique of {int(row.total_rows)} total"),
    ]
    dq_rows = []
    for check_name, passed, details in results:
        log.info(f"DQ check [{'PASS' if passed else 'FAIL'}] {check_name}: {details}")
        dq_rows.append({"table_name": table, "check_name": check_name,
                        "passed": passed, "details": details})
    load_dataframe(engine, pd.DataFrame(dq_rows), "data_quality_log", if_exists="append")
    return all(r[1] for r in results)


if __name__ == "__main__":

    # Read from environment — works locally AND inside Docker
    engine = get_engine(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5433)),
        db=os.getenv("DB_NAME", "delearning"),
        user=os.getenv("DB_USER", "deuser"),
        password=os.getenv("DB_PASSWORD", "depassword"),
    )

    pipeline = "titanic_summary_refresh"
    run_id = log_pipeline_start(engine, pipeline)

    try:
        summary = run_query(engine, """
            SELECT
                "Pclass"                                          AS pclass,
                COUNT(*)                                          AS total,
                SUM("Survived")                                   AS survived,
                ROUND(AVG("Fare")::numeric, 2)                    AS avg_fare,
                ROUND(100.0 * SUM("Survived") / COUNT(*)::numeric, 1) AS survival_pct,
                ROUND(AVG("Age")::numeric, 1)                     AS avg_age
            FROM titanic
            GROUP BY "Pclass"
            ORDER BY "Pclass"
        """)
        print("\n=== Summary ===")
        print(summary.to_string(index=False))

        load_dataframe(engine, summary, "titanic_summary", if_exists="replace")

        print("\n=== Data quality checks ===")
        all_passed = run_data_quality_checks(engine, "titanic")

        log_pipeline_end(engine, run_id,
                         status="success" if all_passed else "dq_warning",
                         rows_loaded=len(summary))

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        log_pipeline_end(engine, run_id, status="failed")
        raise

    finally:
        engine.dispose()

    print("\n=== Pipeline run history ===")
    history = run_query(engine, "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5")
    print(history.to_string(index=False))

    print("\n=== Data quality log ===")
    dq_log = run_query(engine, "SELECT * FROM data_quality_log ORDER BY checked_at DESC LIMIT 8")
    print(dq_log.to_string(index=False))
