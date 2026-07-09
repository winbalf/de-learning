from datetime import datetime, timedelta
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "de-learner",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

DB_CONF = {
    "host": "de-postgres",
    "port": 5432,
    "dbname": "delearning",
    "user": "deuser",
    "password": "depassword",
}

def get_conn():
    return psycopg2.connect(**DB_CONF)


def check_source_data(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM titanic')
    row_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    if row_count == 0:
        raise ValueError("titanic table is empty — aborting")
    print(f"Source check passed — {row_count} rows found")
    context["ti"].xcom_push(key="source_row_count", value=row_count)


def run_data_quality(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE "Name" IS NULL)             AS null_names,
            COUNT(*) FILTER (WHERE "Fare" < 0)                 AS negative_fares,
            COUNT(*) FILTER (WHERE "Age" < 0 OR "Age" > 120)  AS invalid_ages,
            COUNT(*) - COUNT(DISTINCT "PassengerId")           AS duplicate_ids
        FROM titanic
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    issues = {
        "null_names": row[0], "negative_fares": row[1],
        "invalid_ages": row[2], "duplicate_ids": row[3],
    }
    print("=== DQ Results ===")
    for check, count in issues.items():
        print(f"  [{'PASS' if count == 0 else 'FAIL'}] {check}: {count}")
    failed = {k: v for k, v in issues.items() if v > 0}
    if failed:
        raise ValueError(f"DQ checks failed: {failed}")
    print("All DQ checks passed")


def build_summary(**context):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            "Pclass",
            COUNT(*),
            SUM("Survived"),
            ROUND(AVG("Fare")::numeric, 2),
            ROUND(100.0 * SUM("Survived") / COUNT(*)::numeric, 1),
            ROUND(AVG("Age")::numeric, 1)
        FROM titanic
        GROUP BY "Pclass"
        ORDER BY "Pclass"
    """)
    rows = cur.fetchall()
    print("=== Summary ===")
    print(f"{'pclass':>8} {'total':>6} {'survived':>9} {'avg_fare':>9} {'surv_%':>7} {'avg_age':>8}")
    for r in rows:
        print(f"{r[0]:>8} {r[1]:>6} {r[2]:>9} {r[3]:>9} {r[4]:>7} {r[5]:>8}")
    cur.execute("DROP TABLE IF EXISTS titanic_summary")
    cur.execute("""
        CREATE TABLE titanic_summary (
            pclass INT, total INT, survived NUMERIC,
            avg_fare NUMERIC, survival_pct NUMERIC, avg_age NUMERIC
        )
    """)
    cur.executemany("INSERT INTO titanic_summary VALUES (%s,%s,%s,%s,%s,%s)", rows)
    conn.commit()
    cur.close()
    conn.close()
    ti = context["ti"]
    source_count = ti.xcom_pull(key="source_row_count", task_ids="check_source_data")
    print(f"Summary built from {source_count} source rows → {len(rows)} summary rows")
    ti.xcom_push(key="summary_rows", value=len(rows))


def log_pipeline_run(**context):
    conn = get_conn()
    cur = conn.cursor()
    ti = context["ti"]
    summary_rows = ti.xcom_pull(key="summary_rows", task_ids="build_summary")
    run_date = context["ds"]
    cur.execute("""
        INSERT INTO pipeline_runs (pipeline, status, rows_loaded, finished_at)
        VALUES (%s, 'success', %s, NOW())
    """, (f"titanic_dag_{run_date}", summary_rows))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Pipeline run logged — date={run_date}, summary_rows={summary_rows}")


with DAG(
    dag_id="titanic_pipeline",
    description="Titanic ETL — DQ checks, summary build, run logging",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["titanic", "week4"],
) as dag:

    start = EmptyOperator(task_id="start")
    t1 = PythonOperator(task_id="check_source_data",  python_callable=check_source_data)
    t2 = PythonOperator(task_id="run_data_quality",   python_callable=run_data_quality)
    t3 = PythonOperator(task_id="build_summary",      python_callable=build_summary)
    t4 = PythonOperator(task_id="log_pipeline_run",   python_callable=log_pipeline_run)
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> t4 >> end
