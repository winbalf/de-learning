# inspections_pipeline.py
# Orchestrates the Chicago food inspections pipeline
from datetime import datetime, timedelta
import subprocess
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "de-learner",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

DB_CONF = {
    "host": "de-postgres", "port": 5432,
    "dbname": "delearning", "user": "deuser", "password": "depassword",
}

def check_source_data(**context):
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_inspections")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"Source rows: {count}")
    if count == 0:
        raise ValueError("raw_inspections is empty — run ingest.py first")
    context["ti"].xcom_push(key="source_count", value=count)
    return count

def run_dq_checks(**context):
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                                        AS total,
            COUNT(*) FILTER (WHERE inspection_id IS NULL)  AS null_ids,
            COUNT(*) FILTER (WHERE results IS NULL)        AS null_results,
            COUNT(DISTINCT inspection_id)                  AS unique_ids
        FROM raw_inspections
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    total, null_ids, null_results, unique_ids = row
    print(f"Total: {total} | Null IDs: {null_ids} | Null results: {null_results} | Unique IDs: {unique_ids}")

    issues = []
    if null_ids > 0:
        issues.append(f"{null_ids} null inspection IDs")
    if unique_ids != total:
        issues.append(f"{total - unique_ids} duplicate IDs")
    if null_results > total * 0.1:
        issues.append(f"{null_results} null results (>{10}% threshold)")

    if issues:
        raise ValueError(f"DQ failed: {', '.join(issues)}")

    print("All DQ checks passed")
    context["ti"].xcom_push(key="dq_passed", value=True)

def check_dbt_needed(**context):
    """Branch — only run dbt if source count changed since last run."""
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()

    # Check if fct tables exist and have data
    try:
        cur.execute("""
            SELECT COUNT(*) FROM inspections_dbt.fct_yearly_trends
        """)
        existing = cur.fetchone()[0]
    except Exception:
        existing = 0
    finally:
        cur.close()
        conn.close()

    source_count = context["ti"].xcom_pull(key="source_count", task_ids="check_source_data")
    print(f"Source: {source_count} rows | Existing mart rows: {existing}")

    # Always rebuild for now — in production you'd compare checksums
    return "run_dbt_models"

def log_pipeline_completion(**context):
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*), ROUND(AVG(pass_rate_pct), 1)
        FROM inspections_dbt.fct_inspection_summary
    """)
    segments, avg_pass = cur.fetchone()

    cur.execute("""
        INSERT INTO pipeline_runs (pipeline, status, rows_loaded, finished_at)
        VALUES (%s, 'success', %s, NOW())
    """, (f"inspections_dag_{context['ds']}", segments))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Pipeline complete — {segments} segments, avg pass rate {avg_pass}%")


with DAG(
    dag_id="chicago_inspections_pipeline",
    description="Chicago food inspections — DQ, dbt transform, audit log",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["inspections", "week7", "capstone"],
) as dag:

    # start task
    start = EmptyOperator(task_id="start")

    # check source data task
    check_source = PythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data,
    )

    # run dq checks task
    dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_dq_checks,
    )

    # check dbt needed task
    branch = BranchPythonOperator(
        task_id="check_dbt_needed",
        python_callable=check_dbt_needed,
    )

    # dbt runs as a BashOperator — exactly how it works in production
    # run dbt models task
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            cd /opt/airflow/dags &&
            echo "dbt would run here in production" &&
            echo "Models: stg_inspections, int_inspections_enriched, fct_inspection_summary, fct_yearly_trends, fct_top_failing_businesses"
        """,
    )

    # log pipeline completion task
    log_completion = PythonOperator(
        task_id="log_pipeline_completion",
        python_callable=log_pipeline_completion,
        trigger_rule="none_failed_min_one_success",
    )


    # end task
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )
    
    # define the task dependencies
    start >> check_source >> dq_checks >> branch >> run_dbt >> log_completion >> end
