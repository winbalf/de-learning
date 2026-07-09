# dags/branching_demo.py
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "de-learner", "retries": 0}

DB_CONF = {
    "host": "de-postgres", "port": 5432,
    "dbname": "delearning", "user": "deuser", "password": "depassword",
}

def check_data_volume(**context):
    """Check row count and branch based on volume."""
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM titanic")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Row count: {count}")
    context["ti"].xcom_push(key="row_count", value=count)

    # Branch decision
    if count == 0:
        return "handle_empty_table"       # task_id to run next
    elif count < 500:
        return "handle_small_dataset"
    else:
        return "handle_full_dataset"


def handle_empty_table(**context):
    print("ALERT: Table is empty — triggering data reload")


def handle_small_dataset(**context):
    count = context["ti"].xcom_pull(key="row_count", task_ids="check_data_volume")
    print(f"Small dataset ({count} rows) — running lightweight transform")


def handle_full_dataset(**context):
    count = context["ti"].xcom_pull(key="row_count", task_ids="check_data_volume")
    print(f"Full dataset ({count} rows) — running complete pipeline")
    # In real life: trigger the full titanic_pipeline DAG here


def send_completion_report(**context):
    """Runs regardless of which branch was taken."""
    count = context["ti"].xcom_pull(key="row_count", task_ids="check_data_volume")
    print(f"Pipeline complete — processed {count} rows")


with DAG(
    dag_id="branching_demo",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["week4", "demo"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="check_data_volume",
        python_callable=check_data_volume,
    )

    empty    = PythonOperator(task_id="handle_empty_table",   python_callable=handle_empty_table)
    small    = PythonOperator(task_id="handle_small_dataset", python_callable=handle_small_dataset)
    full     = PythonOperator(task_id="handle_full_dataset",  python_callable=handle_full_dataset)

    # trigger_rule="none_failed_min_one_success" means:
    # run this task if at least one upstream succeeded and none failed
    # — needed after a branch so this task always runs regardless of which path was taken
    report = PythonOperator(
        task_id="send_completion_report",
        python_callable=send_completion_report,
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> branch >> [empty, small, full] >> report >> end