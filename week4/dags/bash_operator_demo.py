# dags/bash_operator_demo.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "de-learner", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="bash_operator_demo",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["week4", "demo"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_postgres = BashOperator(
        task_id="check_postgres_connection",
        bash_command="pg_isready -h de-postgres -p 5432 -U deuser && echo 'DB is ready'",
    )

    count_rows = BashOperator(
        task_id="count_titanic_rows",
        bash_command="""
            PGPASSWORD=depassword psql \
                -h de-postgres -p 5432 \
                -U deuser -d delearning \
                -c 'SELECT COUNT(*) FROM titanic;'
        """,
    )

    log_date = BashOperator(
        task_id="log_execution_date",
        # Airflow injects execution date as an env var — useful for partitioned loads
        bash_command='echo "Running for date: {{ ds }} at {{ ts }}"',
    )

    end = EmptyOperator(task_id="end")

    start >> check_postgres >> count_rows >> log_date >> end