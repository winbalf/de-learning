from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
import psycopg2
import time

default_args = {"owner": "de-learner", "retries": 1}

DB_CONF = {
    "host": "de-postgres", "port": 5432,
    "dbname": "delearning", "user": "deuser", "password": "depassword",
}

def wait_for_titanic_data():
    """Returns True when data is ready, False to keep waiting."""
    try:
        conn = psycopg2.connect(**DB_CONF)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM titanic")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"Sensor check: {count} rows found")
        return count > 0
    except Exception as e:
        print(f"Sensor check failed: {e}")
        return False


def process_after_sensor(**context):
    print("Sensor condition met — proceeding with processing")
    conn = psycopg2.connect(**DB_CONF)
    cur = conn.cursor()
    cur.execute("""
        SELECT "Pclass", COUNT(*), ROUND(AVG("Fare")::numeric, 2)
        FROM titanic GROUP BY "Pclass" ORDER BY "Pclass"
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for r in rows:
        print(f"  Class {r[0]}: {r[1]} passengers, avg fare £{r[2]}")


with DAG(
    dag_id="sensor_demo",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["week4", "demo"],
) as dag:

    start = EmptyOperator(task_id="start")

    # PythonSensor — polls every 30s until condition is True or timeout
    wait_for_data = PythonSensor(
        task_id="wait_for_titanic_data",
        python_callable=wait_for_titanic_data,
        poke_interval=30,    # check every 30 seconds
        timeout=300,         # fail after 5 minutes
        mode="poke",         # hold the worker slot while waiting
    )

    process = PythonOperator(
        task_id="process_after_sensor",
        python_callable=process_after_sensor,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_data >> process >> end
