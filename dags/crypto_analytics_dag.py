import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

ALERT_EMAIL = os.getenv("ALERT_EMAIL", "admin@example.com")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": True,
}

SPARK_SUBMIT_CMD = """
/opt/spark-jobs/submit_analytics.sh
"""

with DAG(
    dag_id="crypto_analytics",
    default_args=default_args,
    description="Read Parquet from MinIO → Calculate metrics → Write to PostgreSQL",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["crypto", "spark", "analytics"],
    max_active_runs=1,
) as dag:

    run_analytics = BashOperator(
        task_id="run_spark_analytics",
        bash_command=SPARK_SUBMIT_CMD,
        sla=timedelta(minutes=3),
    )