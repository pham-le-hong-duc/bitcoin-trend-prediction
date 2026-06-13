"""
Airflow DAG for BTC dashboard baseline predictions.

The prediction job reads existing dashboard aggregates from TimescaleDB and
upserts one next-step forecast per supported interval.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_dashboard_prediction(**kwargs):
    from src.batch.timescaledb.dashboard.prediction import main

    main()


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="dashboard_prediction",
    default_args=default_args,
    description="Generate baseline BTC predictions for the Grafana dashboard.",
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    predict_dashboard = PythonOperator(
        task_id="predict_dashboard",
        python_callable=run_dashboard_prediction,
        provide_context=True,
    )
