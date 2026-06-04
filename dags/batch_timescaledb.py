"""
Airflow DAG for batch backfill of TimescaleDB dashboard aggregates.

Workflow:
1. Detect gaps in dashboard aggregate tables
2. Reload historical parquet from MinIO
3. Recompute only the missing windows and upsert into TimescaleDB
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def batch_timescaledb_futures_indexpriceklines(**kwargs):
    """Backfill dashboard futures index price klines aggregates."""
    from src.batch.timescaledb.dashboard.futures_indexpriceklines import main

    main()


def batch_timescaledb_futures_metrics(**kwargs):
    """Backfill dashboard futures metrics aggregates."""
    from src.batch.timescaledb.dashboard.futures_metrics import main

    main()


def batch_timescaledb_sentiment(**kwargs):
    """Backfill dashboard sentiment aggregates."""
    from src.batch.timescaledb.dashboard.sentiment import main

    main()


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="batch_timescaledb",
    default_args=default_args,
    description="Backfill dashboard aggregates in TimescaleDB from historical MinIO parquet files.",
    schedule_interval=None,
    catchup=False,
) as dag:
    backfill_futures_indexpriceklines = PythonOperator(
        task_id="backfill_futures_indexpriceklines",
        python_callable=batch_timescaledb_futures_indexpriceklines,
        provide_context=True,
    )

    backfill_futures_metrics = PythonOperator(
        task_id="backfill_futures_metrics",
        python_callable=batch_timescaledb_futures_metrics,
        provide_context=True,
    )

    backfill_sentiment = PythonOperator(
        task_id="backfill_sentiment",
        python_callable=batch_timescaledb_sentiment,
        provide_context=True,
    )

