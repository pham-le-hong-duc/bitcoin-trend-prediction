"""
Airflow DAG for batch backfill of TimescaleDB dashboard and featurestore tables.

Workflow:
1. Detect gaps in TimescaleDB aggregate/feature tables
2. Reload historical parquet from MinIO
3. Recompute only the missing windows and upsert into TimescaleDB
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_dashboard_futures_klines_backfill(**kwargs):
    """Backfill dashboard futures klines aggregates."""
    from src.batch.timescaledb.dashboard.futures_klines import main

    main()


def run_dashboard_futures_metrics_backfill(**kwargs):
    """Backfill dashboard futures metrics aggregates."""
    from src.batch.timescaledb.dashboard.futures_metrics import main

    main()


def run_dashboard_sentiment_backfill(**kwargs):
    """Backfill dashboard sentiment aggregates."""
    from src.batch.timescaledb.dashboard.sentiment import main

    main()


def run_featurestore_futures_klines_backfill(**kwargs):
    """Backfill featurestore futures klines features."""
    from src.batch.timescaledb.featurestore.futures_klines import main

    main()


def run_featurestore_futures_metrics_backfill(**kwargs):
    """Backfill featurestore futures metrics features."""
    from src.batch.timescaledb.featurestore.futures_metrics import main

    main()


def run_featurestore_futures_premiumindexklines_backfill(**kwargs):
    """Backfill featurestore futures premium index klines features."""
    from src.batch.timescaledb.featurestore.futures_premiumIndexKlines import main

    main()


def run_featurestore_spot_klines_backfill(**kwargs):
    """Backfill featurestore spot klines features."""
    from src.batch.timescaledb.featurestore.spot_klines import main

    main()


def run_featurestore_sentiment_backfill(**kwargs):
    """Backfill featurestore sentiment features."""
    from src.batch.timescaledb.featurestore.sentiment import main

    main()


def run_featurestore_futures_aggtrades_backfill(**kwargs):
    """Backfill featurestore futures aggTrades features."""
    from src.batch.timescaledb.featurestore.futures_aggTrades import main

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
    description="Backfill dashboard and featurestore tables in TimescaleDB from historical MinIO parquet files.",
    schedule_interval=None,
    catchup=False,
) as dag:
    backfill_dashboard_futures_klines = PythonOperator(
        task_id="backfill_dashboard_futures_klines",
        python_callable=run_dashboard_futures_klines_backfill,
        provide_context=True,
    )

    backfill_dashboard_futures_metrics = PythonOperator(
        task_id="backfill_dashboard_futures_metrics",
        python_callable=run_dashboard_futures_metrics_backfill,
        provide_context=True,
    )

    backfill_dashboard_sentiment = PythonOperator(
        task_id="backfill_dashboard_sentiment",
        python_callable=run_dashboard_sentiment_backfill,
        provide_context=True,
    )

    backfill_featurestore_futures_klines = PythonOperator(
        task_id="backfill_featurestore_futures_klines",
        python_callable=run_featurestore_futures_klines_backfill,
        provide_context=True,
    )

    backfill_featurestore_futures_metrics = PythonOperator(
        task_id="backfill_featurestore_futures_metrics",
        python_callable=run_featurestore_futures_metrics_backfill,
        provide_context=True,
    )

    backfill_featurestore_futures_premiumindexklines = PythonOperator(
        task_id="backfill_featurestore_futures_premiumindexklines",
        python_callable=run_featurestore_futures_premiumindexklines_backfill,
        provide_context=True,
    )

    backfill_featurestore_spot_klines = PythonOperator(
        task_id="backfill_featurestore_spot_klines",
        python_callable=run_featurestore_spot_klines_backfill,
        provide_context=True,
    )

    backfill_featurestore_sentiment = PythonOperator(
        task_id="backfill_featurestore_sentiment",
        python_callable=run_featurestore_sentiment_backfill,
        provide_context=True,
    )

    backfill_featurestore_futures_aggtrades = PythonOperator(
        task_id="backfill_featurestore_futures_aggtrades",
        python_callable=run_featurestore_futures_aggtrades_backfill,
        provide_context=True,
    )
