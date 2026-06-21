"""
Airflow DAG for batch processing cryptocurrency data from Binance to MinIO.

Workflow:
1. Download bulk historical data from Binance public repository
2. Backfill gaps using REST API (3-tier gap detection)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# ========== Download Task Functions ==========

def download_binance_futures_aggtrades(**kwargs):
    """Download Binance Futures aggregate trades data."""
    from src.batch.minio.download.binance.futures_aggtrades import main
    main()


def download_binance_futures_klines(**kwargs):
    """Download Binance Futures klines data."""
    from src.batch.minio.download.binance.futures_klines import main
    main()


# def download_binance_spot_aggtrades(**kwargs):
#     """Download Binance Spot aggregate trades data."""
#     from src.batch.minio.download.binance.spot_aggtrades import main
#     main()


def download_binance_spot_klines(**kwargs):
    """Download Binance Spot klines data."""
    from src.batch.minio.download.binance.spot_klines import main
    main()


# def download_binance_futures_fundingrate(**kwargs):
#     """Download Binance Futures funding rate data."""
#     from src.batch.minio.download.binance.futures_fundingrate import main
#     main()


# def download_binance_futures_indexpriceklines(**kwargs):
#     """Download Binance Futures index price klines data."""
#     from src.batch.minio.download.binance.futures_indexpriceklines import main
#     main()


# def download_binance_futures_markpriceklines(**kwargs):
#     """Download Binance Futures mark price klines data."""
#     from src.batch.minio.download.binance.futures_markpriceklines import main
#     main()


def download_binance_futures_premiumindexklines(**kwargs):
    """Download Binance Futures premium index klines data."""
    from src.batch.minio.download.binance.futures_premiumindexklines import main
    main()


def download_binance_futures_metrics(**kwargs):
    """Download Binance Futures metrics data."""
    from src.batch.minio.download.binance.futures_metrics import main
    main()


# ========== REST API Task Functions ==========

def restapi_binance_futures_aggtrades(**kwargs):
    """REST API: Fill gaps in Futures aggregate trades."""
    from src.batch.minio.restapi.binance.futures_aggtrades import BinanceFuturesAggTrades
    backfill = BinanceFuturesAggTrades(symbol="BTCUSDT")
    backfill.run()


def restapi_binance_futures_klines(**kwargs):
    """REST API: Fill gaps in Futures klines."""
    from src.batch.minio.restapi.binance.futures_klines import BinanceFuturesKlines
    backfill = BinanceFuturesKlines(symbol="BTCUSDT", interval="1m")
    backfill.run()


# def restapi_binance_spot_aggtrades(**kwargs):
#     """REST API: Fill gaps in Spot aggregate trades."""
#     from src.batch.minio.restapi.binance.spot_aggtrades import BinanceSpotAggTrades
#     backfill = BinanceSpotAggTrades(symbol="BTCUSDT")
#     backfill.run()


def restapi_binance_spot_klines(**kwargs):
    """REST API: Fill gaps in Spot klines."""
    from src.batch.minio.restapi.binance.spot_klines import BinanceSpotKlines
    backfill = BinanceSpotKlines(symbol="BTCUSDT", interval="1m")
    backfill.run()


# def restapi_binance_futures_fundingrate(**kwargs):
#     """REST API: Fill gaps in Futures funding rate."""
#     from src.batch.minio.restapi.binance.futures_fundingrate import BinanceFuturesFundingRate
#     backfill = BinanceFuturesFundingRate(symbol="BTCUSDT")
#     backfill.run()


def restapi_binance_futures_metrics(**kwargs):
    """REST API: Fill gaps in Futures metrics."""
    from src.batch.minio.restapi.binance.futures_metrics import BinanceFuturesMetrics
    backfill = BinanceFuturesMetrics(symbol="BTCUSDT")
    backfill.run()


# def restapi_binance_futures_indexpriceklines(**kwargs):
#     """REST API: Fill gaps in Futures index price klines."""
#     from src.batch.minio.restapi.binance.futures_indexpriceklines import BinanceFuturesIndexPriceKlines
#     backfill = BinanceFuturesIndexPriceKlines(symbol="BTCUSDT", interval="1m")
#     backfill.run()


# def restapi_binance_futures_markpriceklines(**kwargs):
#     """REST API: Fill gaps in Futures mark price klines."""
#     from src.batch.minio.restapi.binance.futures_markpriceklines import BinanceFuturesMarkPriceKlines
#     backfill = BinanceFuturesMarkPriceKlines(symbol="BTCUSDT", interval="1m")
#     backfill.run()


def restapi_binance_futures_premiumindexklines(**kwargs):
    """REST API: Fill gaps in Futures premium index klines."""
    from src.batch.minio.restapi.binance.futures_premiumindexklines import BinanceFuturesPremiumIndexKlines
    backfill = BinanceFuturesPremiumIndexKlines(symbol="BTCUSDT", interval="1m")
    backfill.run()


# ========== DAG Configuration ==========

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
}


# ========== DAG Definition ==========

with DAG(
    dag_id='batch_minio',
    default_args=default_args,
    description='',
    schedule_interval=None,
    catchup=False,
) as dag:

    # ========== Download Tasks ==========
    
    download_futures_aggtrades = PythonOperator(
        task_id='download_futures_aggtrades',
        python_callable=download_binance_futures_aggtrades,
        provide_context=True,
    )

    download_futures_klines = PythonOperator(
        task_id='download_futures_klines',
        python_callable=download_binance_futures_klines,
        provide_context=True,
    )

    # download_futures_fundingrate = PythonOperator(
    #     task_id='download_futures_fundingrate',
    #     python_callable=download_binance_futures_fundingrate,
    #     provide_context=True,
    # )

    # download_futures_indexpriceklines = PythonOperator(
    #     task_id='download_futures_indexpriceklines',
    #     python_callable=download_binance_futures_indexpriceklines,
    #     provide_context=True,
    # )

    # download_futures_markpriceklines = PythonOperator(
    #     task_id='download_futures_markpriceklines',
    #     python_callable=download_binance_futures_markpriceklines,
    #     provide_context=True,
    # )

    download_futures_premiumindexklines = PythonOperator(
        task_id='download_futures_premiumindexklines',
        python_callable=download_binance_futures_premiumindexklines,
        provide_context=True,
    )

    download_futures_metrics = PythonOperator(
        task_id='download_futures_metrics',
        python_callable=download_binance_futures_metrics,
        provide_context=True,
    )

    download_spot_klines = PythonOperator(
        task_id='download_spot_klines',
        python_callable=download_binance_spot_klines,
        provide_context=True,
    )

    # download_spot_aggtrades = PythonOperator(
    #     task_id='download_spot_aggtrades',
    #     python_callable=download_binance_spot_aggtrades,
    #     provide_context=True,
    # )

    # ========== REST API Tasks ==========
    
    restapi_futures_aggtrades = PythonOperator(
        task_id='restapi_futures_aggtrades',
        python_callable=restapi_binance_futures_aggtrades,
        provide_context=True,
    )

    restapi_futures_klines = PythonOperator(
        task_id='restapi_futures_klines',
        python_callable=restapi_binance_futures_klines,
        provide_context=True,
    )

    # restapi_spot_aggtrades = PythonOperator(
    #     task_id='restapi_spot_aggtrades',
    #     python_callable=restapi_binance_spot_aggtrades,
    #     provide_context=True,
    # )

    # restapi_futures_fundingrate = PythonOperator(
    #     task_id='restapi_futures_fundingrate',
    #     python_callable=restapi_binance_futures_fundingrate,
    #     provide_context=True,
    # )

    restapi_futures_metrics = PythonOperator(
        task_id='restapi_futures_metrics',
        python_callable=restapi_binance_futures_metrics,
        provide_context=True,
    )

    restapi_spot_klines = PythonOperator(
        task_id='restapi_spot_klines',
        python_callable=restapi_binance_spot_klines,
        provide_context=True,
    )

    # restapi_futures_indexpriceklines = PythonOperator(
    #     task_id='restapi_futures_indexpriceklines',
    #     python_callable=restapi_binance_futures_indexpriceklines,
    #     provide_context=True,
    # )

    # restapi_futures_markpriceklines = PythonOperator(
    #     task_id='restapi_futures_markpriceklines',
    #     python_callable=restapi_binance_futures_markpriceklines,
    #     provide_context=True,
    # )

    restapi_futures_premiumindexklines = PythonOperator(
        task_id='restapi_futures_premiumindexklines',
        python_callable=restapi_binance_futures_premiumindexklines,
        provide_context=True,
    )

    # ========== Task Dependencies ==========
    # Download first, then fill gaps using REST API
    
    download_futures_aggtrades >> restapi_futures_aggtrades
    download_futures_klines >> restapi_futures_klines
    # download_spot_aggtrades >> restapi_spot_aggtrades
    download_spot_klines >> restapi_spot_klines
    # download_futures_fundingrate >> restapi_futures_fundingrate
    download_futures_metrics >> restapi_futures_metrics
    # download_futures_indexpriceklines >> restapi_futures_indexpriceklines
    # download_futures_markpriceklines >> restapi_futures_markpriceklines
    download_futures_premiumindexklines >> restapi_futures_premiumindexklines

