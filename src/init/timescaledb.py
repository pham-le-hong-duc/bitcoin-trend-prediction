import sys

from src.utils.timescaledb_client import TimescaleDBClient, wait_for_timescaledb


SCHEMAS = ("dashboard", "featurestore")
KLINE_TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")
METRICS_TIMEFRAMES = ("5m", "15m", "1h", "4h", "1d")
SENTIMENT_TIMEFRAMES = ("1h", "4h", "1d")


def initialize_schemas(client: TimescaleDBClient) -> None:
    for schema_name in SCHEMAS:
        if not client.create_schema(schema_name):
            raise RuntimeError(f"Failed to create schema '{schema_name}'")


def initialize_dashboard_tables(client: TimescaleDBClient) -> None:
    create_futures_index_price_kline_tables(client)
    create_futures_metrics_tables(client)
    create_sentiment_tables(client)


def create_futures_index_price_kline_tables(client: TimescaleDBClient) -> None:
    columns_sql = """
        open_time TIMESTAMP PRIMARY KEY,
        close_time TIMESTAMP,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION
    """
    for timeframe in KLINE_TIMEFRAMES:
        client.ensure_table(
            schema_name="dashboard",
            table_name=f"futures_index_price_klines_{timeframe}",
            columns_sql=columns_sql,
            hypertable_time_column="open_time",
        )
        client.execute(
            f"""
            ALTER TABLE dashboard.futures_index_price_klines_{timeframe}
            ADD COLUMN IF NOT EXISTS open_time TIMESTAMP
            """
        )


def create_futures_metrics_tables(client: TimescaleDBClient) -> None:
    columns_sql = """
        create_time TIMESTAMP PRIMARY KEY,
        sum_open_interest DOUBLE PRECISION,
        sum_open_interest_value DOUBLE PRECISION,
        count_toptrader_long_short_ratio DOUBLE PRECISION,
        sum_toptrader_long_short_ratio DOUBLE PRECISION,
        count_long_short_ratio DOUBLE PRECISION,
        sum_taker_long_short_vol_ratio DOUBLE PRECISION
    """
    for timeframe in METRICS_TIMEFRAMES:
        client.ensure_table(
            schema_name="dashboard",
            table_name=f"futures_metrics_{timeframe}",
            columns_sql=columns_sql,
            hypertable_time_column="create_time",
        )


def create_sentiment_tables(client: TimescaleDBClient) -> None:
    columns_sql = """
        create_time TIMESTAMP PRIMARY KEY,
        word_frequency JSONB,
        count BIGINT,
        score DOUBLE PRECISION,
        confidence DOUBLE PRECISION,
        pct_negative DOUBLE PRECISION,
        pct_positive DOUBLE PRECISION,
        pct_neutral DOUBLE PRECISION
    """
    for timeframe in SENTIMENT_TIMEFRAMES:
        client.ensure_table(
            schema_name="dashboard",
            table_name=f"sentiment_{timeframe}",
            columns_sql=columns_sql,
            hypertable_time_column="create_time",
        )


def main() -> None:
    print("TIMESCALEDB INITIALIZATION")
    client = None

    try:
        client = wait_for_timescaledb()
        initialize_schemas(client)
        initialize_dashboard_tables(client)
        print("TIMESCALEDB INITIALIZATION COMPLETED!")
    except Exception as exc:
        print(f"TimescaleDB initialization failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
