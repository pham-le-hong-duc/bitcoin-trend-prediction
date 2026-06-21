"""
Binance Futures Klines REST API backfill.
Fills gaps in MinIO data by fetching from Binance REST API.
API Limit: Full history available.
"""

import time
import polars as pl
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    KlineCandlestickDataIntervalEnum,
)

from .base import RestAPI


class BinanceFuturesKlines(RestAPI):
    """Backfill Binance Futures klines data using REST API."""

    def __init__(self, symbol="BTCUSDT", interval="1m", data_type="futures/um/daily/klines/BTCUSDT/1m"):
        super().__init__(
            symbol=symbol,
            data_type=data_type,
            client_type="futures",
            file_pattern="daily",
            timestamp_field="open_time",
            unique_field="open_time",
            api_limit_days=7,
            gap_threshold_ms=61000,
        )
        self.interval = interval

    def get_api_data(self, start_date, end_date):
        """Fetch futures klines from Binance API with pagination."""
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        current_start = start_ms

        interval_enum = KlineCandlestickDataIntervalEnum[f"INTERVAL_{self.interval}"].value

        while current_start < end_ms:
            max_retries = 5
            retry_delay = 1

            for retry_count in range(max_retries):
                try:
                    response = self.client.rest_api.kline_candlestick_data(
                        symbol=self.symbol,
                        interval=interval_enum,
                        start_time=current_start,
                        end_time=end_ms,
                        limit=1500,
                    )

                    if not response:
                        return

                    data = response.data() if response else []
                    if not data:
                        return

                    yield data

                    if len(data) < 1500:
                        return

                    last_time = data[-1][6]
                    if last_time >= end_ms:
                        return

                    current_start = last_time + 1
                    time.sleep(0.2)
                    break

                except Exception as e:
                    error_msg = str(e)
                    if '-1003' in error_msg or 'Too many requests' in error_msg:
                        if retry_count < max_retries - 1:
                            print(f"Rate limit hit (attempt {retry_count + 1}/{max_retries}). Sleeping 60s...")
                            time.sleep(60)
                            continue

                    if retry_count < max_retries - 1:
                        print(f"Request failed (attempt {retry_count + 1}/{max_retries}): {e}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print("Max retries reached. Skipping...")
                        current_start += 60000
                        break

    def transform_data(self, api_response):
        """Transform API response to DataFrame."""
        if not api_response:
            return pl.DataFrame()

        transformed_records = []
        for kline_data in api_response:
            record = {
                "open_time": int(kline_data[0]),
                "open": float(kline_data[1]),
                "high": float(kline_data[2]),
                "low": float(kline_data[3]),
                "close": float(kline_data[4]),
                "volume": float(kline_data[5]) if kline_data[5] else 0.0,
                "close_time": int(kline_data[6]),
                "quote_volume": float(kline_data[7]) if kline_data[7] else 0.0,
                "count": int(kline_data[8]) if kline_data[8] else 0,
                "taker_buy_volume": float(kline_data[9]) if kline_data[9] else 0.0,
                "taker_buy_quote_volume": float(kline_data[10]) if kline_data[10] else 0.0,
                "ignore": int(kline_data[11]) if len(kline_data) > 11 else 0,
            }
            transformed_records.append(record)

        return pl.DataFrame(transformed_records)


def main():
    """Run Binance Futures Klines backfill."""
    backfill = BinanceFuturesKlines(symbol="BTCUSDT")
    backfill.run()


if __name__ == "__main__":
    main()
