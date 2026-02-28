"""
Binance Futures Aggregate Trades REST API backfill.
Fills gaps in MinIO data by fetching from Binance REST API.
API Limit: 1 year of historical data.
"""

import polars as pl
import time

from .base import RestAPI


class BinanceFuturesAggTrades(RestAPI):
    """Backfill Binance Futures aggregate trades data using REST API."""
    
    def __init__(self, symbol="BTCUSDT", data_type="futures/um/daily/aggTrades/BTCUSDT"):
        """
        Initialize Binance Futures AggTrades backfill.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            data_type: MinIO path prefix
        """
        super().__init__(
            symbol=symbol,
            data_type=data_type,
            client_type="futures",
            file_pattern="daily",
            timestamp_field="transact_time",
            unique_field="agg_trade_id",
            api_limit_days=7,
            gap_threshold_ms=15000
        )
    
    def get_api_data(self, start_date, end_date):
        """Fetch aggregate trades using time-based pagination with retry mechanism."""
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        current_start = start_ms
        
        while current_start < end_ms:
            max_retries = 5
            retry_delay = 1
            
            for retry_count in range(max_retries):
                try:
                    response = self.client.rest_api.compressed_aggregate_trades_list(
                        symbol=self.symbol,
                        start_time=current_start,
                        end_time=end_ms,
                        limit=1000
                    )
                    
                    if not response:
                        return
                    
                    trades = response.data() if hasattr(response, 'data') and callable(response.data) else []
                    if not trades:
                        return
                    
                    yield trades
                    
                    if len(trades) < 1000:
                        return
                    
                    last_time = trades[-1].T
                    if last_time >= end_ms:
                        return
                    
                    current_start = last_time + 1
                    time.sleep(0.2)  # Rate limiting: 10 req/s
                    break
                    
                except Exception as e:
                    if retry_count < max_retries - 1:
                        print(f"Request failed (attempt {retry_count + 1}/{max_retries}): {e}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"Max retries reached for time window {current_start}. Skipping...")
                        current_start += 60000
                        break
    
    def transform_data(self, api_response):
        """Transform API response to DataFrame."""
        if not api_response:
            return pl.DataFrame()
        
        transformed_records = []
        for item in api_response:
            record = {
                "agg_trade_id": item.a,
                "price": float(item.p),
                "quantity": float(item.q),
                "first_trade_id": item.f,
                "last_trade_id": item.l,
                "transact_time": item.T,  # Keep as integer (timestamp)
                "is_buyer_maker": item.m
            }
            transformed_records.append(record)
        
        return pl.DataFrame(transformed_records)
    
def main():
    """Run Binance Futures AggTrades backfill."""
    backfill = BinanceFuturesAggTrades(symbol="BTCUSDT")
    backfill.run()


if __name__ == "__main__":
    main()

