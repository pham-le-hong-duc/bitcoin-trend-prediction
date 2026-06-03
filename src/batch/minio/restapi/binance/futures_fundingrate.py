"""
Binance Futures Funding Rate REST API backfill.
Fills gaps in MinIO data by fetching from Binance REST API.
API Limit: Full history available.
"""

import polars as pl
import time

from .base import RestAPI


class BinanceFuturesFundingRate(RestAPI):
    """Backfill Binance Futures funding rate data using REST API."""
    
    def __init__(self, symbol="BTCUSDT", data_type="futures/um/monthly/fundingRate/BTCUSDT"):
        """
        Initialize Binance Futures Funding Rate backfill.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            data_type: MinIO path prefix
        """
        super().__init__(
            symbol=symbol,
            data_type=data_type,
            client_type="futures",
            file_pattern="monthly",
            timestamp_field="calc_time",
            unique_field="calc_time",
            api_limit_days=7,
            gap_threshold_ms=28801000
        )
    
    def get_api_data(self, start_date, end_date):
        """Fetch funding rate from Binance API with pagination."""
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        current_start = start_ms
        
        while current_start < end_ms:
            max_retries = 5
            retry_delay = 1
            
            for retry_count in range(max_retries):
                try:
                    response = self.client.rest_api.get_funding_rate_history(
                        symbol=self.symbol,
                        start_time=current_start,
                        end_time=end_ms,
                        limit=1000
                    )
                    
                    if not response:
                        return
                    
                    data = response.data() if response else []
                    if not data:
                        return
                    
                    yield data
                    
                    if len(data) < 1000:
                        return
                    
                    last_time = data[-1].funding_time
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
                        print(f"Max retries reached. Skipping...")
                        current_start += 60000
                        break
    
    def transform_data(self, api_response):
        """Transform API response to DataFrame."""
        if not api_response:
            return pl.DataFrame()
        
        transformed_records = []
        for funding_data in api_response:
            record = {
                "calc_time": int(funding_data.funding_time),  # Keep as integer (timestamp)
                "funding_interval_hours": 8,
                "last_funding_rate": float(funding_data.funding_rate)
            }
            transformed_records.append(record)
        
        return pl.DataFrame(transformed_records)
    
def main():
    """Run Binance Futures Funding Rate backfill."""
    backfill = BinanceFuturesFundingRate(symbol="BTCUSDT")
    backfill.run()


if __name__ == "__main__":
    main()


