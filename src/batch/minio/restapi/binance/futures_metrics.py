"""
Binance Futures Metrics REST API backfill.
Fills gaps in MinIO data by fetching from Binance REST API.
API Limit: 30 days of historical data.
"""

import polars as pl
import time
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    OpenInterestStatisticsPeriodEnum,
    TopTraderLongShortRatioAccountsPeriodEnum,
    TopTraderLongShortRatioPositionsPeriodEnum,
    LongShortRatioPeriodEnum,
    TakerBuySellVolumePeriodEnum,
)

from .base import RestAPI


class BinanceFuturesMetrics(RestAPI):
    """Backfill Binance Futures metrics data using REST API."""
    
    def __init__(self, symbol="BTCUSDT", data_type="futures/um/daily/metrics/BTCUSDT"):
        """
        Initialize Binance Futures Metrics backfill.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            data_type: MinIO path prefix
        """
        super().__init__(
            symbol=symbol,
            data_type=data_type,
            client_type="futures",
            file_pattern="daily",
            timestamp_field="create_time",
            unique_field="create_time",
            api_limit_days=7,
            gap_threshold_ms=301000
        )    
    def get_api_data(self, start_date, end_date):
        """Fetch combined metrics from multiple Binance APIs with retry mechanism."""
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        
        max_retries = 5
        retry_delay = 1  # Start with 1 second
        
        for retry_count in range(max_retries):
            try:
                open_interest_response = self.client.rest_api.open_interest_statistics(
                    symbol=self.symbol,
                    period=OpenInterestStatisticsPeriodEnum["PERIOD_5m"].value,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500
                )
                open_interest = open_interest_response.data() if open_interest_response else []
                time.sleep(0.2)
                
                top_accounts_response = self.client.rest_api.top_trader_long_short_ratio_accounts(
                    symbol=self.symbol,
                    period=TopTraderLongShortRatioAccountsPeriodEnum["PERIOD_5m"].value,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500
                )
                top_accounts_ratio = top_accounts_response.data() if top_accounts_response else []
                time.sleep(0.2)
                
                # Fetch Top Trader Long/Short Ratio (Positions)
                top_positions_response = self.client.rest_api.top_trader_long_short_ratio_positions(
                    symbol=self.symbol,
                    period=TopTraderLongShortRatioPositionsPeriodEnum["PERIOD_5m"].value,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500
                )
                top_positions_ratio = top_positions_response.data() if top_positions_response else []
                time.sleep(0.2)
                
                # Fetch Global Long/Short Ratio
                global_ratio_response = self.client.rest_api.long_short_ratio(
                    symbol=self.symbol,
                    period=LongShortRatioPeriodEnum["PERIOD_5m"].value,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500
                )
                global_ratio = global_ratio_response.data() if global_ratio_response else []
                time.sleep(0.2)
                
                taker_volume_response = self.client.rest_api.taker_buy_sell_volume(
                    symbol=self.symbol,
                    period=TakerBuySellVolumePeriodEnum["PERIOD_5m"].value,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500
                )
                taker_volume = taker_volume_response.data() if taker_volume_response else []
                
                combined_data = {
                    'open_interest': open_interest,
                    'top_accounts_ratio': top_accounts_ratio,
                    'top_positions_ratio': top_positions_ratio,
                    'global_ratio': global_ratio,
                    'taker_volume': taker_volume
                }
                
                if any(combined_data.values()):
                    yield combined_data
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
                    print(f"Max retries reached. Skipping this gap.")
                    return
    
    def transform_data(self, api_response):
        """Transform combined metrics API response to DataFrame."""
        if not api_response:
            return pl.DataFrame()
        
        api1_list = api_response.get('open_interest', [])
        api2_list = api_response.get('top_accounts_ratio', [])
        api3_list = api_response.get('top_positions_ratio', [])
        api4_list = api_response.get('global_ratio', [])
        api5_list = api_response.get('taker_volume', [])
        
        # Convert to dicts indexed by timestamp for merging
        api1_dict = {item.get('timestamp'): item for item in api1_list} if api1_list else {}
        api2_dict = {item.get('timestamp'): item for item in api2_list} if api2_list else {}
        api3_dict = {item.get('timestamp'): item for item in api3_list} if api3_list else {}
        api4_dict = {item.get('timestamp'): item for item in api4_list} if api4_list else {}
        api5_dict = {item.get('timestamp'): item for item in api5_list} if api5_list else {}
        
        # Get all unique timestamps
        all_timestamps = set(api1_dict.keys()) | set(api2_dict.keys()) | set(api3_dict.keys()) | set(api4_dict.keys()) | set(api5_dict.keys())
        
        transformed_records = []
        for timestamp in sorted(all_timestamps):
            if timestamp is None:
                continue
                
            api1 = api1_dict.get(timestamp, {})
            api2 = api2_dict.get(timestamp, {})
            api3 = api3_dict.get(timestamp, {})
            api4 = api4_dict.get(timestamp, {})
            api5 = api5_dict.get(timestamp, {})
            
            record = {
                "create_time": int(timestamp) if timestamp else None,
                "symbol": self.symbol,
                "sum_open_interest": float(api1.get('sumOpenInterest', 0)),
                "sum_open_interest_value": float(api1.get('sumOpenInterestValue', 0)),
                "count_toptrader_long_short_ratio": float(api2.get('longShortRatio', 0)),
                "sum_toptrader_long_short_ratio": float(api3.get('longShortRatio', 0)),
                "count_long_short_ratio": float(api4.get('longShortRatio', 0)),
                "sum_taker_long_short_vol_ratio": float(api5.get('buySellRatio', 0)),
            }
            transformed_records.append(record)
        
        df = pl.DataFrame(transformed_records)
        df = df.with_columns(
            pl.from_epoch(pl.col('create_time'), time_unit='ms')
              .dt.strftime('%Y-%m-%d %H:%M:%S')
              .alias('create_time')
        )
        
        return df


def main():
    """Run Binance Futures Metrics backfill."""
    backfill = BinanceFuturesMetrics(symbol="BTCUSDT")
    backfill.run()


if __name__ == "__main__":
    main()


