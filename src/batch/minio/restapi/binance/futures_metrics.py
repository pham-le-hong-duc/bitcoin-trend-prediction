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
    SOURCE_INTERVAL_MS = 5 * 60 * 1000
    MAX_BUCKET_RETRIES = 5
    RETRY_DELAY_SECONDS = 1
    
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

    def _iter_bucket_timestamps(self, start_date, end_date):
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)

        first_bucket_ms = (
            ((start_ms + self.SOURCE_INTERVAL_MS - 1) // self.SOURCE_INTERVAL_MS)
            * self.SOURCE_INTERVAL_MS
        )

        bucket_ms = first_bucket_ms
        while bucket_ms <= end_ms:
            yield bucket_ms
            bucket_ms += self.SOURCE_INTERVAL_MS

    @staticmethod
    def _extract_first_payload(payload_list):
        if not payload_list:
            return None

        for item in payload_list:
            if isinstance(item, dict):
                return item

        return None

    def _fetch_bucket_metrics(self, target_timestamp_ms):
        start_ms = target_timestamp_ms
        end_ms = target_timestamp_ms + self.SOURCE_INTERVAL_MS - 1

        open_interest_response = self.client.rest_api.open_interest_statistics(
            symbol=self.symbol,
            period=OpenInterestStatisticsPeriodEnum["PERIOD_5m"].value,
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        open_interest = open_interest_response.data() if open_interest_response else []
        time.sleep(0.2)

        top_accounts_response = self.client.rest_api.top_trader_long_short_ratio_accounts(
            symbol=self.symbol,
            period=TopTraderLongShortRatioAccountsPeriodEnum["PERIOD_5m"].value,
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        top_accounts_ratio = top_accounts_response.data() if top_accounts_response else []
        time.sleep(0.2)

        top_positions_response = self.client.rest_api.top_trader_long_short_ratio_positions(
            symbol=self.symbol,
            period=TopTraderLongShortRatioPositionsPeriodEnum["PERIOD_5m"].value,
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        top_positions_ratio = top_positions_response.data() if top_positions_response else []
        time.sleep(0.2)

        global_ratio_response = self.client.rest_api.long_short_ratio(
            symbol=self.symbol,
            period=LongShortRatioPeriodEnum["PERIOD_5m"].value,
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        global_ratio = global_ratio_response.data() if global_ratio_response else []
        time.sleep(0.2)

        taker_volume_response = self.client.rest_api.taker_buy_sell_volume(
            symbol=self.symbol,
            period=TakerBuySellVolumePeriodEnum["PERIOD_5m"].value,
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        taker_volume = taker_volume_response.data() if taker_volume_response else []

        return {
            "open_interest": self._extract_first_payload(open_interest),
            "top_accounts_ratio": self._extract_first_payload(top_accounts_ratio),
            "top_positions_ratio": self._extract_first_payload(top_positions_ratio),
            "global_ratio": self._extract_first_payload(global_ratio),
            "taker_volume": self._extract_first_payload(taker_volume),
        }

    def _poll_bucket_until_complete(self, target_timestamp_ms):
        for retry_count in range(self.MAX_BUCKET_RETRIES):
            try:
                combined_data = self._fetch_bucket_metrics(target_timestamp_ms)
                missing_payloads = [
                    payload_name
                    for payload_name, payload in combined_data.items()
                    if not isinstance(payload, dict)
                ]
                if not missing_payloads:
                    return combined_data

                if retry_count < self.MAX_BUCKET_RETRIES - 1:
                    print(
                        f"Bucket {target_timestamp_ms}: missing "
                        f"{', '.join(missing_payloads)} "
                        f"(attempt {retry_count + 1}/{self.MAX_BUCKET_RETRIES}), retrying..."
                    )
                    time.sleep(self.RETRY_DELAY_SECONDS)
            except Exception as e:
                error_msg = str(e)
                if '-1003' in error_msg or 'Too many requests' in error_msg:
                    if retry_count < self.MAX_BUCKET_RETRIES - 1:
                        print(
                            f"Bucket {target_timestamp_ms}: rate limit hit "
                            f"(attempt {retry_count + 1}/{self.MAX_BUCKET_RETRIES}), sleeping 60s..."
                        )
                        time.sleep(60)
                        continue

                if retry_count < self.MAX_BUCKET_RETRIES - 1:
                    print(
                        f"Bucket {target_timestamp_ms}: request failed "
                        f"(attempt {retry_count + 1}/{self.MAX_BUCKET_RETRIES}): {e}"
                    )
                    time.sleep(self.RETRY_DELAY_SECONDS * (2 ** retry_count))
                else:
                    print(f"Bucket {target_timestamp_ms}: max retries reached, skipping.")
                    return None

        return None

    def get_api_data(self, start_date, end_date):
        """Fetch metrics bucket-by-bucket, retrying until each bucket is complete."""
        for bucket_timestamp_ms in self._iter_bucket_timestamps(start_date, end_date):
            combined_data = self._poll_bucket_until_complete(bucket_timestamp_ms)
            if combined_data:
                yield combined_data
    
    def transform_data(self, api_response):
        """Transform one complete 5m metrics bucket to DataFrame."""
        if not api_response:
            return pl.DataFrame()
        
        api1 = api_response.get('open_interest')
        api2 = api_response.get('top_accounts_ratio')
        api3 = api_response.get('top_positions_ratio')
        api4 = api_response.get('global_ratio')
        api5 = api_response.get('taker_volume')

        if not all([api1, api2, api3, api4, api5]):
            return pl.DataFrame()

        timestamp = api1.get("timestamp")
        if timestamp is None:
            return pl.DataFrame()

        required_checks = [
            (api1, ['sumOpenInterest', 'sumOpenInterestValue'], 'open_interest'),
            (api2, ['longShortRatio'], 'top_accounts_ratio'),
            (api3, ['longShortRatio'], 'top_positions_ratio'),
            (api4, ['longShortRatio'], 'global_ratio'),
            (api5, ['buySellRatio'], 'taker_volume'),
        ]
        for payload, required_fields, payload_name in required_checks:
            missing_fields = [field for field in required_fields if field not in payload]
            if missing_fields:
                print(f"Skipping timestamp {timestamp}: {payload_name} missing {missing_fields}")
                return pl.DataFrame()

        transformed_records = [
            {
                "create_time": int(timestamp),
                "symbol": self.symbol,
                "sum_open_interest": float(api1['sumOpenInterest']),
                "sum_open_interest_value": float(api1['sumOpenInterestValue']),
                "count_toptrader_long_short_ratio": float(api2['longShortRatio']),
                "sum_toptrader_long_short_ratio": float(api3['longShortRatio']),
                "count_long_short_ratio": float(api4['longShortRatio']),
                "sum_taker_long_short_vol_ratio": float(api5['buySellRatio']),
            }
        ]

        if not transformed_records:
            return pl.DataFrame()

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


