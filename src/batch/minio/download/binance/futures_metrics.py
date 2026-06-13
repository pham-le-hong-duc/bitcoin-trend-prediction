from .base import Download


def main():
    """Download Binance Futures metrics data."""
    downloader = Download(
        data_type="futures/um/daily/metrics/BTCUSDT",
        url_template="https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-{YYYY_MM_DD}.zip",
        frequency="daily",
        base_start_date="2026-01-01",
        column_names=[
            "create_time",
            "symbol",
            "sum_open_interest",
            "sum_open_interest_value",
            "count_toptrader_long_short_ratio",
            "sum_toptrader_long_short_ratio",
            "count_long_short_ratio",
            "sum_taker_long_short_vol_ratio"
        ]
    )
    downloader.run()


if __name__ == "__main__":
    main()
