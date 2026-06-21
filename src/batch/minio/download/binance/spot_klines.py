from .base import Download


def main():
    """Download Binance Spot klines data."""
    downloader = Download(
        data_type="spot/daily/klines/BTCUSDT/1m",
        url_template="https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-{YYYY_MM_DD}.zip",
        frequency="daily",
        base_start_date="2017-08-17",
        column_names=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
        has_header=False,
    )
    downloader.run()


if __name__ == "__main__":
    main()
