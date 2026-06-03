from .base import Download


def main():
    """Download Binance Futures mark price klines data."""
    downloader = Download(
        data_type="futures/um/daily/markPriceKlines/BTCUSDT/1m",
        url_template="https://data.binance.vision/data/futures/um/daily/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-{YYYY_MM_DD}.zip",
        frequency="daily",
        base_start_date="2019-12-23",
        column_names=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "ignore",
            "close_time",
            "number_of_trades"
        ]
    )
    downloader.run()


if __name__ == "__main__":
    main()
