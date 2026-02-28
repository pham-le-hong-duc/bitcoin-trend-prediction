from .base import Download


def main():
    """Download Binance Futures aggregate trades data."""
    downloader = Download(
        data_type="futures/um/daily/aggTrades/BTCUSDT",
        url_template="https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{YYYY_MM_DD}.zip",
        frequency="daily",
        base_start_date="2019-12-31",
        column_names=[
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker"
        ]
    )
    downloader.run()


if __name__ == "__main__":
    main()
