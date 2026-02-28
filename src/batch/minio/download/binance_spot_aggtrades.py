from .base import Download


def main():
    """Download Binance Spot aggregate trades data."""
    downloader = Download(
        data_type="spot/daily/aggTrades/BTCUSDT",
        url_template="https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{YYYY_MM_DD}.zip",
        frequency="daily",
        base_start_date="2017-08-17",
        column_names=[
            "aggregate_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "timestamp",
            "was_buyer_maker",
            "was_best_price_match"
        ],
        has_header=False  # Spot aggTrades CSV has no header
    )
    downloader.run()


if __name__ == "__main__":
    main()
