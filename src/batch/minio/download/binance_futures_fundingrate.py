from .base import Download


def main():
    """Download Binance Futures funding rate data."""
    downloader = Download(
        data_type="futures/um/monthly/fundingRate/BTCUSDT",
        url_template="https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{YYYY_MM}.zip",
        frequency="monthly",
        base_start_date="2020-01-01",
        column_names=[
            "calc_time",
            "funding_interval_hours",
            "last_funding_rate"
        ]
    )
    downloader.run()


if __name__ == "__main__":
    main()
