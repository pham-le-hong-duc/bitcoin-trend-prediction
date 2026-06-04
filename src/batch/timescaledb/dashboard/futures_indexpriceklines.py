from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from src.batch.timescaledb.base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesIndexPriceKlinesBatch(HistoricalTimescaleBatch):
    KLINE_COLUMNS = [
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
    ]

    def __init__(self) -> None:
        super().__init__(
            schema_name="dashboard",
            time_column="close_time",
            intervals=["1m", "5m", "15m", "1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="index_price_klines",
                    prefix="futures/um/daily/indexPriceKlines/BTCUSDT/1m",
                )
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_index_price_klines_{interval}"

    def _normalize_historical_df(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        if "open_time" in df.columns and "close_time" in df.columns:
            return df

        if df.width != len(self.KLINE_COLUMNS):
            raise ValueError(
                "Unexpected historical kline schema: "
                f"expected {len(self.KLINE_COLUMNS)} columns, got {df.width} "
                f"({df.columns})"
            )

        return df.rename(
            {current: expected for current, expected in zip(df.columns, self.KLINE_COLUMNS)}
        )

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return self._normalize_historical_df(df)

    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: list[int],
        historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(historical_frames["index_price_klines"])
        if df.is_empty():
            return None

        df = df.with_columns(
            [
                pl.col("open_time").cast(pl.Int64),
                pl.col("close_time").cast(pl.Int64),
                (pl.col("close_time").cast(pl.Int64) + 1).alias("effective_close_time"),
            ]
        )
        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = df.filter(
                (pl.col("effective_close_time") > window_start)
                & (pl.col("effective_close_time") <= boundary_ts_ms)
            ).sort("open_time")

            if window_df.is_empty():
                continue

            rows.append(
                {
                    "close_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    "open": float(window_df["open"][0]),
                    "high": float(window_df["high"].max()),
                    "low": float(window_df["low"].min()),
                    "close": float(window_df["close"][-1]),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = FuturesIndexPriceKlinesBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
