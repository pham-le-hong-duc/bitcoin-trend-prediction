from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from src.batch.timescaledb.base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesMetricsBatch(HistoricalTimescaleBatch):
    def __init__(self) -> None:
        super().__init__(
            schema_name="dashboard",
            time_column="create_time",
            intervals=["5m", "15m", "1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="futures_metrics",
                    prefix="futures/um/daily/metrics/BTCUSDT",
                )
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_metrics_{interval}"

    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: list[int],
        historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = historical_frames["futures_metrics"]
        if df.is_empty():
            return None

        df = df.with_columns(pl.col("create_time").cast(pl.Int64))
        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = df.filter(
                (pl.col("create_time") > window_start)
                & (pl.col("create_time") <= boundary_ts_ms)
            ).sort("create_time")

            if window_df.is_empty():
                continue

            latest = window_df.tail(1)
            rows.append(
                {
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    "sum_open_interest": float(latest["sum_open_interest"][0]),
                    "sum_open_interest_value": float(latest["sum_open_interest_value"][0]),
                    "count_toptrader_long_short_ratio": float(latest["count_toptrader_long_short_ratio"][0]),
                    "sum_toptrader_long_short_ratio": float(latest["sum_toptrader_long_short_ratio"][0]),
                    "count_long_short_ratio": float(latest["count_long_short_ratio"][0]),
                    "sum_taker_long_short_vol_ratio": float(latest["sum_taker_long_short_vol_ratio"][0]),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = FuturesMetricsBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
