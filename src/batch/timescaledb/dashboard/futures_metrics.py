from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from src.batch.timescaledb.base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesMetricsBatch(HistoricalTimescaleBatch):
    METRICS_COLUMNS = [
        "create_time",
        "symbol",
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    VALUE_COLUMNS = [
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]

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
            base_start_date=datetime(2021, 12, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_metrics_{interval}"

    def _normalize_historical_df(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        if "create_time" not in df.columns:
            if df.width != len(self.METRICS_COLUMNS):
                raise ValueError(
                    "Unexpected historical metrics schema: "
                    f"expected {len(self.METRICS_COLUMNS)} columns, got {df.width} "
                    f"({df.columns})"
                )

            df = df.rename(
                {
                    current: expected
                    for current, expected in zip(df.columns, self.METRICS_COLUMNS)
                }
            )

        create_time_dtype = df.schema.get("create_time")
        if create_time_dtype == pl.Utf8:
            df = df.with_columns(
                pl.col("create_time")
                .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=True)
                .dt.replace_time_zone("UTC")
                .dt.epoch("ms")
                .alias("create_time")
            )
        else:
            df = df.with_columns(pl.col("create_time").cast(pl.Int64))

        value_expressions = []
        for column in self.VALUE_COLUMNS:
            value_expressions.append(
                pl.when(pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(column))
                .cast(pl.Float64, strict=False)
                .alias(column)
            )

        df = df.with_columns(value_expressions)

        return df

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return self._normalize_historical_df(df)

    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: list[int],
        historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(historical_frames["futures_metrics"])
        if df.is_empty():
            return None
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

            latest_values = {}
            has_any_value = False
            for column in self.VALUE_COLUMNS:
                non_null_series = (
                    window_df
                    .filter(pl.col(column).is_not_null())
                    .select(column)
                    .to_series()
                )
                if non_null_series.is_empty():
                    latest_values[column] = None
                    continue
                latest_values[column] = float(non_null_series[-1])
                has_any_value = True

            if not has_any_value:
                continue

            rows.append(
                {
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    **latest_values,
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
