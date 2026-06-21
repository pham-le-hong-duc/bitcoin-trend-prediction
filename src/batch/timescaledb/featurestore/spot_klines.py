from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class SpotKlinesBatch(HistoricalTimescaleBatch):
    DUPLICATED_SUFFIX_PATTERN = re.compile(r"_duplicated_\d+$")
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
    FLOAT_COLUMNS = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]
    SOURCE_INTERVAL_MS = INTERVAL_TO_MS["1m"]
    MICROSECOND_THRESHOLD = 10_000_000_000_000
    LOGRETURN_COLUMNS = [
        "trade_quantity",
        "trade_turnover",
        "trade_count",
        "buy_quantity",
        "buy_turnover",
    ]
    ROLLING_WINDOWS = [4, 8, 16, 32]
    TEMPORAL_FEATURE_SOURCE_COLUMNS = [
        "imbalance_buy_quantity",
        "imbalance_buy_turnover",
        "log_return_trade_quantity",
        "log_return_trade_turnover",
        "log_return_trade_count",
        "log_return_buy_quantity",
        "log_return_buy_turnover",
    ]
    LAG_WINDOWS = [1, 2, 4, 8, 16, 32]

    def __init__(self) -> None:
        super().__init__(
            schema_name="featurestore",
            time_column="open_time",
            intervals=["1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="spot_klines",
                    prefix="spot/daily/klines/BTCUSDT/1m",
                )
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"spot_klines_{interval}"

    def feature_steps(self) -> list[tuple[str, HistoricalTimescaleBatch.FeatureStep]]:
        return [
            ("derivative", self.derivative),
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def _align_boundary(self, ts_ms: int, interval_ms: int) -> int:
        return (ts_ms // interval_ms) * interval_ms

    def _expected_timestamps(self, interval: str) -> set[int]:
        interval_ms = INTERVAL_TO_MS[interval]
        start_ms = self._align_boundary(
            int(self.base_start_date.timestamp() * 1000),
            interval_ms,
        )
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_bucket_open_ms = (now_ms // interval_ms) * interval_ms
        last_closed_bucket_open_ms = current_bucket_open_ms - interval_ms

        if last_closed_bucket_open_ms < start_ms:
            return set()

        return set(range(start_ms, last_closed_bucket_open_ms + interval_ms, interval_ms))

    def _clean_header_value(self, value: str) -> str:
        return self.DUPLICATED_SUFFIX_PATTERN.sub("", value)

    @classmethod
    def _normalize_timestamp_unit_expr(cls, column: str) -> pl.Expr:
        return (
            pl.when(pl.col(column).abs() >= cls.MICROSECOND_THRESHOLD)
            .then((pl.col(column) // 1000).cast(pl.Int64))
            .otherwise(pl.col(column))
            .alias(column)
        )

    def _recover_headerless_df(self, df: pl.DataFrame) -> pl.DataFrame:
        recovered_first_row = {
            expected: self._clean_header_value(current)
            for current, expected in zip(df.columns, self.KLINE_COLUMNS)
        }
        renamed_df = df.rename(
            {current: expected for current, expected in zip(df.columns, self.KLINE_COLUMNS)}
        )
        recovered_df = pl.DataFrame([recovered_first_row])
        return pl.concat([recovered_df, renamed_df], how="vertical_relaxed")

    def _normalize_historical_df(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        if "open_time" not in df.columns or "close_time" not in df.columns:
            if df.width != len(self.KLINE_COLUMNS):
                raise ValueError(
                    "Unexpected historical kline schema: "
                    f"expected {len(self.KLINE_COLUMNS)} columns, got {df.width} "
                    f"({df.columns})"
                )
            df = self._recover_headerless_df(df)

        return (
            df.with_columns(
                [
                    pl.col("open_time").cast(pl.Int64, strict=False),
                    pl.col("close_time").cast(pl.Int64, strict=False),
                    pl.col("count").cast(pl.Int64, strict=False),
                    *[pl.col(column).cast(pl.Float64, strict=False) for column in self.FLOAT_COLUMNS],
                ]
            )
            .with_columns(
                [
                    self._normalize_timestamp_unit_expr("open_time"),
                    self._normalize_timestamp_unit_expr("close_time"),
                ]
            )
            .filter(
                pl.col("open_time").is_not_null()
                & pl.col("close_time").is_not_null()
            )
        )

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return self._normalize_historical_df(df)

    @staticmethod
    def _safe_div_expr(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
        return (
            pl.when(denominator.is_null() | denominator.eq(0))
            .then(0.0)
            .otherwise(numerator / denominator)
        )

    @staticmethod
    def _safe_log_return_expr(current: pl.Expr, previous: pl.Expr) -> pl.Expr:
        return (
            pl.when(
                current.is_null()
                | previous.is_null()
                | current.le(0)
                | previous.le(0)
            )
            .then(None)
            .otherwise((current / previous).log())
        )

    @staticmethod
    def _safe_zscore_expr(
        value: pl.Expr,
        mean: pl.Expr,
        std: pl.Expr,
    ) -> pl.Expr:
        return (
            pl.when(value.is_null() | mean.is_null() | std.is_null() | std.eq(0))
            .then(None)
            .otherwise((value - mean) / std)
        )

    @staticmethod
    def _combine_history_with_batch(
        batch_df: pl.DataFrame,
        timescaledb_historical_df: pl.DataFrame | None,
        base_columns: list[str],
    ) -> pl.DataFrame:
        if timescaledb_historical_df is None or timescaledb_historical_df.is_empty():
            history_df = batch_df.select(base_columns).head(0)
        else:
            history_df = timescaledb_historical_df.select(base_columns)

        return (
            pl.concat(
                [
                    history_df,
                    batch_df.select(base_columns),
                ],
                how="vertical_relaxed",
            )
            .sort("open_time")
            .unique(subset=["open_time"], keep="last", maintain_order=True)
        )

    def combine_history(
        self,
        aggregated_df: pl.DataFrame,
        timescaledb_historical_df: pl.DataFrame | None,
    ) -> pl.DataFrame:
        return self._combine_history_with_batch(
            aggregated_df,
            timescaledb_historical_df,
            aggregated_df.columns,
        )

    def derivative(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                self._safe_div_expr(
                    pl.col("buy_quantity") - (pl.col("trade_quantity") - pl.col("buy_quantity")),
                    pl.col("trade_quantity"),
                ).alias("imbalance_buy_quantity"),
                self._safe_div_expr(
                    pl.col("buy_turnover") - (pl.col("trade_turnover") - pl.col("buy_turnover")),
                    pl.col("trade_turnover"),
                ).alias("imbalance_buy_turnover"),
            ]
        )

    def log_return(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        previous_columns = [f"previous_{column}" for column in self.LOGRETURN_COLUMNS]

        return combined_df.with_columns(
            [
                *[
                    pl.col(column).shift(1).alias(f"previous_{column}")
                    for column in self.LOGRETURN_COLUMNS
                ],
            ]
        ).with_columns(
            [
                *[
                    self._safe_log_return_expr(
                        pl.col(column),
                        pl.col(f"previous_{column}"),
                    ).alias(f"log_return_{column}")
                    for column in self.LOGRETURN_COLUMNS
                ],
            ]
        ).drop(previous_columns)

    def rolling(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        rolling_exprs: list[pl.Expr] = []
        temp_columns: list[str] = []

        for window in self.ROLLING_WINDOWS:
            for column in [
                "imbalance_buy_quantity",
                "imbalance_buy_turnover",
            ]:
                mean_col = f"_{column}_mean_{window}"
                std_col = f"{column}_rolling_std_{window}"
                temp_columns.append(mean_col)
                rolling_exprs.extend(
                    [
                        pl.col(column).rolling_mean(window_size=window).alias(mean_col),
                        pl.col(column).rolling_std(window_size=window).alias(std_col),
                    ]
                )

            for column in [
                "log_return_trade_quantity",
                "log_return_trade_turnover",
                "log_return_trade_count",
                "log_return_buy_quantity",
                "log_return_buy_turnover",
            ]:
                mean_col = f"_{column}_mean_{window}"
                std_col = f"{column}_rolling_std_{window}"
                temp_columns.append(mean_col)
                rolling_exprs.extend(
                    [
                        pl.col(column).rolling_mean(window_size=window).alias(mean_col),
                        pl.col(column).rolling_std(window_size=window).alias(std_col),
                    ]
                )

        combined_df = combined_df.with_columns(rolling_exprs)

        zscore_exprs: list[pl.Expr] = []
        for window in self.ROLLING_WINDOWS:
            for column in [
                "imbalance_buy_quantity",
                "imbalance_buy_turnover",
            ]:
                zscore_exprs.append(
                    self._safe_zscore_expr(
                        pl.col(column),
                        pl.col(f"_{column}_mean_{window}"),
                        pl.col(f"{column}_rolling_std_{window}"),
                    ).alias(f"{column}_rolling_zscore_{window}")
                )

            for column in [
                "log_return_trade_quantity",
                "log_return_trade_turnover",
                "log_return_trade_count",
                "log_return_buy_quantity",
                "log_return_buy_turnover",
            ]:
                zscore_exprs.append(
                    self._safe_zscore_expr(
                        pl.col(column),
                        pl.col(f"_{column}_mean_{window}"),
                        pl.col(f"{column}_rolling_std_{window}"),
                    ).alias(f"{column}_rolling_zscore_{window}")
                )

        return combined_df.with_columns(zscore_exprs).drop(temp_columns)

    def momentum(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                (pl.col(column) - pl.col(column).shift(window)).alias(
                    f"{column}_momentum_{window}"
                )
                for window in self.ROLLING_WINDOWS
                for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS
            ]
        )

    def lag(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                pl.col(column).shift(window).alias(f"{column}_lag_{window}")
                for window in self.LAG_WINDOWS
                for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS
            ]
        )

    def aggregation(
        self,
        interval: str,
        timestamps: list[int],
        minio_historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(minio_historical_frames["spot_klines"])
        if df.is_empty():
            return None

        df = df.with_columns(
            [
                ((pl.col("open_time") // self.SOURCE_INTERVAL_MS) * self.SOURCE_INTERVAL_MS).alias(
                    "bucket_open_time"
                ),
            ]
        )
        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for open_boundary_ts_ms in timestamps:
            window_end = open_boundary_ts_ms + interval_ms
            window_df = df.filter(
                (pl.col("bucket_open_time") >= open_boundary_ts_ms)
                & (pl.col("bucket_open_time") < window_end)
            ).sort("bucket_open_time")

            if window_df.is_empty():
                continue

            close_ts_ms = window_end - 1
            rows.append(
                {
                    "open_time": datetime.fromtimestamp(open_boundary_ts_ms / 1000, tz=timezone.utc),
                    "close_time": datetime.fromtimestamp(close_ts_ms / 1000, tz=timezone.utc),
                    "trade_quantity": float(window_df["volume"].sum()),
                    "trade_turnover": float(window_df["quote_volume"].sum()),
                    "trade_count": int(window_df["count"].sum()),
                    "buy_quantity": float(window_df["taker_buy_volume"].sum()),
                    "buy_turnover": float(window_df["taker_buy_quote_volume"].sum()),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = SpotKlinesBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
