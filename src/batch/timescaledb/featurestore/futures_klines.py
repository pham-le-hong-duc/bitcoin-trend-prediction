from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesKlinesBatch(HistoricalTimescaleBatch):
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
    LOGRETURN_COLUMNS = [
        "open",
        "high",
        "low",
        "close",
        "trade_quantity",
        "trade_turnover",
        "trade_count",
        "buy_quantity",
        "buy_turnover",
    ]
    ROLLING_WINDOWS = [4, 8, 16, 32]
    TEMPORAL_FEATURE_SOURCE_COLUMNS = [
        "relative_range",
        "imbalance_buy_quantity",
        "imbalance_buy_turnover",
        "log_return_close",
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
                    name="futures_klines",
                    prefix="futures/um/daily/klines/BTCUSDT/1m",
                )
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_klines_{interval}"

    def feature_steps(self) -> list[tuple[str, HistoricalTimescaleBatch.FeatureStep]]:
        return [
            ("derivative", self.derivative),
            ("indicator", self.indicator),
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def _align_boundary(self, ts_ms: int, interval_ms: int) -> int:
        # For open_time-keyed buckets, a raw 1m candle at 10:04 belongs to the
        # 5m bucket that opens at 10:00, so propagation must use floor alignment.
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
                    self._normalize_epoch_to_ms_expr("open_time"),
                    self._normalize_epoch_to_ms_expr("close_time"),
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
                pl.max_horizontal("open", "close").alias("candle_max"),
                pl.min_horizontal("open", "close").alias("candle_min"),
                (pl.col("high") - pl.col("low")).alias("price_range"),
            ]
        ).with_columns(
            [
                self._safe_div_expr(
                    pl.col("high") - pl.col("low"),
                    pl.col("close"),
                ).alias("relative_range"),
                self._safe_div_expr(
                    pl.col("buy_quantity") - (pl.col("trade_quantity") - pl.col("buy_quantity")),
                    pl.col("trade_quantity"),
                ).alias("imbalance_buy_quantity"),
                self._safe_div_expr(
                    pl.col("buy_turnover") - (pl.col("trade_turnover") - pl.col("buy_turnover")),
                    pl.col("trade_turnover"),
                ).alias("imbalance_buy_turnover"),
                self._safe_div_expr(
                    pl.col("close") - pl.col("open"),
                    pl.col("open"),
                ).alias("body_percentage"),
                self._safe_div_expr(
                    pl.col("high") - pl.col("candle_max"),
                    pl.col("price_range"),
                ).alias("upper_wick_percentage"),
                self._safe_div_expr(
                    pl.col("candle_min") - pl.col("low"),
                    pl.col("price_range"),
                ).alias("lower_wick_percentage"),
            ]
        ).drop(
            [
                "candle_max",
                "candle_min",
                "price_range",
            ]
        )

    def indicator(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                pl.col("close").shift(1).alias("previous_close"),
                pl.col("close").diff().alias("close_delta"),
                pl.col("close").ewm_mean(span=12, adjust=False).alias("ema12"),
                pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20"),
                pl.col("close").ewm_mean(span=26, adjust=False).alias("ema26"),
                pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
            ]
        ).with_columns(
            [
                self._safe_div_expr(
                    pl.col("open") - pl.col("previous_close"),
                    pl.col("previous_close"),
                ).alias("gap_percentage"),
                pl.when(pl.col("close_delta") > 0)
                .then(pl.col("close_delta"))
                .otherwise(0.0)
                .alias("gain"),
                pl.when(pl.col("close_delta") < 0)
                .then(-pl.col("close_delta"))
                .otherwise(0.0)
                .alias("loss"),
                pl.max_horizontal(
                    pl.col("high") - pl.col("low"),
                    (pl.col("high") - pl.col("previous_close")).abs(),
                    (pl.col("low") - pl.col("previous_close")).abs(),
                ).alias("true_range"),
                (pl.col("ema12") - pl.col("ema26")).alias("macd"),
            ]
        ).with_columns(
            [
                pl.col("gain").rolling_mean(window_size=14).alias("avg_gain_14"),
                pl.col("loss").rolling_mean(window_size=14).alias("avg_loss_14"),
                pl.col("macd").ewm_mean(span=9, adjust=False).alias("macd_signal"),
                pl.col("true_range").rolling_mean(window_size=14).alias("atr14"),
            ]
        ).with_columns(
            [
                pl.when(
                    pl.col("avg_gain_14").eq(0) & pl.col("avg_loss_14").eq(0)
                )
                .then(50.0)
                .when(pl.col("avg_loss_14").eq(0))
                .then(100.0)
                .otherwise(
                    100.0
                    - (
                        100.0
                        / (
                            1.0
                            + (
                                pl.col("avg_gain_14")
                                / pl.col("avg_loss_14")
                            )
                        )
                    )
                )
                .alias("rsi14"),
                (pl.col("macd") - pl.col("macd_signal")).alias("macd_histogram"),
            ]
        ).drop(
            [
                "previous_close",
                "close_delta",
                "ema12",
                "ema26",
                "gain",
                "loss",
                "true_range",
                "avg_gain_14",
                "avg_loss_14",
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
            relative_range_mean_col = f"_relative_range_mean_{window}"
            relative_range_std_col = f"relative_range_rolling_std_{window}"
            temp_columns.append(relative_range_mean_col)
            rolling_exprs.extend(
                [
                    pl.col("relative_range")
                    .rolling_mean(window_size=window)
                    .alias(relative_range_mean_col),
                    pl.col("relative_range")
                    .rolling_std(window_size=window)
                    .alias(relative_range_std_col),
                ]
            )

            for column in [
                "imbalance_buy_quantity",
                "imbalance_buy_turnover",
            ]:
                mean_col = f"_{column}_mean_{window}"
                std_col = f"{column}_rolling_std_{window}"
                temp_columns.append(mean_col)
                rolling_exprs.extend(
                    [
                        pl.col(column)
                        .rolling_mean(window_size=window)
                        .alias(mean_col),
                        pl.col(column)
                        .rolling_std(window_size=window)
                        .alias(std_col),
                    ]
                )

            log_return_close_mean_col = f"log_return_close_rolling_mean_{window}"
            log_return_close_std_col = f"log_return_close_rolling_std_{window}"
            rolling_exprs.extend(
                [
                    pl.col("log_return_close")
                    .rolling_mean(window_size=window)
                    .alias(log_return_close_mean_col),
                    pl.col("log_return_close")
                    .rolling_std(window_size=window)
                    .alias(log_return_close_std_col),
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
                        pl.col(column)
                        .rolling_mean(window_size=window)
                        .alias(mean_col),
                        pl.col(column)
                        .rolling_std(window_size=window)
                        .alias(std_col),
                    ]
                )

        combined_df = combined_df.with_columns(rolling_exprs)

        zscore_exprs: list[pl.Expr] = []
        for window in self.ROLLING_WINDOWS:
            zscore_exprs.extend(
                [
                    self._safe_zscore_expr(
                        pl.col("relative_range"),
                        pl.col(f"_relative_range_mean_{window}"),
                        pl.col(f"relative_range_rolling_std_{window}"),
                    ).alias(f"relative_range_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("imbalance_buy_quantity"),
                        pl.col(f"_imbalance_buy_quantity_mean_{window}"),
                        pl.col(f"imbalance_buy_quantity_rolling_std_{window}"),
                    ).alias(f"imbalance_buy_quantity_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("imbalance_buy_turnover"),
                        pl.col(f"_imbalance_buy_turnover_mean_{window}"),
                        pl.col(f"imbalance_buy_turnover_rolling_std_{window}"),
                    ).alias(f"imbalance_buy_turnover_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_close"),
                        pl.col(f"log_return_close_rolling_mean_{window}"),
                        pl.col(f"log_return_close_rolling_std_{window}"),
                    ).alias(f"log_return_close_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_trade_quantity"),
                        pl.col(f"_log_return_trade_quantity_mean_{window}"),
                        pl.col(f"log_return_trade_quantity_rolling_std_{window}"),
                    ).alias(f"log_return_trade_quantity_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_trade_turnover"),
                        pl.col(f"_log_return_trade_turnover_mean_{window}"),
                        pl.col(f"log_return_trade_turnover_rolling_std_{window}"),
                    ).alias(f"log_return_trade_turnover_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_trade_count"),
                        pl.col(f"_log_return_trade_count_mean_{window}"),
                        pl.col(f"log_return_trade_count_rolling_std_{window}"),
                    ).alias(f"log_return_trade_count_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_buy_quantity"),
                        pl.col(f"_log_return_buy_quantity_mean_{window}"),
                        pl.col(f"log_return_buy_quantity_rolling_std_{window}"),
                    ).alias(f"log_return_buy_quantity_rolling_zscore_{window}"),
                    self._safe_zscore_expr(
                        pl.col("log_return_buy_turnover"),
                        pl.col(f"_log_return_buy_turnover_mean_{window}"),
                        pl.col(f"log_return_buy_turnover_rolling_std_{window}"),
                    ).alias(f"log_return_buy_turnover_rolling_zscore_{window}"),
                ]
            )

        combined_df = combined_df.with_columns(zscore_exprs).drop(temp_columns)

        return combined_df

    def momentum(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        momentum_exprs: list[pl.Expr] = []
        for window in self.ROLLING_WINDOWS:
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
                momentum_exprs.append(
                    (pl.col(column) - pl.col(column).shift(window)).alias(
                        f"{column}_momentum_{window}"
                    )
                )

        return combined_df.with_columns(momentum_exprs)

    def lag(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        lag_exprs: list[pl.Expr] = []
        for window in self.LAG_WINDOWS:
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
                lag_exprs.append(
                    pl.col(column).shift(window).alias(f"{column}_lag_{window}")
                )

        return combined_df.with_columns(lag_exprs)

    def aggregation(
        self,
        interval: str,
        timestamps: list[int],
        minio_historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(minio_historical_frames["futures_klines"])
        if df.is_empty():
            return None

        df = df.with_columns(
            [
                # Some raw files carry slightly inconsistent open_time/close_time values.
                # Snap the source candle to the canonical 1m bucket first so aggregation
                # always uses the canonical 1m open boundary.
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
                    "open": float(window_df["open"][0]),
                    "high": float(window_df["high"].max()),
                    "low": float(window_df["low"].min()),
                    "close": float(window_df["close"][-1]),
                    "trade_quantity": float(window_df["volume"].sum()),
                    "trade_turnover": float(window_df["quote_volume"].sum()),
                    "trade_count": int(window_df["count"].sum()),
                    "buy_quantity": float(window_df["taker_buy_volume"].sum()),
                    "buy_turnover": float(window_df["taker_buy_quote_volume"].sum()),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = FuturesKlinesBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
