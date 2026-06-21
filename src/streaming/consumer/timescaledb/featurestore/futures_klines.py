"""
Realtime TimescaleDB consumer for featurestore futures klines.

Flow:
- Read 1m futures klines from Redpanda
- Keep recent raw history in RAM for the active 1d window
- Aggregate the closed daily window
- Load recent featurestore history from TimescaleDB
- Recompute feature columns for the combined history + current batch
- Upsert only the current interval row into featurestore.futures_klines_<interval>
"""

from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import Consumer


class FuturesKlinesConsumer(Consumer):
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
    SOURCE_INTERVAL_MS = 60 * 1000
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

    def __init__(self, **kwargs):
        super().__init__(
            topic="binance-futures-klines",
            group_id="timescaledb-featurestore-binance-futures-klines",
            data_type="futures/um/daily/klines/BTCUSDT/1m",
            symbol="btcusdt",
            timestamp_field="effective_close_time",
            intervals=["1h", "4h", "1d"],
            window_timestamp_mode="end",
            dedupe_columns=["effective_close_time"],
            warmup_messages=10,
            schema_name="featurestore",
            key_column="open_time",
            historical_files_to_load=2,
            timescaledb_history_rows=60,
            **kwargs,
        )

    def feature_steps(self):
        return [
            ("derivative", self.derivative),
            ("indicator", self.indicator),
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def transform_record(self, record, topic):
        normalized = dict(record)
        normalized["open_time"] = int(record["open_time"])
        normalized["close_time"] = int(record["close_time"])
        normalized["count"] = int(record["count"])
        for column in self.FLOAT_COLUMNS:
            normalized[column] = float(record[column])

        bucket_open_time = (
            normalized["open_time"] // self.SOURCE_INTERVAL_MS
        ) * self.SOURCE_INTERVAL_MS
        normalized["bucket_open_time"] = bucket_open_time
        normalized["effective_close_time"] = bucket_open_time + self.SOURCE_INTERVAL_MS
        return normalized

    def _clean_header_value(self, value):
        return self.DUPLICATED_SUFFIX_PATTERN.sub("", value)

    def _recover_headerless_df(self, df):
        recovered_first_row = {
            expected: self._clean_header_value(current)
            for current, expected in zip(df.columns, self.KLINE_COLUMNS)
        }
        renamed_df = df.rename(
            {current: expected for current, expected in zip(df.columns, self.KLINE_COLUMNS)}
        )
        recovered_df = pl.DataFrame([recovered_first_row])
        return pl.concat([recovered_df, renamed_df], how="vertical_relaxed")

    def _normalize_historical_df(self, df):
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
            .with_columns(
                (
                    (pl.col("open_time") // self.SOURCE_INTERVAL_MS)
                    * self.SOURCE_INTERVAL_MS
                ).alias("bucket_open_time")
            )
            .with_columns(
                [
                    (pl.col("bucket_open_time") + self.SOURCE_INTERVAL_MS).alias(
                        "effective_close_time"
                    ),
                ]
            )
        )

    def transform_historical_df(self, df, source_name):
        return self._normalize_historical_df(df)

    @staticmethod
    def _safe_div_expr(numerator, denominator):
        return (
            pl.when(denominator.is_null() | denominator.eq(0))
            .then(0.0)
            .otherwise(numerator / denominator)
        )

    @staticmethod
    def _safe_log_return_expr(current, previous):
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
    def _safe_zscore_expr(value, mean, std):
        return (
            pl.when(value.is_null() | mean.is_null() | std.is_null() | std.eq(0))
            .then(None)
            .otherwise((value - mean) / std)
        )

    def combine_history(self, aggregated_df, timescaledb_historical_df):
        return self._combine_history_with_batch(
            aggregated_df,
            timescaledb_historical_df,
            aggregated_df.columns,
        )

    def derivative(self, combined_df):
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

    def indicator(self, combined_df):
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

    def log_return(self, combined_df):
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

    def rolling(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        rolling_exprs = []
        temp_columns = []

        for window in self.ROLLING_WINDOWS:
            relative_range_mean_col = f"_relative_range_mean_{window}"
            relative_range_std_col = f"relative_range_rolling_std_{window}"
            temp_columns.append(relative_range_mean_col)
            rolling_exprs.extend(
                [
                    pl.col("relative_range").rolling_mean(window_size=window).alias(
                        relative_range_mean_col
                    ),
                    pl.col("relative_range").rolling_std(window_size=window).alias(
                        relative_range_std_col
                    ),
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
                        pl.col(column).rolling_mean(window_size=window).alias(mean_col),
                        pl.col(column).rolling_std(window_size=window).alias(std_col),
                    ]
                )

            log_return_close_mean_col = f"log_return_close_rolling_mean_{window}"
            log_return_close_std_col = f"log_return_close_rolling_std_{window}"
            rolling_exprs.extend(
                [
                    pl.col("log_return_close").rolling_mean(window_size=window).alias(
                        log_return_close_mean_col
                    ),
                    pl.col("log_return_close").rolling_std(window_size=window).alias(
                        log_return_close_std_col
                    ),
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

        zscore_exprs = []
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

        return combined_df.with_columns(zscore_exprs).drop(temp_columns)

    def momentum(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        momentum_exprs = []
        for window in self.ROLLING_WINDOWS:
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
                momentum_exprs.append(
                    (pl.col(column) - pl.col(column).shift(window)).alias(
                        f"{column}_momentum_{window}"
                    )
                )

        return combined_df.with_columns(momentum_exprs)

    def lag(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        lag_exprs = []
        for window in self.LAG_WINDOWS:
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
                lag_exprs.append(
                    pl.col(column).shift(window).alias(f"{column}_lag_{window}")
                )

        return combined_df.with_columns(lag_exprs)

    def aggregate_window(self, df_window, window_ts, interval):
        try:
            df_sorted = df_window.sort("bucket_open_time")
            aggregated_df = pl.DataFrame(
                [
                    {
                        "open_time": datetime.fromtimestamp(
                            (window_ts - self._get_window_size_ms(interval)) / 1000,
                            tz=timezone.utc,
                        ),
                        "close_time": datetime.fromtimestamp(
                            (window_ts - 1) / 1000,
                            tz=timezone.utc,
                        ),
                        "open": float(df_sorted["open"][0]),
                        "high": float(df_sorted["high"].max()),
                        "low": float(df_sorted["low"].min()),
                        "close": float(df_sorted["close"][-1]),
                        "trade_quantity": float(df_sorted["volume"].sum()),
                        "trade_turnover": float(df_sorted["quote_volume"].sum()),
                        "trade_count": int(df_sorted["count"].sum()),
                        "buy_quantity": float(df_sorted["taker_buy_volume"].sum()),
                        "buy_turnover": float(df_sorted["taker_buy_quote_volume"].sum()),
                    }
                ]
            )

            current_open_time = aggregated_df["open_time"][0]
            _, table_name = self.resolve_table_target(interval)
            timescaledb_historical_df = self._load_timescaledb_history(
                table_name=table_name,
                current_time=current_open_time,
                time_column="open_time",
                schema_name="featurestore",
            )
            is_valid_history, history_reason = self._validate_timescaledb_history(
                timescaledb_historical_df,
                current_time=current_open_time,
                interval=interval,
                time_column="open_time",
            )
            if not is_valid_history:
                print(
                    f"Skip featurestore futures_klines {interval} @ {current_open_time}: "
                    f"{history_reason}"
                )
                return None

            combined_df = self.combine_history(aggregated_df, timescaledb_historical_df)
            result_df = self._run_feature_steps(combined_df)
            if result_df is None or result_df.is_empty():
                return None

            return result_df.filter(pl.col("open_time") == current_open_time).sort("open_time")
        except Exception as exc:
            print(f"Error in aggregate_window ({interval}): {exc}")
            return None

    def resolve_table_target(self, interval):
        return ("featurestore", f"futures_klines_{interval}")


def main():
    consumer = FuturesKlinesConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
