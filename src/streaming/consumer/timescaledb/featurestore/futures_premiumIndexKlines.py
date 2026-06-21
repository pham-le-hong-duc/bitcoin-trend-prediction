"""
Realtime TimescaleDB consumer for featurestore futures premium index klines.

Flow:
- Read 1m futures premium index klines from Redpanda
- Keep recent raw history in RAM for the active 1d window
- Aggregate the closed daily window
- Load recent featurestore history from TimescaleDB
- Recompute feature columns for the combined history + current batch
- Upsert only the current interval row into featurestore.futures_premiumindexklines_<interval>
"""

from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import Consumer


class FuturesPremiumIndexKlinesConsumer(Consumer):
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
    ROLLING_WINDOWS = [4, 8, 16, 32]
    LAG_WINDOWS = [1, 2, 4, 8, 16, 32]

    def __init__(self, **kwargs):
        super().__init__(
            topic="binance-futures-premiumIndexKlines",
            data_type="futures/um/daily/premiumIndexKlines/BTCUSDT/1m",
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
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def transform_record(self, record, topic):
        normalized = dict(record)
        normalized["open_time"] = int(record["open_time"])
        normalized["close_time"] = int(record["close_time"])
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
        return numerator / (denominator + 1e-8)

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
            self._safe_div_expr(
                pl.col("close") - pl.col("open"),
                pl.col("open"),
            ).alias("body_percentage")
        )

    def indicator(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            pl.col("close").shift(1).alias("previous_close")
        ).with_columns(
            self._safe_div_expr(
                pl.col("open") - pl.col("previous_close"),
                pl.col("previous_close"),
            ).alias("gap_percentage")
        ).drop("previous_close")

    def rolling(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        rolling_exprs = []
        temp_columns = []

        for window in self.ROLLING_WINDOWS:
            mean_col = f"_close_mean_{window}"
            std_col = f"close_rolling_std_{window}"
            temp_columns.append(mean_col)
            rolling_exprs.extend(
                [
                    pl.col("close").rolling_mean(window_size=window).alias(mean_col),
                    pl.col("close").rolling_std(window_size=window).alias(std_col),
                ]
            )

        combined_df = combined_df.with_columns(rolling_exprs)

        zscore_exprs = [
            self._safe_zscore_expr(
                pl.col("close"),
                pl.col(f"_close_mean_{window}"),
                pl.col(f"close_rolling_std_{window}"),
            ).alias(f"close_rolling_zscore_{window}")
            for window in self.ROLLING_WINDOWS
        ]

        return combined_df.with_columns(zscore_exprs).drop(temp_columns)

    def momentum(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                (pl.col("close") - pl.col("close").shift(window)).alias(
                    f"close_momentum_{window}"
                )
                for window in self.ROLLING_WINDOWS
            ]
        )

    def lag(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                pl.col("close").shift(window).alias(f"close_lag_{window}")
                for window in self.LAG_WINDOWS
            ]
        )

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
                    f"Skip featurestore futures_premiumindexklines {interval} @ {current_open_time}: "
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
        return ("featurestore", f"futures_premiumindexklines_{interval}")


def main():
    consumer = FuturesPremiumIndexKlinesConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
