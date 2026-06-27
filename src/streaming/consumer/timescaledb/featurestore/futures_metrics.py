"""
Realtime TimescaleDB consumer for featurestore futures metrics.

Flow:
- Read 5m futures metrics snapshots from Redpanda
- Keep recent raw history in RAM for the active 1d window
- Aggregate the closed 1h/4h/1d window
- Load recent featurestore history from TimescaleDB
- Recompute feature columns for the combined history + current batch
- Upsert only the current interval row into featurestore.futures_metrics_<interval>
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

import polars as pl

from .base import Consumer

logger = logging.getLogger(__name__)


class FuturesMetricsConsumer(Consumer):
    SOURCE_INTERVAL_MS = 5 * 60 * 1000
    VALUE_COLUMNS = [
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    LOGRETURN_COLUMNS = [
        "sum_open_interest_mean",
        "sum_open_interest_min",
        "sum_open_interest_p25",
        "sum_open_interest_p50",
        "sum_open_interest_p75",
        "sum_open_interest_max",
        "sum_open_interest_last",
        "sum_open_interest_value_mean",
        "sum_open_interest_value_min",
        "sum_open_interest_value_p25",
        "sum_open_interest_value_p50",
        "sum_open_interest_value_p75",
        "sum_open_interest_value_max",
        "sum_open_interest_value_last",
    ]
    ROLLING_WINDOWS = [4, 8, 16, 32]
    LAG_WINDOWS = [1, 2, 4, 8, 16, 32]
    TEMPORAL_FEATURE_SOURCE_COLUMNS = [
        "log_return_sum_open_interest_last",
        "log_return_sum_open_interest_value_last",
        "count_toptrader_long_short_ratio_last",
        "sum_toptrader_long_short_ratio_last",
        "count_long_short_ratio_last",
        "sum_taker_long_short_vol_ratio_last",
    ]

    def __init__(self, **kwargs):
        super().__init__(
            topic="binance-futures-metrics",
            group_id="timescaledb-featurestore-binance-futures-metrics",
            data_type="futures/um/daily/metrics/BTCUSDT",
            symbol="btcusdt",
            timestamp_field="bucket_create_time",
            intervals=["1h", "4h", "1d"],
            window_timestamp_mode="end",
            dedupe_columns=["bucket_create_time"],
            warmup_messages=50,
            schema_name="featurestore",
            key_column="create_time",
            historical_files_to_load=2,
            timescaledb_history_rows=60,
            **kwargs,
        )

    def feature_steps(self):
        return [
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def transform_record(self, record, topic):
        normalized = dict(record)
        normalized["create_time"] = self._normalize_create_time(record.get("create_time"))
        if normalized["create_time"] is not None:
            normalized["bucket_create_time"] = (
                normalized["create_time"] // self.SOURCE_INTERVAL_MS
            ) * self.SOURCE_INTERVAL_MS

        for column in self.VALUE_COLUMNS:
            normalized[column] = self._normalize_metric_value(record.get(column))

        return normalized if normalized["create_time"] is not None else None

    def transform_historical_df(self, df, source_name):
        if len(df) == 0 or "create_time" not in df.columns:
            return df

        records = []
        for row in df.to_dicts():
            normalized = self.transform_record(row, topic=self.topic)
            if normalized is not None:
                records.append(normalized)

        return pl.DataFrame(records) if records else pl.DataFrame()

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

    def combine_history(self, aggregated_df, timescaledb_historical_df):
        return self._combine_history_with_batch(
            aggregated_df,
            timescaledb_historical_df,
            aggregated_df.columns,
        )

    def _fill_zero_metric_values(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        return df.with_columns(
            [
                pl.when(pl.col(column).eq(0))
                .then(None)
                .otherwise(pl.col(column))
                .alias(column)
                for column in self.VALUE_COLUMNS
            ]
        ).with_columns(
            [
                pl.col(column).fill_null(strategy="forward").alias(column)
                for column in self.VALUE_COLUMNS
            ]
        ).with_columns(
            [
                pl.col(column).fill_null(strategy="backward").alias(column)
                for column in self.VALUE_COLUMNS
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

        rolling_mean_columns = {
            "log_return_sum_open_interest_last",
            "log_return_sum_open_interest_value_last",
        }
        rolling_std_zscore_columns = [
            "log_return_sum_open_interest_last",
            "log_return_sum_open_interest_value_last",
            "count_toptrader_long_short_ratio_last",
            "sum_toptrader_long_short_ratio_last",
            "count_long_short_ratio_last",
            "sum_taker_long_short_vol_ratio_last",
        ]

        rolling_exprs = []
        temp_columns = []

        for window in self.ROLLING_WINDOWS:
            for column in rolling_std_zscore_columns:
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

        output_exprs = []
        for window in self.ROLLING_WINDOWS:
            for column in rolling_std_zscore_columns:
                if column in rolling_mean_columns:
                    output_exprs.append(
                        pl.col(f"_{column}_mean_{window}").alias(
                            f"{column}_rolling_mean_{window}"
                        )
                    )

                output_exprs.append(
                    self._safe_zscore_expr(
                        pl.col(column),
                        pl.col(f"_{column}_mean_{window}"),
                        pl.col(f"{column}_rolling_std_{window}"),
                    ).alias(f"{column}_rolling_zscore_{window}")
                )

        return combined_df.with_columns(output_exprs).drop(temp_columns)

    def momentum(self, combined_df):
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

    def lag(self, combined_df):
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                pl.col(column).shift(window).alias(f"{column}_lag_{window}")
                for window in self.LAG_WINDOWS
                for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS
            ]
        )

    def aggregate_window(self, df_window, window_ts, interval):
        try:
            df_sorted = df_window.sort("bucket_create_time")
            if len(df_sorted) == 0:
                return None
            df_sorted = self._fill_zero_metric_values(df_sorted)

            aggregated_df = pl.DataFrame(
                [
                    {
                        "create_time": datetime.fromtimestamp(window_ts / 1000, tz=timezone.utc),
                        "sum_open_interest_mean": float(df_sorted["sum_open_interest"].mean() or 0.0),
                        "sum_open_interest_std": float(df_sorted["sum_open_interest"].std() or 0.0),
                        "sum_open_interest_min": float(df_sorted["sum_open_interest"].min() or 0.0),
                        "sum_open_interest_p25": float(df_sorted["sum_open_interest"].quantile(0.25) or 0.0),
                        "sum_open_interest_p50": float(df_sorted["sum_open_interest"].quantile(0.50) or 0.0),
                        "sum_open_interest_p75": float(df_sorted["sum_open_interest"].quantile(0.75) or 0.0),
                        "sum_open_interest_max": float(df_sorted["sum_open_interest"].max() or 0.0),
                        "sum_open_interest_skew": float(df_sorted["sum_open_interest"].skew() or 0.0),
                        "sum_open_interest_kurtosis": float(df_sorted["sum_open_interest"].kurtosis() or 0.0),
                        "sum_open_interest_last": float(df_sorted["sum_open_interest"][-1] or 0.0),
                        "sum_open_interest_value_mean": float(df_sorted["sum_open_interest_value"].mean() or 0.0),
                        "sum_open_interest_value_std": float(df_sorted["sum_open_interest_value"].std() or 0.0),
                        "sum_open_interest_value_min": float(df_sorted["sum_open_interest_value"].min() or 0.0),
                        "sum_open_interest_value_p25": float(df_sorted["sum_open_interest_value"].quantile(0.25) or 0.0),
                        "sum_open_interest_value_p50": float(df_sorted["sum_open_interest_value"].quantile(0.50) or 0.0),
                        "sum_open_interest_value_p75": float(df_sorted["sum_open_interest_value"].quantile(0.75) or 0.0),
                        "sum_open_interest_value_max": float(df_sorted["sum_open_interest_value"].max() or 0.0),
                        "sum_open_interest_value_skew": float(df_sorted["sum_open_interest_value"].skew() or 0.0),
                        "sum_open_interest_value_kurtosis": float(df_sorted["sum_open_interest_value"].kurtosis() or 0.0),
                        "sum_open_interest_value_last": float(df_sorted["sum_open_interest_value"][-1] or 0.0),
                        "count_toptrader_long_short_ratio_mean": float(
                            df_sorted["count_toptrader_long_short_ratio"].mean() or 0.0
                        ),
                        "count_toptrader_long_short_ratio_std": float(
                            df_sorted["count_toptrader_long_short_ratio"].std() or 0.0
                        ),
                        "count_toptrader_long_short_ratio_last": float(
                            df_sorted["count_toptrader_long_short_ratio"][-1] or 0.0
                        ),
                        "sum_toptrader_long_short_ratio_mean": float(
                            df_sorted["sum_toptrader_long_short_ratio"].mean() or 0.0
                        ),
                        "sum_toptrader_long_short_ratio_std": float(
                            df_sorted["sum_toptrader_long_short_ratio"].std() or 0.0
                        ),
                        "sum_toptrader_long_short_ratio_last": float(
                            df_sorted["sum_toptrader_long_short_ratio"][-1] or 0.0
                        ),
                        "count_long_short_ratio_mean": float(
                            df_sorted["count_long_short_ratio"].mean() or 0.0
                        ),
                        "count_long_short_ratio_std": float(
                            df_sorted["count_long_short_ratio"].std() or 0.0
                        ),
                        "count_long_short_ratio_last": float(
                            df_sorted["count_long_short_ratio"][-1] or 0.0
                        ),
                        "sum_taker_long_short_vol_ratio_mean": float(
                            df_sorted["sum_taker_long_short_vol_ratio"].mean() or 0.0
                        ),
                        "sum_taker_long_short_vol_ratio_std": float(
                            df_sorted["sum_taker_long_short_vol_ratio"].std() or 0.0
                        ),
                        "sum_taker_long_short_vol_ratio_last": float(
                            df_sorted["sum_taker_long_short_vol_ratio"][-1] or 0.0
                        ),
                    }
                ]
            )

            current_create_time = aggregated_df["create_time"][0]
            _, table_name = self.resolve_table_target(interval)
            timescaledb_historical_df = self._load_timescaledb_history(
                table_name=table_name,
                current_time=current_create_time,
                time_column="create_time",
                schema_name="featurestore",
            )
            is_valid_history, history_reason = self._validate_timescaledb_history(
                timescaledb_historical_df,
                current_time=current_create_time,
                interval=interval,
                time_column="create_time",
            )
            if not is_valid_history:
                logger.warning(
                    f"Skip featurestore futures_metrics {interval} @ {current_create_time}: "
                    f"{history_reason}"
                )
                return None

            combined_df = self.combine_history(aggregated_df, timescaledb_historical_df)
            result_df = self._run_feature_steps(combined_df)
            if result_df is None or result_df.is_empty():
                return None

            return result_df.filter(pl.col("create_time") == current_create_time).sort("create_time")
        except Exception as exc:
            print(f"Error in aggregate_window ({interval}): {exc}")
            return None

    def resolve_table_target(self, interval):
        return ("featurestore", f"futures_metrics_{interval}")

    def _normalize_create_time(self, value: Any):
        if value is None or value == "":
            return None

        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            else:
                value = value.astimezone(timezone.utc)
            return int(value.timestamp() * 1000)

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            if stripped.isdigit():
                return self._normalize_epoch_value_to_ms(int(stripped))

            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(stripped, fmt).replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue
            return None

        if isinstance(value, (int, float)):
            return self._normalize_epoch_value_to_ms(value)

        return None

    def _normalize_metric_value(self, value: Any):
        if value is None or value == "":
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                return float(stripped)
            except ValueError:
                return None
        if isinstance(value, (int, float)):
            return float(value)
        return None


def main():
    consumer = FuturesMetricsConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
