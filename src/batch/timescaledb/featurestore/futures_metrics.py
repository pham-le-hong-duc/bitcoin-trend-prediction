from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesMetricsBatch(HistoricalTimescaleBatch):
    DUPLICATED_SUFFIX_PATTERN = re.compile(r"_duplicated_\d+$")
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
    SOURCE_INTERVAL_MS = INTERVAL_TO_MS["5m"]

    def __init__(self) -> None:
        super().__init__(
            schema_name="featurestore",
            time_column="create_time",
            intervals=["1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="futures_metrics",
                    prefix="futures/um/daily/metrics/BTCUSDT",
                )
            ],
            base_start_date=datetime(2020, 9, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_metrics_{interval}"

    def feature_steps(self) -> list[tuple[str, HistoricalTimescaleBatch.FeatureStep]]:
        return [
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def _clean_header_value(self, value: str) -> str:
        return self.DUPLICATED_SUFFIX_PATTERN.sub("", value)

    def _recover_headerless_df(self, df: pl.DataFrame) -> pl.DataFrame:
        recovered_first_row = {
            expected: self._clean_header_value(current)
            for current, expected in zip(df.columns, self.METRICS_COLUMNS)
        }
        renamed_df = df.rename(
            {
                current: expected
                for current, expected in zip(df.columns, self.METRICS_COLUMNS)
            }
        )
        recovered_df = pl.DataFrame([recovered_first_row])
        return pl.concat([recovered_df, renamed_df], how="vertical_relaxed")

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
            df = self._recover_headerless_df(df)

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
            df = df.with_columns(pl.col("create_time").cast(pl.Int64, strict=False))

        df = df.with_columns(self._normalize_epoch_to_ms_expr("create_time"))

        value_expressions = []
        for column in self.VALUE_COLUMNS:
            value_expressions.append(
                pl.when(pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(column))
                .cast(pl.Float64, strict=False)
                .alias(column)
            )

        return (
            df.with_columns(value_expressions)
            .filter(pl.col("create_time").is_not_null())
            .with_columns(
                (
                    (pl.col("create_time") // self.SOURCE_INTERVAL_MS)
                    * self.SOURCE_INTERVAL_MS
                ).alias("bucket_create_time")
            )
        )

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return self._normalize_historical_df(df)

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
            .sort("create_time")
            .unique(subset=["create_time"], keep="last", maintain_order=True)
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

        rolling_exprs: list[pl.Expr] = []
        temp_columns: list[str] = []

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

        output_exprs: list[pl.Expr] = []
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
        df = self._normalize_historical_df(minio_historical_frames["futures_metrics"])
        if df.is_empty():
            return None

        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = df.filter(
                (pl.col("bucket_create_time") > window_start)
                & (pl.col("bucket_create_time") <= boundary_ts_ms)
            ).sort("bucket_create_time")

            if window_df.is_empty():
                continue

            window_df = self._fill_zero_metric_values(window_df)

            rows.append(
                {
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    "sum_open_interest_mean": float(window_df["sum_open_interest"].mean() or 0.0),
                    "sum_open_interest_std": float(window_df["sum_open_interest"].std() or 0.0),
                    "sum_open_interest_min": float(window_df["sum_open_interest"].min() or 0.0),
                    "sum_open_interest_p25": float(window_df["sum_open_interest"].quantile(0.25) or 0.0),
                    "sum_open_interest_p50": float(window_df["sum_open_interest"].quantile(0.50) or 0.0),
                    "sum_open_interest_p75": float(window_df["sum_open_interest"].quantile(0.75) or 0.0),
                    "sum_open_interest_max": float(window_df["sum_open_interest"].max() or 0.0),
                    "sum_open_interest_skew": float(window_df["sum_open_interest"].skew() or 0.0),
                    "sum_open_interest_kurtosis": float(window_df["sum_open_interest"].kurtosis() or 0.0),
                    "sum_open_interest_last": float(window_df["sum_open_interest"][-1] or 0.0),
                    "sum_open_interest_value_mean": float(window_df["sum_open_interest_value"].mean() or 0.0),
                    "sum_open_interest_value_std": float(window_df["sum_open_interest_value"].std() or 0.0),
                    "sum_open_interest_value_min": float(window_df["sum_open_interest_value"].min() or 0.0),
                    "sum_open_interest_value_p25": float(window_df["sum_open_interest_value"].quantile(0.25) or 0.0),
                    "sum_open_interest_value_p50": float(window_df["sum_open_interest_value"].quantile(0.50) or 0.0),
                    "sum_open_interest_value_p75": float(window_df["sum_open_interest_value"].quantile(0.75) or 0.0),
                    "sum_open_interest_value_max": float(window_df["sum_open_interest_value"].max() or 0.0),
                    "sum_open_interest_value_skew": float(window_df["sum_open_interest_value"].skew() or 0.0),
                    "sum_open_interest_value_kurtosis": float(window_df["sum_open_interest_value"].kurtosis() or 0.0),
                    "sum_open_interest_value_last": float(window_df["sum_open_interest_value"][-1] or 0.0),
                    "count_toptrader_long_short_ratio_mean": float(
                        window_df["count_toptrader_long_short_ratio"].mean() or 0.0
                    ),
                    "count_toptrader_long_short_ratio_std": float(
                        window_df["count_toptrader_long_short_ratio"].std() or 0.0
                    ),
                    "count_toptrader_long_short_ratio_last": float(
                        window_df["count_toptrader_long_short_ratio"][-1] or 0.0
                    ),
                    "sum_toptrader_long_short_ratio_mean": float(
                        window_df["sum_toptrader_long_short_ratio"].mean() or 0.0
                    ),
                    "sum_toptrader_long_short_ratio_std": float(
                        window_df["sum_toptrader_long_short_ratio"].std() or 0.0
                    ),
                    "sum_toptrader_long_short_ratio_last": float(
                        window_df["sum_toptrader_long_short_ratio"][-1] or 0.0
                    ),
                    "count_long_short_ratio_mean": float(
                        window_df["count_long_short_ratio"].mean() or 0.0
                    ),
                    "count_long_short_ratio_std": float(
                        window_df["count_long_short_ratio"].std() or 0.0
                    ),
                    "count_long_short_ratio_last": float(
                        window_df["count_long_short_ratio"][-1] or 0.0
                    ),
                    "sum_taker_long_short_vol_ratio_mean": float(
                        window_df["sum_taker_long_short_vol_ratio"].mean() or 0.0
                    ),
                    "sum_taker_long_short_vol_ratio_std": float(
                        window_df["sum_taker_long_short_vol_ratio"].std() or 0.0
                    ),
                    "sum_taker_long_short_vol_ratio_last": float(
                        window_df["sum_taker_long_short_vol_ratio"][-1] or 0.0
                    ),
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
