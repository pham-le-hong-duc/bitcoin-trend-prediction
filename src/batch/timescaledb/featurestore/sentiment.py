from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from .base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class SentimentBatch(HistoricalTimescaleBatch):
    ROLLING_WINDOWS = [4, 8, 16, 32]
    LAG_WINDOWS = [1, 2, 4, 8, 16, 32]
    TEMPORAL_FEATURE_SOURCE_COLUMNS = [
        "log_return_count",
        "score",
        "confidence",
        "pct_negative",
        "pct_positive",
        "pct_neutral",
    ]

    def __init__(self) -> None:
        super().__init__(
            schema_name="featurestore",
            time_column="create_time",
            intervals=["1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="submission",
                    prefix="submissions",
                    file_pattern="monthly",
                    file_prefix="RS",
                ),
                HistoricalSource(
                    name="comment",
                    prefix="comments",
                    file_pattern="monthly",
                    file_prefix="RC",
                ),
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="reddit",
        )

    def table_name(self, interval: str) -> str:
        return f"sentiment_{interval}"

    def feature_steps(self) -> list[tuple[str, HistoricalTimescaleBatch.FeatureStep]]:
        return [
            ("log_return", self.log_return),
            ("rolling", self.rolling),
            ("momentum", self.momentum),
            ("lag", self.lag),
        ]

    def _normalize_submission(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        return (
            df.filter(
                pl.col("sentiment").is_not_null()
                & (pl.col("sentiment").cast(pl.Utf8) != "")
                & pl.col("created_utc").is_not_null()
            )
            .with_columns(
                pl.lit("submission").alias("source"),
                self._normalize_epoch_to_ms_expr("created_utc", alias="event_ts_ms"),
                pl.col("sentiment").cast(pl.Int64),
            )
            .select(["source", "event_ts_ms", "sentiment"])
        )

    def _normalize_comment(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        return (
            df.filter(
                pl.col("sentiment").is_not_null()
                & (pl.col("sentiment").cast(pl.Utf8) != "")
                & pl.col("created_utc").is_not_null()
            )
            .with_columns(
                pl.lit("comment").alias("source"),
                self._normalize_epoch_to_ms_expr("created_utc", alias="event_ts_ms"),
                pl.col("sentiment").cast(pl.Int64),
            )
            .select(["source", "event_ts_ms", "sentiment"])
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
            .sort("create_time")
            .unique(subset=["create_time"], keep="last", maintain_order=True)
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

    def aggregation(
        self,
        interval: str,
        timestamps: list[int],
        minio_historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        frames = []
        submission_df = minio_historical_frames.get("submission")
        comment_df = minio_historical_frames.get("comment")

        if submission_df is not None and not submission_df.is_empty():
            frames.append(self._normalize_submission(submission_df))
        if comment_df is not None and not comment_df.is_empty():
            frames.append(self._normalize_comment(comment_df))

        if not frames:
            return None

        df = pl.concat(frames, how="vertical_relaxed")
        if df.is_empty():
            return None

        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = df.filter(
                (pl.col("event_ts_ms") >= window_start)
                & (pl.col("event_ts_ms") < boundary_ts_ms)
            )

            if window_df.is_empty():
                continue

            sentiments = window_df["sentiment"].to_list()
            total = len(sentiments)
            positive = sum(1 for value in sentiments if value == 2)
            neutral = sum(1 for value in sentiments if value == 1)
            negative = sum(1 for value in sentiments if value == 0)

            rows.append(
                {
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    "count": total,
                    "score": (positive - negative) / total,
                    "confidence": (positive + negative) / total,
                    "pct_negative": negative / total,
                    "pct_positive": positive / total,
                    "pct_neutral": neutral / total,
                }
            )

        return pl.DataFrame(rows) if rows else None

    def log_return(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            pl.when(pl.col("count") <= 0)
            .then(None)
            .otherwise(pl.col("count").cast(pl.Float64).log())
            .alias("log_return_count")
        )

    def rolling(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        rolling_exprs: list[pl.Expr] = []
        temp_columns: list[str] = []

        for window in self.ROLLING_WINDOWS:
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
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
            for column in self.TEMPORAL_FEATURE_SOURCE_COLUMNS:
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


def main() -> None:
    batch = SentimentBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
