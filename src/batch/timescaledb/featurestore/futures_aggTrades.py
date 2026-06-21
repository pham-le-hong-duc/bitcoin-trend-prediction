from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from .base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesAggTradesBatch(HistoricalTimescaleBatch):
    DUPLICATED_SUFFIX_PATTERN = re.compile(r"_duplicated_\d+$")
    AGGTRADE_COLUMNS = [
        "agg_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "transact_time",
        "is_buyer_maker",
    ]
    INT_COLUMNS = [
        "agg_trade_id",
        "first_trade_id",
        "last_trade_id",
        "transact_time",
    ]
    FLOAT_COLUMNS = [
        "price",
        "quantity",
    ]
    LOGRETURN_COLUMNS = [
        "trade_price_mean",
        "trade_price_min",
        "trade_price_p25",
        "trade_price_p50",
        "trade_price_p75",
        "trade_price_max",
        "buy_price_mean",
        "buy_price_min",
        "buy_price_p25",
        "buy_price_p50",
        "buy_price_p75",
        "buy_price_max",
        "trade_quantity_mean",
        "trade_quantity_min",
        "trade_quantity_p25",
        "trade_quantity_p50",
        "trade_quantity_p75",
        "trade_quantity_max",
        "buy_quantity_mean",
        "buy_quantity_min",
        "buy_quantity_p25",
        "buy_quantity_p50",
        "buy_quantity_p75",
        "buy_quantity_max",
        "trade_rate_mean",
        "buy_rate_mean",
        "buy_count",
        "tickup_count",
        "trade_vwap",
        "buy_vwap",
    ]
    ROLLING_WINDOWS = [4, 8, 16, 32]
    LAG_WINDOWS = [1, 2, 4, 8, 16, 32]
    TEMPORAL_LOG_FEATURE_COLUMNS = [
        "log_return_trade_price_mean",
        "log_return_buy_price_mean",
        "log_return_trade_rate_mean",
        "log_return_buy_rate_mean",
        "log_return_buy_count",
        "log_return_tickup_count",
        "log_return_trade_vwap",
        "log_return_buy_vwap",
        "imbalance_buy_count",
        "imbalance_tickup_count",
    ]

    def __init__(self) -> None:
        super().__init__(
            schema_name="featurestore",
            time_column="create_time",
            intervals=["1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="futures_aggtrades",
                    prefix="futures/um/daily/aggTrades/BTCUSDT",
                )
            ],
            base_start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_aggtrades_{interval}"

    def feature_steps(self) -> list[tuple[str, HistoricalTimescaleBatch.FeatureStep]]:
        return [
            ("derivative", self.derivative),
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
            for current, expected in zip(df.columns, self.AGGTRADE_COLUMNS)
        }
        renamed_df = df.rename(
            {
                current: expected
                for current, expected in zip(df.columns, self.AGGTRADE_COLUMNS)
            }
        )
        recovered_df = pl.DataFrame([recovered_first_row])
        return pl.concat([recovered_df, renamed_df], how="vertical_relaxed")

    def _normalize_historical_df(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        if "transact_time" not in df.columns:
            if df.width != len(self.AGGTRADE_COLUMNS):
                raise ValueError(
                    "Unexpected historical aggTrades schema: "
                    f"expected {len(self.AGGTRADE_COLUMNS)} columns, got {df.width} "
                    f"({df.columns})"
                )
            df = self._recover_headerless_df(df)

        return (
            df.with_columns(
                [
                    *[
                        pl.col(column).cast(pl.Int64, strict=False)
                        for column in self.INT_COLUMNS
                    ],
                    *[
                        pl.col(column).cast(pl.Float64, strict=False)
                        for column in self.FLOAT_COLUMNS
                    ],
                    pl.when(
                        pl.col("is_buyer_maker").cast(pl.Utf8, strict=False).str.to_lowercase().is_in(
                            ["true", "1", "t"]
                        )
                    )
                    .then(True)
                    .when(
                        pl.col("is_buyer_maker").cast(pl.Utf8, strict=False).str.to_lowercase().is_in(
                            ["false", "0", "f"]
                        )
                    )
                    .then(False)
                    .otherwise(pl.col("is_buyer_maker").cast(pl.Boolean, strict=False))
                    .alias("is_buyer_maker"),
                ]
            )
            .with_columns(self._normalize_epoch_to_ms_expr("transact_time"))
            .filter(
                pl.col("transact_time").is_not_null()
                & pl.col("price").is_not_null()
                & pl.col("quantity").is_not_null()
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

    def derivative(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                (pl.col("trade_count") - pl.col("buy_count")).alias("_sell_count"),
                (pl.col("trade_count") - pl.col("tickup_count")).alias("_tickdown_count"),
            ]
        ).with_columns(
            [
                self._safe_div_expr(
                    pl.col("buy_count") - pl.col("_sell_count"),
                    pl.col("trade_count"),
                ).alias("imbalance_buy_count"),
                self._safe_div_expr(
                    pl.col("tickup_count") - pl.col("_tickdown_count"),
                    pl.col("trade_count"),
                ).alias("imbalance_tickup_count"),
            ]
        ).drop(
            [
                "_sell_count",
                "_tickdown_count",
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
            "log_return_trade_price_mean",
            "log_return_buy_price_mean",
            "log_return_trade_vwap",
            "log_return_buy_vwap",
        }
        rolling_std_zscore_columns = [
            "log_return_trade_price_mean",
            "log_return_buy_price_mean",
            "log_return_trade_rate_mean",
            "log_return_buy_rate_mean",
            "log_return_buy_count",
            "log_return_tickup_count",
            "imbalance_buy_count",
            "imbalance_tickup_count",
            "log_return_trade_vwap",
            "log_return_buy_vwap",
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
                        pl.col(f"_{column}_mean_{window}").alias(f"{column}_rolling_mean_{window}")
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
                for column in self.TEMPORAL_LOG_FEATURE_COLUMNS
            ]
        )

    def lag(self, combined_df: pl.DataFrame) -> pl.DataFrame:
        if combined_df.is_empty():
            return combined_df

        return combined_df.with_columns(
            [
                pl.col(column).shift(window).alias(f"{column}_lag_{window}")
                for window in self.LAG_WINDOWS
                for column in self.TEMPORAL_LOG_FEATURE_COLUMNS
            ]
        )

    def aggregation(
        self,
        interval: str,
        timestamps: list[int],
        minio_historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(minio_historical_frames["futures_aggtrades"])
        if df.is_empty():
            return None

        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = (
                df.filter(
                    (pl.col("transact_time") >= window_start)
                    & (pl.col("transact_time") < boundary_ts_ms)
                )
                .sort(["transact_time", "agg_trade_id"])
                .with_columns(
                    (pl.col("transact_time") - pl.col("transact_time").min()).alias(
                        "time_from_window_start"
                    ),
                    (pl.col("price") * pl.col("quantity")).alias("turnover"),
                    (pl.col("price") > pl.col("price").shift(1))
                    .fill_null(False)
                    .alias("is_tickup")
                )
            )

            if window_df.is_empty():
                continue

            buy_window_df = window_df.filter(~pl.col("is_buyer_maker"))

            rows.append(
                {
                    # Time
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),

                    # Price
                    "trade_price_mean": float(window_df["price"].mean() or 0.0),
                    "trade_price_std": float(window_df["price"].std() or 0.0),
                    "trade_price_min": float(window_df["price"].min() or 0.0),
                    "trade_price_p25": float(window_df["price"].quantile(0.25) or 0.0),
                    "trade_price_p50": float(window_df["price"].quantile(0.50) or 0.0),
                    "trade_price_p75": float(window_df["price"].quantile(0.75) or 0.0),
                    "trade_price_max": float(window_df["price"].max() or 0.0),
                    "trade_price_skew": float(window_df["price"].skew() or 0.0),
                    "trade_price_kurtosis": float(window_df["price"].kurtosis() or 0.0),
                    "buy_price_mean": float(buy_window_df["price"].mean() or 0.0),
                    "buy_price_std": float(buy_window_df["price"].std() or 0.0),
                    "buy_price_min": float(buy_window_df["price"].min() or 0.0),
                    "buy_price_p25": float(buy_window_df["price"].quantile(0.25) or 0.0),
                    "buy_price_p50": float(buy_window_df["price"].quantile(0.50) or 0.0),
                    "buy_price_p75": float(buy_window_df["price"].quantile(0.75) or 0.0),
                    "buy_price_max": float(buy_window_df["price"].max() or 0.0),
                    "buy_price_skew": float(buy_window_df["price"].skew() or 0.0),
                    "buy_price_kurtosis": float(buy_window_df["price"].kurtosis() or 0.0),

                    # Quantity
                    "trade_quantity_mean": float(window_df["quantity"].mean() or 0.0),
                    "trade_quantity_std": float(window_df["quantity"].std() or 0.0),
                    "trade_quantity_min": float(window_df["quantity"].min() or 0.0),
                    "trade_quantity_p25": float(window_df["quantity"].quantile(0.25) or 0.0),
                    "trade_quantity_p50": float(window_df["quantity"].quantile(0.50) or 0.0),
                    "trade_quantity_p75": float(window_df["quantity"].quantile(0.75) or 0.0),
                    "trade_quantity_max": float(window_df["quantity"].max() or 0.0),
                    "trade_quantity_skew": float(window_df["quantity"].skew() or 0.0),
                    "trade_quantity_kurtosis": float(window_df["quantity"].kurtosis() or 0.0),
                    "buy_quantity_mean": float(buy_window_df["quantity"].mean() or 0.0),
                    "buy_quantity_std": float(buy_window_df["quantity"].std() or 0.0),
                    "buy_quantity_min": float(buy_window_df["quantity"].min() or 0.0),
                    "buy_quantity_p25": float(buy_window_df["quantity"].quantile(0.25) or 0.0),
                    "buy_quantity_p50": float(buy_window_df["quantity"].quantile(0.50) or 0.0),
                    "buy_quantity_p75": float(buy_window_df["quantity"].quantile(0.75) or 0.0),
                    "buy_quantity_max": float(buy_window_df["quantity"].max() or 0.0),
                    "buy_quantity_skew": float(buy_window_df["quantity"].skew() or 0.0),
                    "buy_quantity_kurtosis": float(buy_window_df["quantity"].kurtosis() or 0.0),

                    # VWAP
                    "trade_vwap": float(
                        (window_df["turnover"].sum() / window_df["quantity"].sum())
                        if window_df["quantity"].sum() not in (None, 0)
                        else 0.0
                    ),
                    "buy_vwap": float(
                        (buy_window_df["turnover"].sum() / buy_window_df["quantity"].sum())
                        if buy_window_df["quantity"].sum() not in (None, 0)
                        else 0.0
                    ),

                    # Rate
                    "trade_rate_mean": float(window_df["transact_time"].diff().mean() or 0.0),
                    "trade_rate_std": float(window_df["transact_time"].diff().std() or 0.0),
                    "buy_rate_mean": float(buy_window_df["transact_time"].diff().mean() or 0.0),
                    "buy_rate_std": float(buy_window_df["transact_time"].diff().std() or 0.0),

                    # Count
                    "trade_count": int(window_df.height),
                    "buy_count": int((~window_df["is_buyer_maker"]).sum()),
                    "tickup_count": int(window_df["is_tickup"].sum()),

                    # Turnover
                    "trade_turnover_std": float(window_df["turnover"].std() or 0.0),
                    "buy_turnover_std": float(
                        buy_window_df["turnover"].std() or 0.0
                    ),

                    # Correlation
                    "trade_corr_price_quantity": float(window_df.select(pl.corr("price", "quantity")).item() or 0.0),
                    "trade_corr_price_time": float(window_df.select(pl.corr("price", "time_from_window_start")).item() or 0.0),
                    "trade_corr_quantity_time": float(window_df.select(pl.corr("quantity", "time_from_window_start")).item() or 0.0),
                    "buy_corr_price_quantity": float(buy_window_df.select(pl.corr("price", "quantity")).item() or 0.0),
                    "buy_corr_price_time": float(buy_window_df.select(pl.corr("price", "time_from_window_start")).item() or 0.0),
                    "buy_corr_quantity_time": float(buy_window_df.select(pl.corr("quantity", "time_from_window_start")).item() or 0.0),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = FuturesAggTradesBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
