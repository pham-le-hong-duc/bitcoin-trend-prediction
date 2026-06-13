from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from src.batch.timescaledb.base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


class FuturesIndexPriceKlinesBatch(HistoricalTimescaleBatch):
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

    def __init__(self) -> None:
        super().__init__(
            schema_name="dashboard",
            time_column="close_time",
            intervals=["1m", "5m", "15m", "1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="index_price_klines",
                    prefix="futures/um/daily/indexPriceKlines/BTCUSDT/1m",
                )
            ],
            base_start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            minio_bucket="binance",
        )

    def table_name(self, interval: str) -> str:
        return f"futures_index_price_klines_{interval}"

    def _align_boundary(self, ts_ms: int, interval_ms: int) -> int:
        # close_time is stored on the exact UTC boundary for the bucket end
        # (for example 10:05:00 for a 5m bucket ending at 10:05).
        return ((ts_ms + interval_ms - 1) // interval_ms) * interval_ms

    def _expected_timestamps(self, interval: str) -> set[int]:
        interval_ms = INTERVAL_TO_MS[interval]
        start_open_ms = (int(self.base_start_date.timestamp() * 1000) // interval_ms) * interval_ms
        start_close_ms = start_open_ms + interval_ms
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_bucket_open_ms = (now_ms // interval_ms) * interval_ms
        last_closed_close_ms = current_bucket_open_ms

        if last_closed_close_ms < start_close_ms:
            return set()

        return set(range(start_close_ms, last_closed_close_ms + interval_ms, interval_ms))

    def _open_boundary_from_close(self, close_ts_ms: int, interval_ms: int) -> int:
        return close_ts_ms - interval_ms

    def propagate_missing_timestamps(self) -> None:
        super().propagate_missing_timestamps()
        for interval in self.intervals:
            expected_ts = self._expected_timestamps(interval)
            for date_key in list(self.missing_ts[interval].keys()):
                self.missing_ts[interval][date_key].intersection_update(expected_ts)
                if not self.missing_ts[interval][date_key]:
                    del self.missing_ts[interval][date_key]

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

        return df.with_columns(
            [
                pl.col("open_time").cast(pl.Int64),
                pl.col("close_time").cast(pl.Int64),
                pl.col("count").cast(pl.Int64),
                *[pl.col(column).cast(pl.Float64, strict=False) for column in self.FLOAT_COLUMNS],
            ]
        )

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return self._normalize_historical_df(df)

    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: list[int],
        historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        df = self._normalize_historical_df(historical_frames["index_price_klines"])
        if df.is_empty():
            return None

        df = df.with_columns(
            [
                # Some raw files carry slightly inconsistent open_time/close_time values.
                # Snap the source candle to the canonical 1m bucket first, then derive
                # the exact close boundary from that normalized open time.
                ((pl.col("open_time") // self.SOURCE_INTERVAL_MS) * self.SOURCE_INTERVAL_MS).alias(
                    "bucket_open_time"
                ),
            ]
        ).with_columns(
            [
                (pl.col("bucket_open_time") + self.SOURCE_INTERVAL_MS).alias("effective_close_time"),
            ]
        )
        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for close_boundary_ts_ms in timestamps:
            open_boundary_ts_ms = self._open_boundary_from_close(close_boundary_ts_ms, interval_ms)
            window_end = open_boundary_ts_ms + interval_ms
            window_df = df.filter(
                (pl.col("bucket_open_time") >= open_boundary_ts_ms)
                & (pl.col("bucket_open_time") < window_end)
            ).sort("bucket_open_time")

            if window_df.is_empty():
                continue

            rows.append(
                {
                    "open_time": datetime.fromtimestamp(open_boundary_ts_ms / 1000, tz=timezone.utc),
                    "close_time": datetime.fromtimestamp(close_boundary_ts_ms / 1000, tz=timezone.utc),
                    "open": float(window_df["open"][0]),
                    "high": float(window_df["high"].max()),
                    "low": float(window_df["low"].min()),
                    "close": float(window_df["close"][-1]),
                    "volume": float(window_df["volume"].sum()),
                    "quote_volume": float(window_df["quote_volume"].sum()),
                    "taker_buy_volume": float(window_df["taker_buy_volume"].sum()),
                    "taker_buy_quote_volume": float(window_df["taker_buy_quote_volume"].sum()),
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = FuturesIndexPriceKlinesBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
