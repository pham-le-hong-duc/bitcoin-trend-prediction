"""
Realtime TimescaleDB consumer for Binance futures index price klines.

Flow:
- Read 1m index price klines from Redpanda
- Keep recent history in RAM
- Aggregate on UTC boundaries for 1m/5m/15m/1h/4h/1d
- Upsert into fixed dashboard tables
"""

from __future__ import annotations

from datetime import datetime, timezone
import re

import polars as pl

from src.streaming.consumer.timescaledb.consumer import Consumer


class FuturesIndexPriceKlinesConsumer(Consumer):
    """Realtime dashboard consumer for futures index price klines."""
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
    SOURCE_INTERVAL_MS = 60 * 1000

    def __init__(self, **kwargs):
        super().__init__(
            topic="binance-futures-indexPriceKlines",
            data_type="futures/um/daily/indexPriceKlines/BTCUSDT/1m",
            symbol="btcusdt",
            timestamp_field="effective_close_time",
            intervals=["1m", "5m", "15m", "1h", "4h", "1d"],
            window_timestamp_mode="end",
            dedupe_columns=["effective_close_time"],
            warmup_messages=10,
            schema_name="dashboard",
            key_column="open_time",
            **kwargs,
        )

    def transform_record(self, record, topic):
        normalized = dict(record)
        normalized["open_time"] = int(record["open_time"])
        normalized["close_time"] = int(record["close_time"])
        normalized["open"] = float(record["open"])
        normalized["high"] = float(record["high"])
        normalized["low"] = float(record["low"])
        normalized["close"] = float(record["close"])

        bucket_open_time = (normalized["open_time"] // self.SOURCE_INTERVAL_MS) * self.SOURCE_INTERVAL_MS
        normalized["bucket_open_time"] = bucket_open_time
        normalized["effective_close_time"] = bucket_open_time + self.SOURCE_INTERVAL_MS
        return normalized

    def transform_historical_df(self, df, source_name):
        if df.is_empty():
            return df
        if "open_time" not in df.columns and df.width == len(self.KLINE_COLUMNS):
            recovered_first_row = {
                expected: self.DUPLICATED_SUFFIX_PATTERN.sub("", current)
                for current, expected in zip(df.columns, self.KLINE_COLUMNS)
            }
            df = pl.concat(
                [
                    pl.DataFrame([recovered_first_row]),
                    df.rename(
                        {
                            current: expected
                            for current, expected in zip(df.columns, self.KLINE_COLUMNS)
                        }
                    ),
                ],
                how="vertical_relaxed",
            )
        return (
            df.with_columns(
                (
                    (pl.col("open_time").cast(pl.Int64) // self.SOURCE_INTERVAL_MS)
                    * self.SOURCE_INTERVAL_MS
                ).alias("bucket_open_time")
            )
            .with_columns(
                [
                    pl.col("close_time").cast(pl.Int64),
                    pl.col("open").cast(pl.Float64, strict=False),
                    pl.col("high").cast(pl.Float64, strict=False),
                    pl.col("low").cast(pl.Float64, strict=False),
                    pl.col("close").cast(pl.Float64, strict=False),
                    (pl.col("bucket_open_time") + self.SOURCE_INTERVAL_MS).alias("effective_close_time"),
                ]
            )
        )

    def aggregate_window(self, df_window, window_ts, interval):
        """Aggregate one window of 1m klines into a single OHLC row."""
        try:
            df_sorted = df_window.sort("bucket_open_time")
            result = df_sorted.select(
                [
                    pl.col("open").first().alias("open"),
                    pl.col("high").max().alias("high"),
                    pl.col("low").min().alias("low"),
                    pl.col("close").last().alias("close"),
                ]
            ).to_dict(as_series=False)

            row = {}
            for key, value in result.items():
                row[key] = value[0] if isinstance(value, list) and value else value

            if row.get("open") is None:
                return None

            interval_ms = self._get_window_size_ms(interval)
            row["open_time"] = datetime.fromtimestamp(
                (window_ts - interval_ms) / 1000,
                tz=timezone.utc,
            )
            row["close_time"] = datetime.fromtimestamp((window_ts - 1) / 1000, tz=timezone.utc)
            return pl.DataFrame([row])
        except Exception as exc:
            print(f"Error in aggregate_window ({interval}): {exc}")
            return None

    def resolve_table_target(self, interval):
        return ("dashboard", f"futures_index_price_klines_{interval}")


def main():
    consumer = FuturesIndexPriceKlinesConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
