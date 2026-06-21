"""
Realtime TimescaleDB consumer for Binance futures metrics.

Flow:
- Read futures metrics snapshots from Redpanda
- Keep recent history in RAM
- Aggregate on UTC boundaries for 5m/15m/1h/4h/1d
- Upsert into fixed dashboard tables
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from .base import Consumer


class FuturesMetricsConsumer(Consumer):
    """Realtime dashboard consumer for futures metrics snapshots."""
    SOURCE_INTERVAL_MS = 5 * 60 * 1000
    VALUE_COLUMNS = [
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]

    def __init__(self, **kwargs):
        super().__init__(
            topic="binance-futures-metrics",
            data_type="futures/um/daily/metrics/BTCUSDT",
            symbol="btcusdt",
            timestamp_field="bucket_create_time",
            intervals=["5m", "15m", "1h", "4h", "1d"],
            window_timestamp_mode="end",
            dedupe_columns=["bucket_create_time"],
            warmup_messages=50,
            schema_name="dashboard",
            key_column="create_time",
            **kwargs,
        )

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

    def aggregate_window(self, df_window, window_ts, interval):
        """
        Aggregate one window of futures metrics.

        Assumption: futures metrics are snapshot-like records, so the latest
        record inside the window is the most representative value for that
        boundary.
        """
        try:
            df_sorted = df_window.sort("bucket_create_time")
            last_row = df_sorted.tail(1)

            if len(last_row) == 0:
                return None

            result = last_row.select(
                [
                    pl.col("sum_open_interest"),
                    pl.col("sum_open_interest_value"),
                    pl.col("count_toptrader_long_short_ratio"),
                    pl.col("sum_toptrader_long_short_ratio"),
                    pl.col("count_long_short_ratio"),
                    pl.col("sum_taker_long_short_vol_ratio"),
                ]
            ).to_dict(as_series=False)

            row = {}
            for key, value in result.items():
                row[key] = value[0] if isinstance(value, list) and value else value

            row["create_time"] = datetime.fromtimestamp(window_ts / 1000, tz=timezone.utc)
            return pl.DataFrame([row])
        except Exception as exc:
            print(f"Error in aggregate_window ({interval}): {exc}")
            return None

    def resolve_table_target(self, interval):
        return ("dashboard", f"futures_metrics_{interval}")

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
