from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List

import polars as pl

from src.utils.s3_client import MinIOWriter
from src.utils.timescaledb_client import TimescaleDBClient


INTERVAL_TO_MS: Dict[str, int] = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}


@dataclass(frozen=True)
class HistoricalSource:
    name: str
    prefix: str
    file_pattern: str = "daily"
    file_prefix: str | None = None


class HistoricalTimescaleBatch(ABC):
    SECOND_THRESHOLD = 100_000_000_000
    MICROSECOND_THRESHOLD = 10_000_000_000_000

    def __init__(
        self,
        schema_name: str,
        time_column: str,
        intervals: List[str],
        historical_sources: List[HistoricalSource],
        base_start_date: datetime,
        minio_bucket: str = "binance",
        minio_endpoint: str = "minio:9000",
        minio_access_key: str = "admin",
        minio_secret_key: str = "password",
        minio_secure: bool = False,
    ) -> None:
        self.schema_name = schema_name
        self.time_column = time_column
        self.intervals = intervals
        self.historical_sources = historical_sources
        self.base_start_date = base_start_date.astimezone(timezone.utc)
        self.minio_bucket = minio_bucket
        self._minio_client = MinIOWriter(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=minio_bucket,
            secure=minio_secure,
        )
        self._ts_client = TimescaleDBClient()
        self.missing_ts: Dict[str, Dict[str, set[int]]] = {
            interval: defaultdict(set) for interval in self.intervals
        }

    @staticmethod
    def _drop_embedded_header_rows(df: pl.DataFrame) -> pl.DataFrame:
        """Drop rows whose values are exactly the column names repeated."""
        if df.is_empty():
            return df

        header_checks = [
            pl.col(column).cast(pl.Utf8, strict=False).eq(pl.lit(column))
            for column in df.columns
        ]
        if not header_checks:
            return df

        return (
            df.with_columns(pl.all_horizontal(header_checks).alias("_is_embedded_header"))
            .filter(~pl.col("_is_embedded_header"))
            .drop("_is_embedded_header")
        )

    @classmethod
    def _normalize_epoch_to_ms_expr(cls, column_name: str, alias: str | None = None) -> pl.Expr:
        target_name = alias or column_name
        return (
            pl.when(pl.col(column_name).abs() >= cls.MICROSECOND_THRESHOLD)
            .then((pl.col(column_name) // 1000).cast(pl.Int64))
            .when(pl.col(column_name).abs() < cls.SECOND_THRESHOLD)
            .then((pl.col(column_name) * 1000).cast(pl.Int64))
            .otherwise(pl.col(column_name).cast(pl.Int64))
            .alias(target_name)
        )

    @abstractmethod
    def table_name(self, interval: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: List[int],
        historical_frames: Dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        raise NotImplementedError

    def normalize_historical_frame(self, source_name: str, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def close(self) -> None:
        self._ts_client.close()

    def _full_table_name(self, interval: str) -> str:
        return f"{self.schema_name}.{self.table_name(interval)}"

    def _align_boundary(self, ts_ms: int, interval_ms: int) -> int:
        return ((ts_ms + interval_ms - 1) // interval_ms) * interval_ms

    def _group_by_date(self, interval: str, timestamps: Iterable[int]) -> None:
        for ts_ms in timestamps:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            self.missing_ts[interval][dt.strftime("%Y-%m-%d")].add(ts_ms)

    def _normalize_timestamp_value(self, value) -> int:
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        if isinstance(value, date):
            dt = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        raise TypeError(f"Unsupported timestamp value: {type(value)}")

    def _expected_timestamps(self, interval: str) -> set[int]:
        interval_ms = INTERVAL_TO_MS[interval]
        start_ms = self._align_boundary(
            int(self.base_start_date.timestamp() * 1000),
            interval_ms,
        )
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        end_ms = (now_ms // interval_ms) * interval_ms
        return set(range(start_ms, end_ms + interval_ms, interval_ms))

    def detect_gaps(self, interval: str) -> dict:
        full_table_name = self._full_table_name(interval)
        query = (
            f'SELECT "{self.time_column}" '
            f"FROM {full_table_name} "
            f'WHERE "{self.time_column}" IS NOT NULL '
            f'ORDER BY "{self.time_column}"'
        )

        try:
            result = self._ts_client.execute(query)
            existing_ts = {self._normalize_timestamp_value(row[0]) for row in result} if result else set()
            expected_ts = self._expected_timestamps(interval)
            missing_ts = expected_ts - existing_ts
            self._group_by_date(interval, missing_ts)
            return {"success": True, "missing": len(missing_ts), "existing": len(existing_ts)}
        except Exception as exc:
            return {"error": str(exc)}

    def propagate_missing_timestamps(self) -> None:
        ordered_intervals = sorted(self.intervals, key=lambda item: INTERVAL_TO_MS[item])
        expected_by_interval = {
            interval: self._expected_timestamps(interval)
            for interval in ordered_intervals
        }

        for source_index, source_interval in enumerate(ordered_intervals[:-1]):
            source_missing = set()
            for values in self.missing_ts[source_interval].values():
                source_missing.update(values)

            if not source_missing:
                continue

            for target_interval in ordered_intervals[source_index + 1 :]:
                target_ms = INTERVAL_TO_MS[target_interval]
                affected = {
                    self._align_boundary(ts_ms, target_ms)
                    for ts_ms in source_missing
                }
                affected &= expected_by_interval[target_interval]
                self._group_by_date(target_interval, affected)

    def detect_all_gaps_and_propagate(self) -> Dict[str, Dict[str, set[int]]]:
        print(f"{'=' * 60}")
        print(f"HISTORICAL GAP DETECTION: {self.schema_name}")
        print(f"{'=' * 60}")

        for interval in self.intervals:
            result = self.detect_gaps(interval)
            if "error" in result:
                print(f"[{interval}] ERROR: {result['error']}")

        self.propagate_missing_timestamps()

        for interval in self.intervals:
            missing_count = sum(len(values) for values in self.missing_ts[interval].values())
            if missing_count:
                print(f"  {interval}: {missing_count} gaps")

        return self.missing_ts

    def _source_path(self, source: HistoricalSource, target_date: date) -> str:
        if source.file_pattern == "monthly":
            period_str = target_date.strftime("%Y-%m")
        else:
            period_str = target_date.strftime("%Y-%m-%d")

        filename = (
            f"{source.file_prefix}_{period_str}.parquet"
            if source.file_prefix
            else f"{period_str}.parquet"
        )
        return f"{source.prefix}/{filename}"

    def _window_dates(self, source: HistoricalSource, current_date: date) -> list[date]:
        if source.file_pattern == "monthly":
            current_month = current_date.replace(day=1)
            previous_month_last_day = current_month - timedelta(days=1)
            previous_month = previous_month_last_day.replace(day=1)
            return [previous_month, current_month]
        return [current_date - timedelta(days=1), current_date]

    def _load_source_window(self, source: HistoricalSource, current_date: date) -> pl.DataFrame | None:
        frames = []

        for target_date in self._window_dates(source, current_date):
            path = self._source_path(source, target_date)
            df = self._minio_client.read_parquet(path)
            if df is not None and not df.is_empty():
                cleaned_df = self._drop_embedded_header_rows(df)
                if not cleaned_df.is_empty():
                    frames.append(self.normalize_historical_frame(source.name, cleaned_df))

        if not frames:
            return None
        if len(frames) == 1:
            return frames[0]
        return pl.concat(frames, how="vertical_relaxed")

    def _load_historical_frames(self, current_date: date) -> Dict[str, pl.DataFrame]:
        frames: Dict[str, pl.DataFrame] = {}
        for source in self.historical_sources:
            df = self._load_source_window(source, current_date)
            if df is not None and not df.is_empty():
                frames[source.name] = df
        return frames

    def fill_gaps(self) -> None:
        print(f"{'=' * 60}")
        print(f"HISTORICAL GAP FILL: {self.schema_name}")
        print(f"{'=' * 60}")

        for interval in self.intervals:
            date_groups = self.missing_ts.get(interval, {})
            if not date_groups:
                continue

            print(f"[{interval}] Processing {len(date_groups)} days")
            for date_str in sorted(date_groups.keys()):
                current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                timestamps = sorted(date_groups[date_str])
                historical_frames = self._load_historical_frames(current_date)

                if not historical_frames:
                    print(f"  {date_str}: no historical parquet found")
                    continue

                result_df = self.aggregate_timestamps(interval, timestamps, historical_frames)
                if result_df is None or result_df.is_empty():
                    print(f"  {date_str}: aggregation returned no rows")
                    continue

                rows = self._ts_client.upsert_dataframe(
                    result_df,
                    self.table_name(interval),
                    key_column=self.time_column,
                    schema_name=self.schema_name,
                )
                print(f"  {date_str}: upserted {rows} rows")

        print(f"{'=' * 60}")
        print("HISTORICAL GAP FILL COMPLETED")
        print(f"{'=' * 60}")
