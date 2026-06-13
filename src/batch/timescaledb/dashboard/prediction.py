from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from src.batch.timescaledb.base import INTERVAL_TO_MS
from src.init.timescaledb import KLINE_TIMEFRAMES, METRICS_TIMEFRAMES, SENTIMENT_TIMEFRAMES
from src.utils.timescaledb_client import TimescaleDBClient


MODEL_NAME = "baseline_dashboard_v1"


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _metrics_interval(interval: str) -> str:
    if interval in METRICS_TIMEFRAMES:
        return interval
    return "5m"


def _sentiment_interval(interval: str) -> str:
    if interval in SENTIMENT_TIMEFRAMES:
        return interval
    return "1h"


class DashboardPredictionBatch:
    """Deterministic baseline forecast for the Grafana prediction dashboard."""

    def __init__(self, model_name: str = MODEL_NAME) -> None:
        self.model_name = model_name
        self.client = TimescaleDBClient()

    def close(self) -> None:
        self.client.close()

    def run(self) -> None:
        total_rows = 0
        for interval in KLINE_TIMEFRAMES:
            self.update_actuals(interval)
            row = self.predict_interval(interval)
            if row is None:
                print(f"[{interval}] skipped: not enough dashboard data")
                continue

            df = pl.DataFrame([row])
            total_rows += self.client.upsert_dataframe(
                df,
                table_name=f"predictions_{interval}",
                key_column="target_time",
                schema_name="dashboard",
            )
            print(f"[{interval}] upserted prediction for {row['target_time']}")

        print(f"Dashboard prediction completed: {total_rows} rows upserted")

    def update_actuals(self, interval: str) -> None:
        self.client.execute(
            f"""
            UPDATE dashboard.predictions_{interval} AS prediction
            SET actual_close = price.close,
                prediction_error = price.close - prediction.predicted_close
            FROM dashboard.futures_index_price_klines_{interval} AS price
            WHERE prediction.target_time = price.close_time
              AND prediction.actual_close IS NULL
              AND price.close IS NOT NULL
            """
        )

    def predict_interval(self, interval: str) -> dict | None:
        price_rows = self._latest_price_rows(interval)
        if len(price_rows) < 2:
            return None

        latest = price_rows[0]
        previous = price_rows[1]
        latest_time = latest[0]
        latest_close = float(latest[4])
        previous_close = float(previous[4])

        if previous_close == 0:
            return None

        target_time = latest_time + timedelta(milliseconds=INTERVAL_TO_MS[interval])
        momentum_1 = ((latest_close - previous_close) / previous_close) * 100
        momentum_5 = self._momentum(price_rows, latest_close, offset=5)
        volatility = self._volatility(price_rows)
        metrics_bias = self._metrics_bias(interval, latest_time)
        sentiment_bias = self._sentiment_bias(interval, latest_time)

        predicted_return_pct = _clamp(
            (momentum_1 * 0.65)
            + (momentum_5 * 0.25)
            + metrics_bias
            + sentiment_bias,
            -3.0,
            3.0,
        )
        predicted_close = latest_close * (1 + predicted_return_pct / 100)
        actual_close = self._actual_close(interval, target_time)
        prediction_error = (
            actual_close - predicted_close if actual_close is not None else None
        )
        confidence = _clamp(
            0.55
            + min(abs(predicted_return_pct) / 8, 0.15)
            + (0.05 if metrics_bias != 0 else 0)
            + (0.05 if sentiment_bias != 0 else 0)
            - min(volatility / 20, 0.20),
            0.20,
            0.90,
        )

        return {
            "target_time": target_time,
            "generated_at": datetime.now(timezone.utc),
            "model_name": self.model_name,
            "interval": interval,
            "predicted_close": predicted_close,
            "predicted_return_pct": predicted_return_pct,
            "predicted_direction": self._direction(predicted_return_pct),
            "confidence": confidence,
            "actual_close": actual_close,
            "prediction_error": prediction_error,
        }

    def _latest_price_rows(self, interval: str) -> list[tuple]:
        return self.client.execute(
            f"""
            SELECT close_time, open, high, low, close, volume
            FROM dashboard.futures_index_price_klines_{interval}
            WHERE close IS NOT NULL
            ORDER BY close_time DESC
            LIMIT 25
            """
        ) or []

    def _momentum(self, rows: list[tuple], latest_close: float, offset: int) -> float:
        if len(rows) <= offset:
            return 0.0

        base_close = float(rows[offset][4])
        if base_close == 0:
            return 0.0
        return ((latest_close - base_close) / base_close) * 100

    def _volatility(self, rows: list[tuple]) -> float:
        returns = []
        for current, previous in zip(rows, rows[1:]):
            current_close = float(current[4])
            previous_close = float(previous[4])
            if previous_close:
                returns.append(abs((current_close - previous_close) / previous_close) * 100)
        return sum(returns) / len(returns) if returns else 0.0

    def _metrics_bias(self, interval: str, as_of: datetime) -> float:
        metrics_interval = _metrics_interval(interval)
        rows = self.client.execute(
            f"""
            SELECT count_long_short_ratio, sum_taker_long_short_vol_ratio
            FROM dashboard.futures_metrics_{metrics_interval}
            WHERE create_time <= %s
            ORDER BY create_time DESC
            LIMIT 1
            """,
            (as_of,),
        )
        if not rows:
            return 0.0

        long_short_ratio, taker_ratio = rows[0]
        long_short_ratio = float(long_short_ratio or 1.0)
        taker_ratio = float(taker_ratio or 1.0)
        return _clamp(((long_short_ratio - 1) * 0.12) + ((taker_ratio - 1) * 0.08), -0.5, 0.5)

    def _sentiment_bias(self, interval: str, as_of: datetime) -> float:
        sentiment_interval = _sentiment_interval(interval)
        rows = self.client.execute(
            f"""
            SELECT score, confidence, pct_positive, pct_negative
            FROM dashboard.sentiment_{sentiment_interval}
            WHERE create_time <= %s
            ORDER BY create_time DESC
            LIMIT 1
            """,
            (as_of,),
        )
        if not rows:
            return 0.0

        score, confidence, pct_positive, pct_negative = rows[0]
        score = float(score or 0.0)
        confidence = float(confidence or 0.0)
        spread = float(pct_positive or 0.0) - float(pct_negative or 0.0)
        return _clamp((score * 0.18) + (spread * 0.12) + (confidence * 0.04), -0.5, 0.5)

    def _actual_close(self, interval: str, target_time: datetime) -> float | None:
        rows = self.client.execute(
            f"""
            SELECT close
            FROM dashboard.futures_index_price_klines_{interval}
            WHERE close_time = %s
            LIMIT 1
            """,
            (target_time,),
        )
        if not rows:
            return None
        return float(rows[0][0])

    def _direction(self, predicted_return_pct: float) -> str:
        if predicted_return_pct > 0.05:
            return "UP"
        if predicted_return_pct < -0.05:
            return "DOWN"
        return "FLAT"


def main() -> None:
    batch = DashboardPredictionBatch()
    try:
        batch.run()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
