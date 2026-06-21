"""
Debug helper for Binance Spot 1m klines.

Usage:
  python -m src.streaming.producer.binance.test_spot_klines
"""
import os
from datetime import datetime, timezone

from binance_sdk_spot.spot import Spot, ConfigurationRestAPI, SPOT_REST_API_PROD_URL
from binance_sdk_spot.rest_api.models import KlinesIntervalEnum


SYMBOL = os.getenv("SYMBOL", "BTCUSDT")

configuration_rest_api = ConfigurationRestAPI(
  api_key=os.getenv("API_KEY", ""),
  api_secret=os.getenv("API_SECRET", ""),
  base_path=os.getenv("BASE_PATH", SPOT_REST_API_PROD_URL),
)
client = Spot(config_rest_api=configuration_rest_api)


def fmt_ts(ts_ms):
  return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def transform_kline(kline_data):
  if not kline_data:
    return None

  return {
    "open_time": int(kline_data[0]),
    "open": float(kline_data[1]),
    "high": float(kline_data[2]),
    "low": float(kline_data[3]),
    "close": float(kline_data[4]),
    "volume": float(kline_data[5]) if kline_data[5] else 0.0,
    "close_time": int(kline_data[6]),
    "quote_volume": float(kline_data[7]) if kline_data[7] else 0.0,
    "count": int(kline_data[8]) if kline_data[8] else 0,
    "taker_buy_volume": float(kline_data[9]) if kline_data[9] else 0.0,
    "taker_buy_quote_volume": float(kline_data[10]) if kline_data[10] else 0.0,
    "ignore": int(kline_data[11]) if len(kline_data) > 11 else 0,
  }


def fetch_recent_klines(limit=2):
  response = client.rest_api.klines(
    symbol=SYMBOL,
    interval=KlinesIntervalEnum["INTERVAL_1m"].value,
    limit=limit,
  )
  return response.data() or []


def main():
  now = datetime.now(timezone.utc)
  current_minute_open = now.replace(second=0, microsecond=0)
  last_closed_open = int(current_minute_open.timestamp() * 1000) - 60_000

  klines = fetch_recent_klines(limit=2)

  print("=" * 72)
  print(f"Spot klines debug for {SYMBOL}")
  print(f"Now UTC:              {now.strftime('%Y-%m-%d %H:%M:%S.%f UTC')}")
  print(f"Expected last closed: {last_closed_open} ({fmt_ts(last_closed_open)})")
  print(f"Rows returned:        {len(klines)}")
  print("=" * 72)

  matched = None
  for idx, row in enumerate(klines):
    open_time = int(row[0])
    close_time = int(row[6])
    status = []
    if open_time == last_closed_open:
      status.append("MATCH_LAST_CLOSED")
      matched = row
    if close_time > int(now.timestamp() * 1000):
      status.append("STILL_OPEN")

    suffix = f" [{' | '.join(status)}]" if status else ""
    print(
      f"Row {idx}: open_time={open_time} ({fmt_ts(open_time)}), "
      f"close_time={close_time} ({fmt_ts(close_time)}){suffix}"
    )
    print(f"  raw={row}")

  print("=" * 72)
  if matched:
    print("Transformed matched row:")
    print(transform_kline(matched))
  else:
    print("No row matched the expected last-closed 1m candle.")
    print("If row 0 is STILL_OPEN, then producer logic should fetch limit=2 and pick the matching row.")


if __name__ == "__main__":
  main()
