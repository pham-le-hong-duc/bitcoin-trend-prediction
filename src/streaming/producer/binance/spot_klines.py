"""
Binance Spot Klines Producer
Streams BTCUSDT 1m spot kline data from Binance Spot to Redpanda
"""
import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from binance_sdk_spot.spot import Spot, ConfigurationRestAPI, SPOT_REST_API_PROD_URL
from binance_sdk_spot.rest_api.models import KlinesIntervalEnum

from src.streaming.producer.producer import Producer


logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SYMBOL = os.getenv('SYMBOL', 'BTCUSDT')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC = os.getenv('TOPIC', 'binance-spot-klines')

configuration_rest_api = ConfigurationRestAPI(
  api_key=os.getenv("API_KEY", ""),
  api_secret=os.getenv("API_SECRET", ""),
  base_path=os.getenv("BASE_PATH", SPOT_REST_API_PROD_URL),
)
client = Spot(config_rest_api=configuration_rest_api)


def get_next_kline_close_time():
  """Wake exactly at the 1m close boundary."""
  now = datetime.now(timezone.utc)
  next_close = now.replace(second=59, microsecond=999000)

  if now.second >= 59:
    next_close = (now + timedelta(minutes=1)).replace(second=59, microsecond=999000)

  return next_close


def _sync_fetch_spot_klines():
  """Synchronous fetch of the 2 most recent klines (to be run in thread)."""
  try:
    response = client.rest_api.klines(
      symbol=SYMBOL,
      interval=KlinesIntervalEnum["INTERVAL_1m"].value,
      limit=2,
    )

    data = response.data()
    return data if data else []

  except Exception as e:
    logger.error(f"Error fetching klines: {e}")
    return []


async def fetch_spot_klines():
  """Fetch the most recent spot klines in a thread to avoid blocking."""
  return await asyncio.to_thread(_sync_fetch_spot_klines)


def transform_kline(kline_data):
  """Transform Binance kline payload into the shared 12-column schema."""
  if not kline_data:
    return None

  try:
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
  except Exception as e:
    logger.error(f"Error transforming kline: {e}, data: {kline_data}")
    return None


async def poll_until_new_data(expected_open_time):
  """Poll until the newly closed 1m kline is available."""
  start_time = datetime.now(timezone.utc)
  request_count = 0

  while True:
    request_count += 1

    try:
      klines = await fetch_spot_klines()

      if klines:
        for kline in klines:
          kline_open_time = int(kline[0])
          if kline_open_time == expected_open_time:
            latency = (datetime.now(timezone.utc) - start_time).total_seconds()
            transformed = transform_kline(kline)
            return transformed, request_count, latency

    except Exception as e:
      logger.error(f"Error in poll loop: {e}")
    
    await asyncio.sleep(1)


async def main():
  """Main producer loop."""
  with Producer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC) as producer:
    while True:
      try:
        next_close_time = get_next_kline_close_time()

        early_wake_time = next_close_time - timedelta(seconds=10)
        early_sleep = (early_wake_time - datetime.now(timezone.utc)).total_seconds()
        if early_sleep > 0:
          await asyncio.sleep(early_sleep)

        while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 1:
          await asyncio.sleep(0.1)

        while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.1:
          await asyncio.sleep(0.01)

        while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.01:
          await asyncio.sleep(0.001)

        while datetime.now(timezone.utc) < next_close_time:
          await asyncio.sleep(0.0001)

        wake_latency = (datetime.now(timezone.utc) - next_close_time).total_seconds()

        open_time = next_close_time.replace(second=0, microsecond=0)
        close_boundary_time = open_time + timedelta(minutes=1)
        expected_open_time = int(open_time.timestamp() * 1000)

        data, requests, api_latency = await poll_until_new_data(
          expected_open_time=expected_open_time,
        )

        if data:
          producer.send(data)
          logger.info(
            f"[{close_boundary_time.strftime('%H:%M:%S')}] | "
            f"Wake:{wake_latency:.3f}s | API:{api_latency:.3f}s | Requests:{requests}"
          )
        else:
          logger.warning(f"Failed to get kline after {requests} requests")

      except KeyboardInterrupt:
        logger.info("Shutting down...")
        break
      except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
        await asyncio.sleep(5)


if __name__ == "__main__":
  try:
    asyncio.run(main())
  except KeyboardInterrupt:
    logger.info("Producer stopped by user")
