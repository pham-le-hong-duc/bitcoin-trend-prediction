"""
Binance Futures Index Price Klines Producer
Streams index price kline data (1 minute) from Binance Futures to Redpanda
"""
import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
  DerivativesTradingUsdsFutures,
  ConfigurationRestAPI,
  DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
  IndexPriceKlineCandlestickDataIntervalEnum,
)

from src.streaming.producer.producer import Producer

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SYMBOL = os.getenv('SYMBOL', 'btcusdt')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC = os.getenv('TOPIC', 'binance-futures-indexPriceKlines')

# Binance client
configuration_rest_api = ConfigurationRestAPI(
  api_key=os.getenv("API_KEY", ""),
  api_secret=os.getenv("API_SECRET", ""),
  base_path=os.getenv("BASE_PATH", DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL),
)
client = DerivativesTradingUsdsFutures(config_rest_api=configuration_rest_api)


def get_next_kline_close_time():
  """
  Calculate next kline close time (end of current minute)
  Wake at XX:XX:59.999 to get kline immediately when it closes
  """
  now = datetime.now(timezone.utc)
  next_close = now.replace(second=59, microsecond=999000)
  
  if now.second >= 59:
    next_close = (now + timedelta(minutes=1)).replace(second=59, microsecond=999000)
  
  return next_close


def _sync_fetch_index_price_klines():
  """Synchronous fetch of the 2 most recent klines (to be run in thread)"""
  try:
    response = client.rest_api.index_price_kline_candlestick_data(
      pair=SYMBOL,
      interval=IndexPriceKlineCandlestickDataIntervalEnum["INTERVAL_1m"].value,
      limit=2
    )
    
    data = response.data()
    return data if data else []
    
  except Exception as e:
    logger.error(f"Error fetching klines: {e}")
    return []


async def fetch_index_price_klines():
  """Fetch the most recent kline data (runs in thread to avoid blocking)"""
  return await asyncio.to_thread(_sync_fetch_index_price_klines)


def transform_kline(kline_data):
  """
  Transform Binance kline data to match schema.md
  
  Schema: futures-indexPriceKlines.csv
  - open_time, open, high, low, close, volume, close_time, 
   quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
  """
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
      "ignore": int(kline_data[11]) if len(kline_data) > 11 else 0
    }
  except Exception as e:
    logger.error(f"Error transforming kline: {e}, data: {kline_data}")
    return None


async def poll_until_new_data(expected_open_time):
  """
  Poll API liên tục cho đến khi có kline mới
  
  Args:
    expected_open_time: Timestamp (ms) của kline cần lấy
  Returns:
    tuple: (data, request_count, latency)
  """
  start_time = datetime.now(timezone.utc)
  request_count = 0
  
  while True:
    request_count += 1
    
    try:
      klines = await fetch_index_price_klines()
      
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
  """Main producer loop"""
  with Producer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC) as producer:
      
      while True:
        try:
          next_close_time = get_next_kline_close_time()
          
          # Wake up 10 seconds early to compensate for asyncio.sleep() imprecision
          early_wake_time = next_close_time - timedelta(seconds=10)
          early_sleep = (early_wake_time - datetime.now(timezone.utc)).total_seconds()
          
          if early_sleep > 0:
            await asyncio.sleep(early_sleep)
          
          # Multi-stage sleep for better precision with lower CPU churn.
          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 1:
            await asyncio.sleep(0.1)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.1:
            await asyncio.sleep(0.01)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.01:
            await asyncio.sleep(0.001)

          while datetime.now(timezone.utc) < next_close_time:
            await asyncio.sleep(0.0001)
          
          # Calculate wake latency
          wake_latency = (datetime.now(timezone.utc) - next_close_time).total_seconds()
          
          open_time = next_close_time.replace(second=0, microsecond=0)
          close_boundary_time = open_time + timedelta(minutes=1)
          expected_open_time = int(open_time.timestamp() * 1000)
          
          data, requests, api_latency = await poll_until_new_data(
            expected_open_time=expected_open_time
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








