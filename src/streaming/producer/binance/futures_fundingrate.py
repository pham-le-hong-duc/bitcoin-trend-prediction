"""
Binance Futures Funding Rate Producer
Streams funding rate data (every 8 hours: 00:00, 08:00, 16:00 UTC) from Binance Futures to Redpanda
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

from src.streaming.producer.producer import Producer

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SYMBOL = os.getenv('SYMBOL', 'BTCUSDT')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC = os.getenv('TOPIC', 'binance-futures-fundingRate')

# Binance client
configuration_rest_api = ConfigurationRestAPI(
  api_key=os.getenv("API_KEY", ""),
  api_secret=os.getenv("API_SECRET", ""),
  base_path=os.getenv("BASE_PATH", DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL),
)
client = DerivativesTradingUsdsFutures(config_rest_api=configuration_rest_api)


def get_next_funding_close_time():
  """
  Calculate when to wake up (just before funding time)
  Funding at: 00:00, 08:00, 16:00 UTC
  Wake at: 23:59:59.999, 07:59:59.999, 15:59:59.999
  """
  now = datetime.now(timezone.utc)
  funding_hours = [0, 8, 16]
  
  for i, hour in enumerate(funding_hours):
    if now.hour < hour or (now.hour == hour and now.minute == 0 and now.second < 30):
      # Wake 1 second before funding time
      if hour == 0:
        return now.replace(hour=23, minute=59, second=59, microsecond=999000)
      else:
        prev_hour = funding_hours[i-1] if i > 0 else 23
        return now.replace(hour=prev_hour + 8 - 1, minute=59, second=59, microsecond=999000) if prev_hour != 23 else now.replace(hour=23, minute=59, second=59, microsecond=999000)
  
  # Default: wake at 23:59:59.999 for next 00:00
  return now.replace(hour=23, minute=59, second=59, microsecond=999000)


def _sync_fetch_funding_rate():
  """Synchronous fetch (to be run in thread)"""
  try:
    response = client.rest_api.get_funding_rate_history(
      symbol=SYMBOL,
      limit=1
    )
    
    data = response.data()
    if data and len(data) > 0:
      return data[0]
    return None
    
  except Exception as e:
    logger.error(f"Error fetching funding rate: {e}")
    return None


async def fetch_funding_rate():
  """Fetch latest kline data (runs in thread to avoid blocking)"""
  return await asyncio.to_thread(_sync_fetch_funding_rate)


def transform_funding_rate(funding_data):
  """
  Transform Binance funding rate data to match schema.md
  
  Schema: futures-fundingRate.csv
  - calc_time, funding_interval_hours, last_funding_rate
  """
  if not funding_data:
    return None
  
  try:
    return {
      "calc_time": int(funding_data.funding_time),
      "funding_interval_hours": 8,
      "last_funding_rate": float(funding_data.funding_rate)
    }
  except Exception as e:
    logger.error(f"Error transforming funding rate: {e}, data: {funding_data}")
    return None


async def poll_until_new_funding(expected_funding_time, poll_duration=60):
  """
  Poll API continuously until new funding rate is available
  
  Args:
    expected_funding_time: Timestamp (ms) of funding rate to fetch
    poll_duration: Maximum seconds to poll
  Returns:
    tuple: (data, request_count, latency)
  """
  start_time = datetime.now(timezone.utc)
  request_count = 0
  
  while (datetime.now(timezone.utc) - start_time).total_seconds() < poll_duration:
    request_count += 1
    
    try:
      funding = await fetch_funding_rate()
      
      if funding:
        funding_calc_time = int(funding.funding_time)
        if expected_funding_time <= funding_calc_time < expected_funding_time + 1000:
          latency = (datetime.now(timezone.utc) - start_time).total_seconds()
          transformed = transform_funding_rate(funding)
          return transformed, request_count, latency
        
    except Exception as e:
      logger.error(f"Error in poll loop: {e}")
  logger.warning(f" Timeout after {poll_duration}s, {request_count} requests")
  return None, request_count, None


async def main():
  """Main producer loop"""
  with Producer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC) as producer:
      
      while True:
        try:
          # Wake just before funding time
          next_close_time = get_next_funding_close_time()
          
          # Calculate funding time (next hour:00:00)
          funding_time = (next_close_time + timedelta(seconds=1)).replace(second=0, microsecond=0)
          expected_funding_time = int(funding_time.timestamp() * 1000)
          
          # Wake up 20 minutes early for very long sleep periods (8h cycle has significant drift)
          early_wake_time = next_close_time - timedelta(minutes=20)
          early_sleep = (early_wake_time - datetime.now(timezone.utc)).total_seconds()
          
          if early_sleep > 0:
            await asyncio.sleep(early_sleep)
          
          # Multi-stage sleep for better precision with lower CPU churn.
          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 60:
            await asyncio.sleep(10)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 10:
            await asyncio.sleep(1)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 1:
            await asyncio.sleep(0.1)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.1:
            await asyncio.sleep(0.01)

          while (next_close_time - datetime.now(timezone.utc)).total_seconds() > 0.01:
            await asyncio.sleep(0.001)

          while datetime.now(timezone.utc) < next_close_time:
            await asyncio.sleep(0.0001)
          
          # Calculate wake latency (how late we woke up after target time)
          wake_latency = (datetime.now(timezone.utc) - next_close_time).total_seconds()
          
          data, requests, api_latency = await poll_until_new_funding(
            expected_funding_time=expected_funding_time,
            poll_duration=60,
            )
          
          if data:
            producer.send(data)
            logger.info(
              f"[{funding_time.strftime('%H:%M:%S')}] | "
              f"Wake:{wake_latency:.3f}s | API:{api_latency:.3f}s | Requests:{requests}"
            )
          else:
            logger.warning(f"Failed to get funding rate after {requests} requests")
          
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










