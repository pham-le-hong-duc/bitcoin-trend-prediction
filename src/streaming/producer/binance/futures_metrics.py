"""
Binance Futures Metrics Producer
Aggregates data from 5 different APIs into a single metrics record every 5 minutes
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
    TopTraderLongShortRatioAccountsPeriodEnum,
    TopTraderLongShortRatioPositionsPeriodEnum,
    LongShortRatioPeriodEnum,
    TakerBuySellVolumePeriodEnum,
    OpenInterestStatisticsPeriodEnum,
)

from src.streaming.producer.producer import Producer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SYMBOL = os.getenv('SYMBOL', 'BTCUSDT')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'redpanda:9092')
TOPIC = os.getenv('TOPIC', 'binance-futures-metrics')

config = ConfigurationRestAPI(
    api_key='',
    api_secret='',
    base_path=DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
)
client = DerivativesTradingUsdsFutures(config_rest_api=config)


def get_next_5min_time():
    """
    Calculate next 5-minute close time
    Wake at XX:X4:59.999 or XX:X9:59.999
    """
    now = datetime.now(timezone.utc)
    current_minute = now.minute
    
    if current_minute < 4:
        next_close = now.replace(minute=4, second=59, microsecond=999000)
    elif current_minute < 9:
        next_close = now.replace(minute=9, second=59, microsecond=999000)
    elif current_minute < 14:
        next_close = now.replace(minute=14, second=59, microsecond=999000)
    elif current_minute < 19:
        next_close = now.replace(minute=19, second=59, microsecond=999000)
    elif current_minute < 24:
        next_close = now.replace(minute=24, second=59, microsecond=999000)
    elif current_minute < 29:
        next_close = now.replace(minute=29, second=59, microsecond=999000)
    elif current_minute < 34:
        next_close = now.replace(minute=34, second=59, microsecond=999000)
    elif current_minute < 39:
        next_close = now.replace(minute=39, second=59, microsecond=999000)
    elif current_minute < 44:
        next_close = now.replace(minute=44, second=59, microsecond=999000)
    elif current_minute < 49:
        next_close = now.replace(minute=49, second=59, microsecond=999000)
    elif current_minute < 54:
        next_close = now.replace(minute=54, second=59, microsecond=999000)
    elif current_minute < 59:
        next_close = now.replace(minute=59, second=59, microsecond=999000)
    else:
        next_close = (now + timedelta(hours=1)).replace(minute=4, second=59, microsecond=999000)
    
    if next_close <= now:
        next_close = (now + timedelta(minutes=5)).replace(second=59, microsecond=999000)
    
    return next_close


def _sync_fetch_open_interest():
    """Synchronous fetch (to be run in thread)"""
    try:
        response = client.rest_api.open_interest_statistics(
            symbol=SYMBOL,
            period=OpenInterestStatisticsPeriodEnum["PERIOD_5m"].value,
            limit=1,
        )
        data = response.data()
        return data if data else None
    except Exception as e:
        logger.error(f"Error fetching open interest: {e}")
        return None


async def fetch_open_interest():
    """API #1: Open Interest Statistics (runs in thread to avoid blocking)"""
    return await asyncio.to_thread(_sync_fetch_open_interest)


def _sync_fetch_top_trader_ratio_accounts():
    """Synchronous fetch (to be run in thread)"""
    try:
        response = client.rest_api.top_trader_long_short_ratio_accounts(
            symbol=SYMBOL,
            period=TopTraderLongShortRatioAccountsPeriodEnum["PERIOD_5m"].value,
            limit=1,
        )
        data = response.data()
        return data if data else None
    except Exception as e:
        logger.error(f"Error fetching top trader ratio accounts: {e}")
        return None


async def fetch_top_trader_ratio_accounts():
    """API #2: Top Trader Long/Short Ratio (Accounts) (runs in thread to avoid blocking)"""
    return await asyncio.to_thread(_sync_fetch_top_trader_ratio_accounts)


def _sync_fetch_top_trader_ratio_positions():
    """Synchronous fetch (to be run in thread)"""
    try:
        response = client.rest_api.top_trader_long_short_ratio_positions(
            symbol=SYMBOL,
            period=TopTraderLongShortRatioPositionsPeriodEnum["PERIOD_5m"].value,
            limit=1,
        )
        data = response.data()
        return data if data else None
    except Exception as e:
        logger.error(f"Error fetching top trader ratio positions: {e}")
        return None


async def fetch_top_trader_ratio_positions():
    """API #3: Top Trader Long/Short Ratio (Positions) (runs in thread to avoid blocking)"""
    return await asyncio.to_thread(_sync_fetch_top_trader_ratio_positions)


def _sync_fetch_long_short_ratio():
    """Synchronous fetch (to be run in thread)"""
    try:
        response = client.rest_api.long_short_ratio(
            symbol=SYMBOL,
            period=LongShortRatioPeriodEnum["PERIOD_5m"].value,
            limit=1,
        )
        data = response.data()
        return data if data else None
    except Exception as e:
        logger.error(f"Error fetching long/short ratio: {e}")
        return None


async def fetch_long_short_ratio():
    """API #4: Long/Short Ratio (runs in thread to avoid blocking)"""
    return await asyncio.to_thread(_sync_fetch_long_short_ratio)


def _sync_fetch_taker_buy_sell_volume():
    """Synchronous fetch (to be run in thread)"""
    try:
        response = client.rest_api.taker_buy_sell_volume(
            symbol=SYMBOL,
            period=TakerBuySellVolumePeriodEnum["PERIOD_5m"].value,
            limit=1,
        )
        data = response.data()
        return data if data else None
    except Exception as e:
        logger.error(f"Error fetching taker volume: {e}")
        return None


async def fetch_taker_buy_sell_volume():
    """API #5: Taker Buy/Sell Volume (runs in thread to avoid blocking)"""
    return await asyncio.to_thread(_sync_fetch_taker_buy_sell_volume)


def transform_metrics(api1, api2, api3, api4, api5):
    """
    Aggregate data from 5 APIs into schema format
    
    Schema: create_time (string datetime), symbol, sum_open_interest, sum_open_interest_value,
            count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
            count_long_short_ratio, sum_taker_long_short_vol_ratio
    """
    try:
        timestamp = api1['timestamp'] if api1 and 'timestamp' in api1 else None
        
        # Convert Unix timestamp to datetime string (match CSV format: "2026-02-22 00:05:00")
        if timestamp:
            create_time_dt = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
            create_time_str = create_time_dt.strftime('%Y-%m-%d %H:%M:%S')  # ISO format
        else:
            create_time_str = None
        
        return {
            "create_time": create_time_str,
            "symbol": SYMBOL,
            "sum_open_interest": float(api1['sumOpenInterest']) if api1 and 'sumOpenInterest' in api1 else 0,
            "sum_open_interest_value": float(api1['sumOpenInterestValue']) if api1 and 'sumOpenInterestValue' in api1 else 0,
            "count_toptrader_long_short_ratio": float(api2['longShortRatio']) if api2 and 'longShortRatio' in api2 else 0,
            "sum_toptrader_long_short_ratio": float(api3['longShortRatio']) if api3 and 'longShortRatio' in api3 else 0,
            "count_long_short_ratio": float(api4['longShortRatio']) if api4 and 'longShortRatio' in api4 else 0,
            "sum_taker_long_short_vol_ratio": float(api5['buySellRatio']) if api5 and 'buySellRatio' in api5 else 0,
        }
    except Exception as e:
        logger.error(f"Error transforming metrics: {e}")
        return None


async def poll_until_new_metrics(expected_time):
    """
    Poll 5 APIs in parallel until a sufficiently fresh bucket is available.
    
    Returns:
        tuple: (data, request_count, latency)
    """
    start_time = datetime.now(timezone.utc)
    request_count = 0

    while True:
        request_count += 1

        try:
            results = await asyncio.gather(
                fetch_open_interest(),
                fetch_top_trader_ratio_accounts(),
                fetch_top_trader_ratio_positions(),
                fetch_long_short_ratio(),
                fetch_taker_buy_sell_volume(),
            )
            
            api1, api2, api3, api4, api5 = results
            
            # Handle list responses (APIs return arrays)
            if isinstance(api1, list) and api1:
                api1 = api1[0]
            if isinstance(api2, list) and api2:
                api2 = api2[0]
            if isinstance(api3, list) and api3:
                api3 = api3[0]
            if isinstance(api4, list) and api4:
                api4 = api4[0]
            if isinstance(api5, list) and api5:
                api5 = api5[0]
            
            # APIs return dict, not object
            if api1 and isinstance(api1, dict) and 'timestamp' in api1:
                metrics_time = int(api1['timestamp'])

                # Accept the first bucket that is at least as new as the one we expect.
                if metrics_time >= expected_time:
                    latency = (datetime.now(timezone.utc) - start_time).total_seconds()
                    transformed = transform_metrics(api1, api2, api3, api4, api5)
                    return transformed, request_count, latency, metrics_time
        
        except Exception as e:
            logger.error(f"Error polling metrics: {e}")

        await asyncio.sleep(1)


async def main():
    with Producer(bootstrap_servers=BOOTSTRAP_SERVERS, topic=TOPIC) as producer:
        
        while True:
            try:
                next_close = get_next_5min_time()
                
                # Wake up 20 seconds early for long sleep periods (5min cycle has more drift)
                early_wake_time = next_close - timedelta(seconds=20)
                early_sleep = (early_wake_time - datetime.now(timezone.utc)).total_seconds()
                
                if early_sleep > 0:
                    await asyncio.sleep(early_sleep)
                
                # Multi-stage sleep for better precision with lower CPU churn.
                while (next_close - datetime.now(timezone.utc)).total_seconds() > 10:
                    await asyncio.sleep(1)

                while (next_close - datetime.now(timezone.utc)).total_seconds() > 1:
                    await asyncio.sleep(0.1)

                while (next_close - datetime.now(timezone.utc)).total_seconds() > 0.1:
                    await asyncio.sleep(0.01)

                while (next_close - datetime.now(timezone.utc)).total_seconds() > 0.01:
                    await asyncio.sleep(0.001)

                while datetime.now(timezone.utc) < next_close:
                    await asyncio.sleep(0.0001)
                
                # Calculate wake latency (how late we woke up after target time)
                wake_latency = (datetime.now(timezone.utc) - next_close).total_seconds()
                
                # The API timestamp represents the close time of the 5m bucket.
                expected_time = int((next_close + timedelta(milliseconds=1)).timestamp() * 1000)
                
                data, requests, api_latency, metrics_time = await poll_until_new_metrics(
                    expected_time=expected_time,
                )

                producer.send(data)
                actual_bucket_time = datetime.fromtimestamp(
                    metrics_time / 1000, tz=timezone.utc
                )
                logger.info(
                    f"[{actual_bucket_time.strftime('%H:%M:%S')}] | "
                    f"Wake:{wake_latency:.3f}s | API:{api_latency:.3f}s | Requests:{requests}"
                )
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

