"""
Multi-Producer: Run all Binance producers in a single process
Saves memory by sharing one Python interpreter and dependencies
"""
import asyncio
import logging

# Import all producer modules
from src.streaming.producer.binance import (
    futures_aggtrades,
    # futures_fundingrate,
    # futures_indexpriceklines,
    futures_klines,
    # futures_markpriceklines,
    futures_metrics,
    futures_premiumindexklines,
    # spot_aggtrades,
    spot_klines,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
RESTART_DELAY_SECONDS = 15

# Giảm log level cho các thư viện bên ngoài
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('binance_sdk_derivatives_trading_usds_futures').setLevel(logging.WARNING)
logging.getLogger('websocket').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


async def run_with_logging(name, producer_main):
    """Keep one producer alive even if its coroutine crashes."""
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info(f"Starting producer: {name} (attempt {attempt})")
            await producer_main()
            logger.warning(f"Producer {name} exited unexpectedly, restarting soon")
        except Exception as e:
            logger.error(f"Producer {name} failed: {e}", exc_info=True)

        await asyncio.sleep(RESTART_DELAY_SECONDS)


async def main():
    """Run all producers concurrently"""
    # Create tasks for all producers
    tasks = [
        asyncio.create_task(run_with_logging("binance-futures-aggtrades", futures_aggtrades.main)),
        # asyncio.create_task(run_with_logging("binance-spot-aggtrades", spot_aggtrades.main)),
        asyncio.create_task(run_with_logging("binance-futures-klines", futures_klines.main)),
        asyncio.create_task(run_with_logging("binance-spot-klines", spot_klines.main)),
        # asyncio.create_task(run_with_logging("binance-futures-indexpriceklines", futures_indexpriceklines.main)),
        # asyncio.create_task(run_with_logging("binance-futures-markpriceklines", futures_markpriceklines.main)),
        asyncio.create_task(run_with_logging("binance-futures-premiumindexklines", futures_premiumindexklines.main)),
        asyncio.create_task(run_with_logging("binance-futures-metrics", futures_metrics.main)),
        # asyncio.create_task(run_with_logging("binance-futures-fundingrate", futures_fundingrate.main)),
    ]
    
    # Wait for all tasks
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Multi-Producer stopped by user")
