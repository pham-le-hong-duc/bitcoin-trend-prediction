"""
Multi-Consumer Manager for MinIO Sink
Runs all 7 Binance consumers concurrently in a single process
"""

import asyncio
import logging

# Import all consumer modules
from src.streaming.consumer.minio.binance import (
    futures_aggtrades,
    futures_fundingrate,
    futures_indexpriceklines,
    futures_markpriceklines,
    futures_metrics,
    futures_premiumindexklines,
    spot_aggtrades,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce log level for external libraries
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)


async def run_consumer(name, consumer_main):
    """Run a consumer with error handling"""
    try:
        logger.info(f"Starting consumer: {name}")
        # Run synchronous consumer in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, consumer_main)
    except Exception as e:
        logger.error(f"Consumer {name} failed: {e}", exc_info=True)


async def main():
    """Run all consumers concurrently"""
    # Create tasks for all consumers
    tasks = [
        asyncio.create_task(run_consumer("binance-futures-aggtrades", futures_aggtrades.main)),
        asyncio.create_task(run_consumer("binance-spot-aggtrades", spot_aggtrades.main)),
        asyncio.create_task(run_consumer("binance-futures-indexpriceklines", futures_indexpriceklines.main)),
        asyncio.create_task(run_consumer("binance-futures-markpriceklines", futures_markpriceklines.main)),
        asyncio.create_task(run_consumer("binance-futures-premiumindexklines", futures_premiumindexklines.main)),
        asyncio.create_task(run_consumer("binance-futures-metrics", futures_metrics.main)),
        asyncio.create_task(run_consumer("binance-futures-fundingrate", futures_fundingrate.main)),
    ]
    
    # Wait for all tasks
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
