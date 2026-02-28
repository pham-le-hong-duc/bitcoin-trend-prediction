"""
Multi-Producer: Run all 7 Binance producers in a single process
Saves memory by sharing one Python interpreter and dependencies
"""
import asyncio
import logging

# Import all producer modules
import binance_futures_aggtrades
import binance_spot_aggtrades
import binance_futures_indexpriceklines
import binance_futures_markpriceklines
import binance_futures_premiumindexklines
import binance_futures_metrics
import binance_futures_fundingrate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Giảm log level cho các thư viện bên ngoài
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('binance_sdk_derivatives_trading_usds_futures').setLevel(logging.WARNING)
logging.getLogger('websocket').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


async def run_with_logging(name, producer_main):
    """Wrapper to run a producer with error handling"""
    try:
        logger.info(f"Starting producer: {name}")
        await producer_main()
    except Exception as e:
        logger.error(f"Producer {name} failed: {e}", exc_info=True)


async def main():
    """Run all producers concurrently"""
    # Create tasks for all producers
    tasks = [
        asyncio.create_task(run_with_logging("binance-futures-aggtrades", binance_futures_aggtrades.main)),
        asyncio.create_task(run_with_logging("binance-spot-aggtrades", binance_spot_aggtrades.main)),
        asyncio.create_task(run_with_logging("binance-futures-indexpriceklines", binance_futures_indexpriceklines.main)),
        asyncio.create_task(run_with_logging("binance-futures-markpriceklines", binance_futures_markpriceklines.main)),
        asyncio.create_task(run_with_logging("binance-futures-premiumindexklines", binance_futures_premiumindexklines.main)),
        asyncio.create_task(run_with_logging("binance-futures-metrics", binance_futures_metrics.main)),
        asyncio.create_task(run_with_logging("binance-futures-fundingrate", binance_futures_fundingrate.main)),
    ]
    
    # Wait for all tasks
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Multi-Producer stopped by user")
