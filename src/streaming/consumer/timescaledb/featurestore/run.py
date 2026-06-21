"""
Runner for TimescaleDB featurestore consumers.
"""

import asyncio
import logging
import time

from src.streaming.consumer.timescaledb.featurestore import (
    futures_aggTrades,
    futures_klines,
    futures_metrics,
    futures_premiumIndexKlines,
    sentiment,
    spot_klines,
)


logging.Formatter.converter = time.gmtime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
RESTART_DELAY_SECONDS = 15

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


async def run_consumer(name, consumer_main):
    """Keep one featurestore consumer alive even if it crashes."""
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info(f"Starting consumer: {name} (attempt {attempt})")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, consumer_main)
            logger.warning(f"Consumer {name} exited unexpectedly, restarting soon")
        except Exception as exc:
            logger.error(f"Consumer {name} failed: {exc}", exc_info=True)

        await asyncio.sleep(RESTART_DELAY_SECONDS)


async def main():
    tasks = [
        asyncio.create_task(
            run_consumer(
                "featurestore-futures-klines",
                futures_klines.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "featurestore-futures-metrics",
                futures_metrics.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "featurestore-futures-premiumindexklines",
                futures_premiumIndexKlines.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "featurestore-spot-klines",
                spot_klines.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "featurestore-sentiment",
                sentiment.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "featurestore-futures-aggtrades",
                futures_aggTrades.main,
            )
        ),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
