"""
Multi-consumer runner for TimescaleDB dashboard consumers.

Runs dashboard consumers concurrently in a single process.
"""

import asyncio
import logging
import time

from src.streaming.consumer.timescaledb.dashboard import (
    futures_indexpriceklines,
    futures_metrics,
    sentiment,
)


logging.Formatter.converter = time.gmtime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


async def run_consumer(name, consumer_main):
    """Run a synchronous consumer inside an executor with logging."""
    try:
        logger.info(f"Starting consumer: {name}")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, consumer_main)
    except Exception as exc:
        logger.error(f"Consumer {name} failed: {exc}", exc_info=True)


async def main():
    tasks = [
        asyncio.create_task(
            run_consumer(
                "dashboard-futures-indexpriceklines",
                futures_indexpriceklines.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "dashboard-futures-metrics",
                futures_metrics.main,
            )
        ),
        asyncio.create_task(
            run_consumer(
                "dashboard-sentiment",
                sentiment.main,
            )
        ),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
