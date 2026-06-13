"""
Multi-consumer runner for TimescaleDB dashboard consumers.

Runs dashboard consumers concurrently in a single process.
"""

import asyncio
import logging
import os
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
RESTART_DELAY_SECONDS = int(
    os.getenv("TIMESCALEDB_CONSUMER_RESTART_DELAY_SECONDS", "15")
)

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


async def run_consumer(name, consumer_main):
    """Keep one dashboard consumer alive even if it crashes."""
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
