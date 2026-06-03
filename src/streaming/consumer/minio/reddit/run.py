"""
Multi-Consumer Manager for Reddit MinIO Sink
Runs Reddit comments and submissions consumers concurrently in a single process
"""

import asyncio
import logging

from src.streaming.consumer.minio.reddit import comments, submissions


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


async def run_consumer(name, consumer_main):
    """Run a consumer with error handling."""
    try:
        logger.info(f"Starting consumer: {name}")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, consumer_main)
    except Exception as exc:
        logger.error(f"Consumer {name} failed: {exc}", exc_info=True)


async def main():
    """Run Reddit consumers concurrently."""
    tasks = [
        asyncio.create_task(run_consumer("reddit-comments", comments.main)),
        asyncio.create_task(run_consumer("reddit-submissions", submissions.main)),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
