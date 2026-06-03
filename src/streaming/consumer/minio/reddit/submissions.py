"""
Reddit Submissions Consumer
Kafka -> MinIO (Batch mode for medium volume)
"""
from src.streaming.consumer.minio.consumer import Consumer


def main():
    """Main consumer function."""
    consumer = Consumer(
        topic="reddit-submissions",
        data_type="submissions",
        unique_field="id",
        timestamp_field="created_utc",
        file_pattern="monthly",
        file_prefix="RS",
        column_names=[
            "id",
            "author",
            "created_utc",
            "subreddit",
            "link_flair_text",
            "title",
            "selftext",
            "language",
            "relevance",
            "bot",
            "sentiment",
        ],
        bootstrap_servers="redpanda:9092",
        batch_size=100,
        enable_batching=True,
        timestamp_unit="s",
        minio_bucket="reddit",
    )
    consumer.consume()


if __name__ == "__main__":
    main()
