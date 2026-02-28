"""
Binance Futures Metrics Consumer
Kafka → MinIO (Instant mode for very low volume)
"""
from base import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-futures-metrics',
        data_type='futures/um/daily/metrics/BTCUSDT',
        unique_field='create_time',
        timestamp_field='create_time',
        file_pattern='daily',
        bootstrap_servers='redpanda:9092',
        enable_batching=False
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
