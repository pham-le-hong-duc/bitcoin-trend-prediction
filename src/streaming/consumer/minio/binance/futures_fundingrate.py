"""
Binance Futures Funding Rate Consumer
Kafka → MinIO (Instant mode for ultra-low volume)
"""
from src.streaming.consumer.minio.base import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-futures-fundingRate',
        data_type='futures/um/monthly/fundingRate/BTCUSDT',
        unique_field='calc_time',
        timestamp_field='calc_time',
        file_pattern='monthly',
        bootstrap_servers='redpanda:9092',
        enable_batching=False
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
