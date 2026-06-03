"""
Binance Futures Mark Price Klines Consumer
Kafka → MinIO (Instant mode for low volume)
"""
from src.streaming.consumer.minio.consumer import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-futures-markPriceKlines',
        data_type='futures/um/daily/markPriceKlines/BTCUSDT/1m',
        unique_field='open_time',
        timestamp_field='open_time',
        file_pattern='daily',
        bootstrap_servers='redpanda:9092',
        enable_batching=False
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
