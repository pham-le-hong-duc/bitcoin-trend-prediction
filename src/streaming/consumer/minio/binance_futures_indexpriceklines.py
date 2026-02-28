"""
Binance Futures Index Price Klines Consumer
Kafka → MinIO (Instant mode for low volume)
"""
from base import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-futures-indexPriceKlines',
        data_type='futures/um/daily/indexPriceKlines/BTCUSDT/1m',
        unique_field='open_time',
        timestamp_field='open_time',
        file_pattern='daily',
        bootstrap_servers='redpanda:9092',
        enable_batching=False
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
