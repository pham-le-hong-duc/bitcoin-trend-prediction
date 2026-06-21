"""
Binance Futures AggTrades Consumer
Kafka → MinIO (Batch mode for high volume)
"""
from src.streaming.consumer.minio.base import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-futures-aggTrades',
        data_type='futures/um/daily/aggTrades/BTCUSDT',
        unique_field='agg_trade_id',
        timestamp_field='transact_time',
        file_pattern='daily',
        column_names=[
            'agg_trade_id',
            'price',
            'quantity',
            'first_trade_id',
            'last_trade_id',
            'transact_time',
            'is_buyer_maker'
        ],
        bootstrap_servers='redpanda:9092',
        batch_size=100,
        enable_batching=True
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
