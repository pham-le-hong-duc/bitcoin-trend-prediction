"""
Binance Spot AggTrades Consumer
Kafka → MinIO (Batch mode for high volume)
"""
from src.streaming.consumer.minio.consumer import Consumer


def main():
    """Main consumer function"""
    consumer = Consumer(
        topic='binance-spot-aggTrades',
        data_type='spot/daily/aggTrades/BTCUSDT',
        unique_field='aggregate_trade_id',
        timestamp_field='timestamp',
        file_pattern='daily',
        column_names=[
            'aggregate_trade_id',
            'price',
            'quantity',
            'first_trade_id',
            'last_trade_id',
            'timestamp',
            'was_buyer_maker',
            'was_best_price_match'
        ],
        bootstrap_servers='redpanda:9092',
        batch_size=100,
        enable_batching=True
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()
