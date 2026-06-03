"""
Binance Futures Aggregate Trades Producer

Streams aggregate trade data from Binance Futures WebSocket and produces to Redpanda.
Schema matches futures-aggTrades.csv format.
"""
import asyncio
import os
import logging
import time
from src.streaming.producer.producer import Producer

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
  DerivativesTradingUsdsFutures,
  DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
  ConfigurationWebSocketStreams,
)


# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceFuturesAggTradesProducer:
  """
  Producer for Binance Futures Aggregate Trades.
  
  Schema (futures-aggTrades.csv):
  - agg_trade_id: integer
  - price: decimal
  - quantity: decimal
  - first_trade_id: integer
  - last_trade_id: integer
  - transact_time: integer (timestamp)
  - is_buyer_maker: boolean
  """
  
  def __init__(self, 
         symbol="btcusdt",
         bootstrap_servers='redpanda:9092',
         topic='binance-futures-aggTrades'):
    """
    Initialize the producer.
    
    Args:
      symbol: Trading pair symbol (default: btcusdt)
      bootstrap_servers: Redpanda broker address
      topic: Kafka topic to produce to
    """
    self.symbol = symbol.lower()
    self.topic = topic
    self.bootstrap_servers = bootstrap_servers
    
    # Initialize Binance WebSocket client
    configuration_ws_streams = ConfigurationWebSocketStreams(
      stream_url=os.getenv(
        "STREAM_URL", DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL
      )
    )
    self.client = DerivativesTradingUsdsFutures(config_ws_streams=configuration_ws_streams)
    
    # Initialize Kafka producer
    self.producer = Producer(
      bootstrap_servers=bootstrap_servers,
      topic=topic,
      compression_type='lz4'
    )
    
    self.message_count = 0
    self.last_log_time = time.time()
    self.last_log_count = 0
    self.connection = None
    
  def transform_message(self, data):
    """
    Transform Binance WebSocket message to match schema.
    
    Binance SDK returns AggregateTradeStreamsResponse object with attributes:
    - e: event type
    - E: event time
    - s: symbol
    - a: aggregate trade ID
    - p: price
    - q: quantity (or nq for notional quantity)
    - f: first trade ID
    - l: last trade ID
    - T: trade time
    - m: is buyer maker
    
    Returns schema format:
    {
      "agg_trade_id": integer,
      "price": decimal,
      "quantity": decimal,
      "first_trade_id": integer,
      "last_trade_id": integer,
      "transact_time": integer,
      "is_buyer_maker": boolean
    }
    """
    try:
      return {
        "agg_trade_id": data.a,
        "price": float(data.p),
        "quantity": float(data.q),
        "first_trade_id": data.f,
        "last_trade_id": data.l,
        "transact_time": data.T,
        "is_buyer_maker": data.m
      }
    except Exception as e:
      logger.error(f"Error transforming message: {e}, data: {data}")
      return None
  
  def on_message(self, data):
    """
    Callback for WebSocket messages.
    
    Args:
      data: Raw message from Binance WebSocket
    """
    # Transform to schema format
    transformed = self.transform_message(data)
    
    if transformed:
      # Send to Redpanda
      future = self.producer.send(transformed)
      
      if future:
        self.message_count += 1
        
        # Log every 60 seconds
        now = time.time()
        elapsed = now - self.last_log_time
        
        if elapsed >= 60:
          messages_in_period = self.message_count - self.last_log_count
          rate = messages_in_period / elapsed
          from datetime import datetime, timezone
          current_time = datetime.now(timezone.utc).strftime('%H:%M:%S')
          logger.info(f"[{current_time}] | {messages_in_period:,} msgs | {rate:.0f} msgs/s")
          
          self.last_log_time = now
          self.last_log_count = self.message_count
      else:
        logger.error(f"Failed to send message: {transformed}")
  
  async def start(self):
    """
    Start streaming aggregate trades from Binance and produce to Redpanda.
    """
    try:
      # Create WebSocket connection
      self.connection = await self.client.websocket_streams.create_connection()
      
      # Subscribe to aggregate trade stream
      stream = await self.connection.aggregate_trade_streams(symbol=self.symbol)
      stream.on("message", self.on_message)
      
      # Keep running indefinitely
      while True:
        await asyncio.sleep(1)
        
    except KeyboardInterrupt:
      logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
      logger.error(f"Error in stream: {e}")
      raise
    finally:
      await self.cleanup()
  
  async def cleanup(self):
    """
    Cleanup resources.
    """
    logger.info("Cleaning up resources...")
    
    # Flush pending messages
    if self.producer:
      logger.info("Flushing producer...")
      self.producer.flush()
      self.producer.close()
    
    # Close WebSocket connection
    if self.connection:
      logger.info("Closing WebSocket connection...")
      await self.connection.close_connection(close_session=True)
    
    logger.info(f"Total messages sent: {self.message_count}")


async def main():
  """
  Main entry point.
  """
  # Get configuration from environment variables
  symbol = os.getenv("SYMBOL", "btcusdt")
  bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
  topic = os.getenv("TOPIC", "binance-futures-aggTrades")
  
  # Create and start producer
  producer = BinanceFuturesAggTradesProducer(
    symbol=symbol,
    bootstrap_servers=bootstrap_servers,
    topic=topic
  )
  
  await producer.start()


if __name__ == "__main__":
  asyncio.run(main())




