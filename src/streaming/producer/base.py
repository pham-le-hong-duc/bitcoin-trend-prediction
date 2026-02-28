"""
Redpanda Producer Helper for Binance Data Ingestion.

This class provides a simple wrapper around Kafka producer for sending messages to Redpanda.
"""
import json
import traceback
from kafka import KafkaProducer
from kafka.errors import KafkaError


class Producer:
  """
  Wrapper class for producing messages to Redpanda topics.
  
  Features:
  - Automatic JSON serialization
  - Error handling
  - Async sending with callbacks
  """
  
  def __init__(self, 
         bootstrap_servers='redpanda:9092', # Docker internal hostname
         topic=None,
         compression_type='lz4', # Default compression (match Redpanda config)
         batch_size=16384,
         linger_ms=10):
    """
    Initialize Redpanda producer.
    
    Args:
      bootstrap_servers: Redpanda broker addresses (default: localhost:19092)
      topic: Default topic to send messages to
      compression_type: Compression algorithm (snappy, gzip, lz4, zstd)
      batch_size: Batch size in bytes before sending
      linger_ms: Time to wait before sending batch (allows batching)
    """
    self.topic = topic
    self.bootstrap_servers = bootstrap_servers
    
    try:
      self.producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        compression_type=compression_type,
        batch_size=batch_size,
        linger_ms=linger_ms,
        acks=1,
        retries=3,
        max_in_flight_requests_per_connection=5,
        api_version=(0, 10, 0),
        request_timeout_ms=10000,
        metadata_max_age_ms=300000,
        buffer_memory=33554432,
        max_block_ms=5000,
        connections_max_idle_ms=600000  # 10 minutes - keep connection alive
      )
      # Đã kết nối (bỏ log để giảm nhiễu)
    except Exception as e:
      print(f" Failed to initialize Redpanda producer: {e}")
      raise
  
  def send(self, message, topic=None, key=None):
    """
    Send a message to Redpanda topic.
    
    Args:
      message: Message to send (dict, will be JSON serialized)
      topic: Topic to send to (uses default if not specified)
      key: Optional message key for partitioning
    
    Returns:
      FutureRecordMetadata: Future object for the send result
    """
    target_topic = topic or self.topic
    
    if not target_topic:
      raise ValueError("No topic specified and no default topic set")
    
    try:
      return self.producer.send(target_topic, value=message, key=key)
    except KafkaError as e:
      print(f" KafkaError sending message: {e}")
      traceback.print_exc()
      return None
    except Exception as e:
      print(f" Unexpected error sending message: {e}")
      traceback.print_exc()
      return None
  
  def flush(self, timeout=30):
    """
    Force send all buffered messages.
    
    Args:
      timeout: Max time to wait in seconds (default: 5)
    """
    try:
      self.producer.flush(timeout=timeout)
    except Exception as e:
      print(f"⚠️ Flush error: {e}")
      traceback.print_exc()
  
  def close(self, timeout=None):
    """
    Close the producer and flush all pending messages.
    
    Args:
      timeout: Max time to wait in seconds
    """
    try:
      self.producer.close(timeout=timeout)
      print(" Producer closed successfully")
    except Exception as e:
      print(f" Error closing producer: {e}")
  
  def __enter__(self):
    """Context manager entry."""
    return self
  
  def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit."""
    self.close()

