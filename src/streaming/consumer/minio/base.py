"""
Redpanda Consumer for Binance Data Ingestion.

This consumer reads messages from Redpanda topics, batches them, and writes to MinIO.
Architecture: Redpanda Topic → Batch Process → MinIO (Parquet)
"""
import json
import time
import traceback
from datetime import datetime, timezone
from kafka import KafkaConsumer
import polars as pl

from src.utils.s3_client import MinIOWriter


class Consumer:
    """
    Consumer that reads from Redpanda and writes to MinIO.
    
    Features:
    - Batch processing for efficiency
    - Automatic date-based file partitioning
    - Deduplication by unique field
    - Graceful shutdown
    """
    
    def __init__(self,
                 topic,
                 data_type,
                 unique_field,
                 timestamp_field,
                 file_pattern='daily',
                 column_names=None,
                 bootstrap_servers='localhost:19092',
                 group_id=None,
                 batch_size=100,
                 enable_batching=True):
        """
        Initialize consumer.
        
        Args:
            topic: Redpanda topic to consume from
            data_type: Full data type path including symbol (e.g., 'futures/um/daily/aggTrades/BTCUSDT')
            unique_field: Field to use for deduplication (e.g., 'trade_id')
            timestamp_field: Field to use for timestamp (e.g., 'created_time')
            file_pattern: 'daily' or 'monthly' file partitioning
            column_names: Expected column names (optional, for schema enforcement)
            bootstrap_servers: Redpanda broker addresses
            group_id: Consumer group ID (default: {topic}-consumer-group)
            batch_size: Number of messages to batch before writing (default: 100)
            enable_batching: Enable batching (False = write immediately, for low-volume streams)
        """
        self.topic = topic
        self.data_type = data_type  
        self.unique_field = unique_field
        self.timestamp_field = timestamp_field
        self.file_pattern = file_pattern
        self.column_names = column_names
        self.enable_batching = enable_batching
        self.batch_size = batch_size if enable_batching else 1
        
        # Initialize MinIO writer
        self.minio_writer = MinIOWriter()
        
        # Initialize Kafka consumer
        consumer_group_id = group_id or f"{topic}-consumer-group"
        
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=consumer_group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=40000,
                metadata_max_age_ms=300000,
                consumer_timeout_ms=-1  # Block indefinitely
            )
            # Subscribe to topic (triggers partition assignment)
            self.consumer.subscribe([topic])
            partitions = []
            max_wait = 30  # Wait up to 30 seconds
            for _ in range(max_wait):
                partitions = self.consumer.assignment()
                if partitions:
                    break
                self.consumer.poll(timeout_ms=1000)  # Trigger assignment
            
            if not partitions:
                raise Exception(f"Failed to get partition assignment after {max_wait}s")
            
        except Exception as e:
            print(f"Failed to connect to Redpanda: {e}")
            raise
        
        self.running = True  # Set to True so consume loop can run
        self.batch = []
        
        # Stats
        self.total_consumed = 0
        self.total_written = 0
        self.batch_timeout = 12  # seconds
        self.last_flush_time = time.time()
    
    def _group_by_period(self, records):
        """
        Group records by date or month based on file_pattern.
        
        Args:
            records: List of record dicts
        
        Returns:
            dict: {period_str: [records]}
        """
        grouped = {}
        
        for record in records:
            ts = record[self.timestamp_field]
            
            # Handle both integer timestamps and string datetimes
            if isinstance(ts, str):
                # String datetime (e.g., "2026-02-23 12:00:00")
                dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            else:
                # Integer timestamp in milliseconds
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if self.file_pattern == 'daily':
                period_str = dt.strftime('%Y-%m-%d')
            else:  # monthly
                period_str = dt.strftime('%Y-%m')
            
            if period_str not in grouped:
                grouped[period_str] = []
            grouped[period_str].append(record)
        
        return grouped
    
    def _write_to_minio(self, records):
        """
        Write records to MinIO, grouped by period.
        
        Args:
            records: List of record dicts
        """
        if not records:
            return
        
        # Group by period
        grouped = self._group_by_period(records)
        
        # Write each period
        for period_str, period_records in grouped.items():
            try:
                # Convert to DataFrame
                df = pl.DataFrame(period_records)
                
                # Enforce schema if column_names specified
                if self.column_names:
                    # Select and reorder columns to match expected schema
                    missing_cols = [col for col in self.column_names if col not in df.columns]
                    extra_cols = [col for col in df.columns if col not in self.column_names]
                    
                    if missing_cols:
                        print(f"WARNING: Missing expected columns: {missing_cols}")
                    if extra_cols:
                        print(f"WARNING: Extra columns (will be dropped): {extra_cols}")
                    
                    # Select only expected columns that exist
                    available_cols = [col for col in self.column_names if col in df.columns]
                    df = df.select(available_cols)
                
                df = df.sort(self.timestamp_field)
                
                # MinIO path matching Binance structure
                # data_type already includes full path with symbol
                object_path = f"{self.data_type}/{period_str}.parquet"
                
                # Check if file exists
                existing_df = self.minio_writer.read_parquet(object_path)
                
                if existing_df is not None:
                    # Merge and deduplicate
                    combined_df = pl.concat([existing_df, df]).unique(
                        subset=[self.unique_field],
                        keep="last"
                    ).sort(self.timestamp_field)
                    final_df = combined_df
                else:
                    final_df = df
                
                # Write to MinIO
                if self.minio_writer.write_parquet(final_df, object_path):
                    print(f"{len(period_records)} records -> {object_path}")
                    self.total_written += len(period_records)
                else:
                    print(f"Failed to write {len(period_records)} records to {object_path}")
                    
            except Exception as e:
                print(f"Error writing to MinIO: {e}")
    
    def _flush_batch(self):
        """Flush current batch to MinIO."""
        if not self.batch:
            return
        
        try:
            self._write_to_minio(self.batch)
            self.batch.clear()
            self.last_flush_time = time.time()
        except Exception as e:
            print(f"Error flushing batch: {e}")
    
    def consume(self):
        """
        Main consume loop with non-blocking poll.
        Ultra-low latency: Poll instantly, sleep only when no messages.
        """
        self.running = True
        
        mode = f"Batch ({self.batch_size} msgs)" if self.enable_batching else "Instant write"
        
        try:
            while self.running:
                # Non-blocking poll (instant return)
                messages = self.consumer.poll(timeout_ms=0)
                
                if not messages:
                    # No messages - check timeout for batch flush
                    if self.enable_batching and len(self.batch) > 0:
                        if (time.time() - self.last_flush_time) >= self.batch_timeout:
                            self._flush_batch()
                            self.consumer.commit()
                    # Sleep 1ms to avoid busy-wait
                    time.sleep(0.001)
                    continue
                
                # Process messages
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            if record.value is None:
                                print(f"WARNING: Received None value, skipping")
                                continue
                            
                            self.batch.append(record.value)
                            self.total_consumed += 1
                            
                            # Flush logic
                            if self.enable_batching:
                                # Batch mode: Flush when batch is full or timeout
                                if len(self.batch) >= self.batch_size:
                                    self._flush_batch()
                                    self.consumer.commit()
                            else:
                                # Instant mode: Write immediately
                                self._flush_batch()
                                self.consumer.commit()
                                
                        except Exception as e:
                            print(f"Error processing message: {e}")
                            traceback.print_exc()
        
        except KeyboardInterrupt:
            print("\nReceived interrupt signal")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """Graceful shutdown."""
        print("\nShutting down consumer...")
        
        # Flush remaining batch
        if self.batch:
            self._flush_batch()
        
        # Commit offsets
        try:
            self.consumer.commit()
            print("Offsets committed")
        except Exception as e:
            print(f"Failed to commit offsets: {e}")
        
        # Close consumer
        try:
            self.consumer.close()
            print("Consumer closed")
        except Exception as e:
            print(f"Failed to close consumer: {e}")
        
        # Print stats
        print(f"\n{'='*80}")
        print("CONSUMER STATISTICS:")
        print(f"{'='*80}")
        print(f"Total consumed: {self.total_consumed}")
        print(f"Total written: {self.total_written}")
        print(f"{'='*80}\n")
    
    def stop(self):
        """Stop the consumer."""
        self.running = False
