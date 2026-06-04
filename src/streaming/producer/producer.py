"""Shared Redpanda producer helper for streaming ingestion."""

import json
import traceback

from kafka import KafkaProducer
from kafka.errors import KafkaError


class Producer:
    """Wrapper class for producing JSON messages to Redpanda topics."""

    def __init__(
        self,
        bootstrap_servers="redpanda:9092",
        topic=None,
        compression_type="lz4",
        batch_size=16384,
        linger_ms=10,
        acks=1,
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                compression_type=compression_type,
                batch_size=batch_size,
                linger_ms=linger_ms,
                acks=acks,
                retries=3,
                max_in_flight_requests_per_connection=5,
                api_version=(0, 10, 0),
                request_timeout_ms=10000,
                metadata_max_age_ms=300000,
                buffer_memory=33554432,
                max_block_ms=5000,
                connections_max_idle_ms=600000,
            )
        except Exception as exc:
            print(f" Failed to initialize Redpanda producer: {exc}")
            raise

    def send(self, message, topic=None, key=None):
        """Send a message to Redpanda."""
        target_topic = topic or self.topic
        if not target_topic:
            raise ValueError("No topic specified and no default topic set")

        try:
            return self.producer.send(target_topic, value=message, key=key)
        except KafkaError as exc:
            print(f" KafkaError sending message: {exc}")
            traceback.print_exc()
            return None
        except Exception as exc:
            print(f" Unexpected error sending message: {exc}")
            traceback.print_exc()
            return None

    def flush(self, timeout=30):
        """Force-send all buffered messages."""
        try:
            self.producer.flush(timeout=timeout)
        except Exception as exc:
            print(f" Flush error: {exc}")
            traceback.print_exc()

    def close(self, timeout=None):
        """Close the producer and flush pending messages."""
        try:
            self.producer.close(timeout=timeout)
            print(" Producer closed successfully")
        except Exception as exc:
            print(f" Error closing producer: {exc}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
