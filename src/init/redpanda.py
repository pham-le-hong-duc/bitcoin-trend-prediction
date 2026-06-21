from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import sys
import time

REDPANDA_BROKERS = ["redpanda:9092"]

TOPICS = [
    {
        "name": "binance-futures-aggTrades",
        "partitions": 6,
        "replication": 1,
        "config": {
            "retention.ms": "604800000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    # {
    #     "name": "binance-spot-aggTrades",
    #     "partitions": 6,
    #     "replication": 1,
    #     "config": {
    #         "retention.ms": "604800000",
    #         "compression.type": "lz4",
    #         "cleanup.policy": "delete"
    #     }
    # },
    {
        "name": "binance-futures-klines",
        "partitions": 1,
        "replication": 1,
        "config": {
            "retention.ms": "604800000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    # {
    #     "name": "binance-futures-indexPriceKlines",
    #     "partitions": 1,
    #     "replication": 1,
    #     "config": {
    #         "retention.ms": "604800000",
    #         "compression.type": "lz4",
    #         "cleanup.policy": "delete"
    #     }
    # },
    # {
    #     "name": "binance-futures-markPriceKlines",
    #     "partitions": 1,
    #     "replication": 1,
    #     "config": {
    #         "retention.ms": "604800000",
    #         "compression.type": "lz4",
    #         "cleanup.policy": "delete"
    #     }
    # },
    {
        "name": "binance-futures-premiumIndexKlines",
        "partitions": 1,
        "replication": 1,
        "config": {
            "retention.ms": "604800000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    {
        "name": "binance-futures-metrics",
        "partitions": 1,
        "replication": 1,
        "config": {
            "retention.ms": "2592000000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    # {
    #     "name": "binance-futures-fundingRate",
    #     "partitions": 1,
    #     "replication": 1,
    #     "config": {
    #         "retention.ms": "2592000000",
    #         "compression.type": "lz4",
    #         "cleanup.policy": "delete"
    #     }
    # },
    {
        "name": "binance-spot-klines",
        "partitions": 1,
        "replication": 1,
        "config": {
            "retention.ms": "604800000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    {
        "name": "reddit-comments",
        "partitions": 3,
        "replication": 1,
        "config": {
            "retention.ms": "2592000000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    {
        "name": "reddit-submissions",
        "partitions": 3,
        "replication": 1,
        "config": {
            "retention.ms": "2592000000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    },
    {
        "name": "reddit-status",
        "partitions": 1,
        "replication": 1,
        "config": {
            "retention.ms": "2592000000",
            "compression.type": "lz4",
            "cleanup.policy": "delete"
        }
    }
]

def wait_for_redpanda(brokers, max_retries=30):
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=brokers,
                request_timeout_ms=5000
            )
            admin.close()
            return True
        except Exception as e:
            time.sleep(2)
    return False

def create_topics(brokers, topics):
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=brokers,
            request_timeout_ms=10000
        )
        
        new_topics = []
        for topic in topics:
            new_topic = NewTopic(
                name=topic["name"],
                num_partitions=topic["partitions"],
                replication_factor=topic["replication"],
                topic_configs=topic.get("config", {})
            )
            new_topics.append(new_topic)
        
        try:
            admin.create_topics(new_topics, validate_only=False)
        except TopicAlreadyExistsError:
            pass
        except Exception:
            for topic in new_topics:
                try:
                    admin.create_topics([topic], validate_only=False)
                except TopicAlreadyExistsError:
                    pass
                except Exception as e:
                    print(f"Error creating topic '{topic.name}': {e}", file=sys.stderr)
        
        admin.close()
        return True
        
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return False

def main():
    print("REDPANDA INITIALIZATION")
    
    if not wait_for_redpanda(REDPANDA_BROKERS):
        print("Redpanda is not ready", file=sys.stderr)
        sys.exit(1)
    
    if not create_topics(REDPANDA_BROKERS, TOPICS):
        sys.exit(1)
    
    print("REDPANDA INITIALIZATION COMPLETED!")

if __name__ == "__main__":
    main()
