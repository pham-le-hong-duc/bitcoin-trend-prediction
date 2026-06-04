import json
import os
import sys
import time

from kafka import KafkaConsumer


REDDIT_STATUS_TOPIC = "reddit-status"
BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092")
WAIT_TIMEOUT_SECONDS = int(os.getenv("REDDIT_STATUS_WAIT_TIMEOUT_SECONDS", "1800"))
POLL_TIMEOUT_MS = int(os.getenv("REDDIT_STATUS_POLL_TIMEOUT_MS", "1000"))


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        REDDIT_STATUS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )


def wait_for_first_run_status() -> bool:
    deadline = time.time() + WAIT_TIMEOUT_SECONDS
    consumer = build_consumer()

    try:
        print("Waiting for reddit first-run status!")
        print(f"Topic: {REDDIT_STATUS_TOPIC}")
        print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
        print(f"Timeout: {WAIT_TIMEOUT_SECONDS}s")
        while time.time() < deadline:
            messages = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            if not messages:
                continue

            for _, records in messages.items():
                for record in records:
                    payload = record.value
                    if payload.get("first_run") is True:
                        print("Detected reddit first-run status!")
                        return True
        return False
    finally:
        consumer.close()


def main() -> None:
    if wait_for_first_run_status():
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()

