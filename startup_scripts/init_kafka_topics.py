"""
setup_kafka_topics.py
---------------------
Creates and verifies Kafka topics for the NL2Pipeline ingestion layer.

Topics:
    gdelt-events-raw    — GDELT global events stream
    pipeline-errors-dlq — Dead Letter Queue for failed/malformed messages

Broker: Resolved from KAFKA_HOST env var (default: localhost:9092)
"""

import logging
import os
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BROKER = os.getenv("KAFKA_HOST", "localhost:9092")

TOPICS = [
    {
        "name": "gdelt-events-raw",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": "604800000",    # 7 days
            "cleanup.policy": "delete",
            "max.message.bytes": "5242880", # 5 MB
        },
    },
    {
        "name": "pipeline-errors-dlq",
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": "2592000000",   # 30 days
            "cleanup.policy": "delete",
            "max.message.bytes": "5242880",
        },
    },
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def wait_for_kafka(broker: str, retries: int = 10, delay: int = 5) -> AdminClient:
    """Poll broker until ready, return AdminClient. Raises RuntimeError on timeout."""
    log.info(f"Waiting for Kafka broker at {broker} ...")
    for attempt in range(1, retries + 1):
        try:
            client = AdminClient({"bootstrap.servers": broker})
            client.list_topics(timeout=5)
            log.info(f"Kafka broker ready after {attempt} attempt(s).")
            return client
        except Exception as exc:
            log.warning(f"Not ready (attempt {attempt}/{retries}): {exc} — retrying in {delay}s ...")
            time.sleep(delay)
    raise RuntimeError(f"Kafka broker at '{broker}' unavailable after {retries} attempts.")


def create_topic(client: AdminClient, topic_cfg: dict) -> None:
    """Create a topic if it doesn't already exist."""
    name = topic_cfg["name"]

    if name in client.list_topics(timeout=10).topics:
        log.info(f"Topic '{name}' already exists — skipping.")
        return

    new_topic = NewTopic(
        topic=name,
        num_partitions=topic_cfg["partitions"],
        replication_factor=topic_cfg["replication_factor"],
        config=topic_cfg["config"],
    )

    futures = client.create_topics([new_topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info(f"Topic '{topic_name}' created successfully.")
        except KafkaException as e:
            log.error(f"Failed to create topic '{topic_name}': {e}")
            raise


def verify_topic(client: AdminClient, topic_name: str) -> None:
    """Confirm topic is live and log partition count."""
    metadata = client.list_topics(topic=topic_name, timeout=10)
    if topic_name not in metadata.topics:
        raise RuntimeError(f"Topic '{topic_name}' not found after creation.")
    partition_count = len(metadata.topics[topic_name].partitions)
    log.info(f"Verified '{topic_name}': {partition_count} partition(s) online.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("NL2Pipeline — Kafka Topic Setup")
    log.info(f"Broker: {BROKER}")
    log.info("=" * 60)

    try:
        admin = wait_for_kafka(BROKER)
    except RuntimeError as e:
        log.error(str(e))
        sys.exit(1)

    for topic_cfg in TOPICS:
        try:
            create_topic(admin, topic_cfg)
            verify_topic(admin, topic_cfg["name"])
        except Exception as e:
            log.error(f"Setup failed for '{topic_cfg['name']}': {e}")
            sys.exit(1)

    log.info("=" * 60)
    log.info("All topics ready. Setup complete.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()