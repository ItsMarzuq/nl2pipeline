import json
import logging
import time
from uuid import uuid4

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from app.logging_config import setup_logging
from app.settings import CONSUMER_MAX_MESSAGES, KAFKA_BOOTSTRAP, KAFKA_TOPIC


logger = logging.getLogger(__name__)


def create_consumer() -> KafkaConsumer:
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=f"gdelt-test-consumer-{uuid4()}",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                consumer_timeout_ms=15000,
            )
            logger.info("Connected consumer to Kafka")
            return consumer

        except NoBrokersAvailable:
            logger.warning("Kafka not available yet. Attempt %s/10", attempt)
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka")


def main() -> None:
    setup_logging()

    consumer = create_consumer()

    logger.info("Reading up to %s messages from topic %s", CONSUMER_MAX_MESSAGES, KAFKA_TOPIC)

    count = 0

    for message in consumer:
        print(json.dumps(message.value, indent=2))
        count += 1

        if count >= CONSUMER_MAX_MESSAGES:
            break

    logger.info("Consumer test complete. Messages read: %s", count)

    consumer.close()


if __name__ == "__main__":
    main()