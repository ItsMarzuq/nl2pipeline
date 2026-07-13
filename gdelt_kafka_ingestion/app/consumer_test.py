import json
import logging
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException

from app.logging_config import setup_logging
from app.settings import CONSUMER_MAX_MESSAGES, KAFKA_BOOTSTRAP, KAFKA_TOPIC


logger = logging.getLogger(__name__)

_POLL_IDLE_TIMEOUT = 15.0  # seconds with no message before giving up


def create_consumer() -> Consumer:
    for attempt in range(1, 11):
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": f"gdelt-test-consumer-{uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            })
            consumer.list_topics(timeout=5)
            consumer.subscribe([KAFKA_TOPIC])
            logger.info("Connected consumer to Kafka")
            return consumer

        except KafkaException as exc:
            logger.warning("Kafka not available yet (%s). Attempt %s/10", exc, attempt)
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka")


def main() -> None:
    setup_logging()

    consumer = create_consumer()

    logger.info("Reading up to %s messages from topic %s", CONSUMER_MAX_MESSAGES, KAFKA_TOPIC)

    count = 0

    try:
        while count < CONSUMER_MAX_MESSAGES:
            msg = consumer.poll(timeout=_POLL_IDLE_TIMEOUT)

            if msg is None:
                logger.info("No more messages after %ss idle — stopping.", _POLL_IDLE_TIMEOUT)
                break

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            value = json.loads(msg.value().decode("utf-8"))
            print(json.dumps(value, indent=2))
            count += 1
    finally:
        consumer.close()

    logger.info("Consumer test complete. Messages read: %s", count)


if __name__ == "__main__":
    main()