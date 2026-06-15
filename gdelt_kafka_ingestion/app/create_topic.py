import logging
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from app.logging_config import setup_logging
from app.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC


logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()

    logger.info("Creating Kafka topic if missing: %s", KAFKA_TOPIC)

    admin = None

    for attempt in range(1, 11):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="gdelt-topic-admin",
            )
            break
        except NoBrokersAvailable:
            logger.warning("Kafka not ready yet. Attempt %s/10", attempt)
            time.sleep(3)

    if admin is None:
        raise RuntimeError("Could not connect to Kafka admin client")

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=1,
        replication_factor=1,
    )

    try:
        admin.create_topics([topic])
        logger.info("Topic created: %s", KAFKA_TOPIC)
    except TopicAlreadyExistsError:
        logger.info("Topic already exists: %s", KAFKA_TOPIC)
    finally:
        admin.close()


if __name__ == "__main__":
    main()