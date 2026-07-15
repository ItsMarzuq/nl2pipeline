import csv
import io
import json
import logging
import time
import zipfile
from pathlib import Path
from typing import Iterable

import requests
from confluent_kafka import KafkaException, Producer

from app.gdelt_mapper import map_gdelt_row_to_message
from app.logging_config import setup_logging
from app.settings import (
    DATA_PATH,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    LIVE_POLL_SECONDS,
    PRODUCER_MODE,
    PUBLISH_RATE,
    REPLAY_LOOP,
)


GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

logger = logging.getLogger(__name__)


def create_kafka_producer() -> Producer:
    for attempt in range(1, 11):
        try:
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            # Force a broker round-trip so connection failures surface here,
            # not on the first publish_message() call.
            producer.list_topics(timeout=5)
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer

        except KafkaException as exc:
            logger.warning("Kafka not available yet (%s). Attempt %s/10", exc, attempt)
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka")


def publish_message(producer: Producer, message: dict) -> None:
    key = message["event_id"]
    producer.produce(
        KAFKA_TOPIC,
        key=str(key).encode("utf-8"),
        value=json.dumps(message).encode("utf-8"),
    )
    producer.poll(0)


def sleep_for_publish_rate() -> None:
    if PUBLISH_RATE <= 0:
        return

    time.sleep(1.0 / PUBLISH_RATE)


def publish_rows(
    producer: Producer,
    rows: Iterable[list[str]],
    source_name: str,
    published_count_start: int = 0,
) -> int:
    published_count = published_count_start
    skipped_count = 0

    for row in rows:
        try:
            message = map_gdelt_row_to_message(row)
            publish_message(producer, message)

            published_count += 1

            if published_count % 1000 == 0:
                producer.flush()
                logger.info("Published %s events", published_count)

            sleep_for_publish_rate()

        except ValueError as exc:
            skipped_count += 1
            logger.warning("Skipping malformed row from %s: %s", source_name, exc)

    producer.flush()

    logger.info(
        "Finished source=%s published_total=%s skipped_in_source=%s",
        source_name,
        published_count,
        skipped_count,
    )

    return published_count


def read_tsv_file(file_path: Path) -> Iterable[list[str]]:
    with file_path.open("r", encoding="utf-8", errors="replace", newline="") as file:
        reader = csv.reader(file, delimiter="\t")
        yield from reader


def list_replay_files() -> list[Path]:
    files = sorted(DATA_PATH.glob("*.tsv"))

    if not files:
        files = sorted(DATA_PATH.glob("*.CSV"))

    return files


def run_replay_mode(producer: Producer) -> None:
    files = list_replay_files()

    if not files:
        raise RuntimeError(
            f"No GDELT TSV files found in {DATA_PATH}. Run the downloader first."
        )

    logger.info("Starting replay mode")
    logger.info("Found %s files", len(files))
    logger.info("Publish rate: %s events/sec", PUBLISH_RATE)

    published_count = 0

    while True:
        for file_path in files:
            logger.info("Reading replay file: %s", file_path.name)
            rows = read_tsv_file(file_path)
            published_count = publish_rows(
                producer=producer,
                rows=rows,
                source_name=file_path.name,
                published_count_start=published_count,
            )

        if not REPLAY_LOOP:
            logger.info("Replay complete. REPLAY_LOOP=false, exiting.")
            break

        logger.info("Replay reached end of files. Looping again.")


def get_latest_export_urls() -> list[str]:
    response = requests.get(GDELT_LAST_UPDATE_URL, timeout=30)
    response.raise_for_status()

    urls = []

    for line in response.text.splitlines():
        parts = line.strip().split()

        for part in parts:
            if part.endswith(".export.CSV.zip"):
                urls.append(part)

    return urls


def download_live_zip(url: str) -> bytes:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def rows_from_zip_bytes(zip_bytes: bytes) -> Iterable[list[str]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zip_file:
        names = zip_file.namelist()

        if not names:
            return

        inner_file_name = names[0]

        with zip_file.open(inner_file_name) as raw_file:
            text_file = io.TextIOWrapper(raw_file, encoding="utf-8", errors="replace")
            reader = csv.reader(text_file, delimiter="\t")
            yield from reader


def run_live_mode(producer: Producer) -> None:
    logger.info("Starting live mode")
    logger.info("Polling every %s seconds", LIVE_POLL_SECONDS)

    seen_urls = set()
    published_count = 0

    while True:
        try:
            urls = get_latest_export_urls()

            for url in urls:
                if url in seen_urls:
                    logger.info("Already published in this session: %s", url)
                    continue

                logger.info("Downloading latest GDELT file: %s", url)
                zip_bytes = download_live_zip(url)

                rows = rows_from_zip_bytes(zip_bytes)

                published_count = publish_rows(
                    producer=producer,
                    rows=rows,
                    source_name=url,
                    published_count_start=published_count,
                )

                seen_urls.add(url)

        except requests.RequestException as exc:
            logger.warning("Live mode network error: %s", exc)
            logger.warning("Will retry on next cycle")

        except zipfile.BadZipFile as exc:
            logger.warning("Live mode bad zip file: %s", exc)
            logger.warning("Will retry on next cycle")

        logger.info("Waiting %s seconds before next live poll", LIVE_POLL_SECONDS)
        time.sleep(LIVE_POLL_SECONDS)


def main() -> None:
    setup_logging()

    logger.info("Producer mode: %s", PRODUCER_MODE)

    producer = create_kafka_producer()

    if PRODUCER_MODE == "replay":
        run_replay_mode(producer)
    elif PRODUCER_MODE == "live":
        run_live_mode(producer)
    else:
        raise ValueError("PRODUCER_MODE must be either 'replay' or 'live'")


if __name__ == "__main__":
    main()