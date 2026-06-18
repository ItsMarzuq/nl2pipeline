import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests

from app.logging_config import setup_logging
from app.settings import DATA_PATH, START_DATE, END_DATE


GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"

logger = logging.getLogger(__name__)


def parse_gdelt_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d%H%M%S")


def generate_15_minute_timestamps(start_date: str, end_date: str):
    current = parse_gdelt_timestamp(start_date)
    end = parse_gdelt_timestamp(end_date)

    while current <= end:
        yield current.strftime("%Y%m%d%H%M%S")
        current += timedelta(minutes=15)


def download_and_extract(timestamp: str, output_dir: Path) -> str:
    output_tsv = output_dir / f"{timestamp}.export.tsv"

    if output_tsv.exists() and output_tsv.stat().st_size > 0:
        return "skipped"

    zip_url = f"{GDELT_BASE_URL}/{timestamp}.export.CSV.zip"
    temp_zip = output_dir / f"{timestamp}.export.CSV.zip.tmp"

    try:
        response = requests.get(zip_url, timeout=60)

        if response.status_code == 404:
            logger.warning("Archive gap: missing file %s", zip_url)
            return "failed"

        response.raise_for_status()

        temp_zip.write_bytes(response.content)

        with zipfile.ZipFile(temp_zip, "r") as zip_file:
            names = zip_file.namelist()

            if not names:
                logger.warning("Empty zip file for timestamp %s", timestamp)
                return "failed"

            inner_file_name = names[0]

            with zip_file.open(inner_file_name) as source:
                output_tsv.write_bytes(source.read())

        logger.info("Downloaded and extracted %s", output_tsv.name)
        return "downloaded"

    except requests.RequestException as exc:
        logger.warning("Network/download error for %s: %s", zip_url, exc)
        return "failed"

    except zipfile.BadZipFile as exc:
        logger.warning("Bad zip file for %s: %s", timestamp, exc)
        return "failed"

    finally:
        if temp_zip.exists():
            temp_zip.unlink()


def main() -> None:
    setup_logging()

    DATA_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("Starting GDELT download")
    logger.info("Date range: %s to %s", START_DATE, END_DATE)
    logger.info("Output directory: %s", DATA_PATH)

    downloaded = 0
    skipped = 0
    failed = 0
    processed = 0

    for timestamp in generate_15_minute_timestamps(START_DATE, END_DATE):
        status = download_and_extract(timestamp, DATA_PATH)

        if status == "downloaded":
            downloaded += 1
        elif status == "skipped":
            skipped += 1
        else:
            failed += 1

        processed += 1

        if processed % 50 == 0:
            logger.info(
                "Progress: processed=%s downloaded=%s skipped=%s failed=%s",
                processed,
                downloaded,
                skipped,
                failed,
            )

    logger.info("Download complete")
    logger.info("Total downloaded: %s", downloaded)
    logger.info("Total skipped: %s", skipped)
    logger.info("Total failed: %s", failed)


if __name__ == "__main__":
    main()