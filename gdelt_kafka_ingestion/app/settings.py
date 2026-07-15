import os
from pathlib import Path
from datetime import datetime, timedelta, timezone


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "environment.yaml"


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        return default

    return int(value)


def get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        return default

    return float(value)


def get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y"}


def round_down_to_15_minutes(dt: datetime) -> datetime:
    minute = dt.minute - (dt.minute % 15)
    return dt.replace(minute=minute, second=0, microsecond=0)


def default_start_end_dates() -> tuple[str, str]:
    end_dt = round_down_to_15_minutes(datetime.now(timezone.utc) - timedelta(hours=1))
    start_dt = end_dt - timedelta(days=7)

    return (
        start_dt.strftime("%Y%m%d%H%M%S"),
        end_dt.strftime("%Y%m%d%H%M%S"),
    )


DEFAULT_START_DATE, DEFAULT_END_DATE = default_start_end_dates()


KAFKA_BOOTSTRAP = get_env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = get_env("KAFKA_TOPIC", "gdelt-events-raw")

PRODUCER_MODE = get_env("PRODUCER_MODE", "replay").lower()
PUBLISH_RATE = get_float_env("PUBLISH_RATE", 10.0)
DATA_PATH = Path(get_env("DATA_PATH", "/data/gdelt"))
REPLAY_LOOP = get_bool_env("REPLAY_LOOP", True)

LIVE_POLL_SECONDS = get_int_env("LIVE_POLL_SECONDS", 900)

START_DATE = get_env("START_DATE", DEFAULT_START_DATE)
END_DATE = get_env("END_DATE", DEFAULT_END_DATE)

LOG_LEVEL = get_env("LOG_LEVEL", "INFO")

CONSUMER_MAX_MESSAGES = get_int_env("CONSUMER_MAX_MESSAGES", 10)