from datetime import datetime, timezone


EXPECTED_FIELDS = [
    "event_id",
    "event_date",
    "actor1",
    "actor2",
    "event_code",
    "country",
    "lat",
    "lon",
    "tone",
    "num_mentions",
    "num_sources",
    "num_articles",
    "is_root_event",
    "ts",
]

# We need index 57 for longitude, so the row must have at least 58 columns.
MIN_REQUIRED_COLUMNS = 58


def parse_float(value: str):
    value = value.strip()

    if value == "":
        return None

    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value: str):
    value = value.strip()

    if value == "":
        return None

    try:
        return int(value)
    except ValueError:
        return None


def parse_bool(value: str) -> bool:
    return value.strip() == "1"


def parse_event_date(value: str) -> str:
    value = value.strip()

    if len(value) == 8 and value.isdigit():
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"

    return value


def ingest_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def map_gdelt_row_to_message(columns: list[str]) -> dict:
    if len(columns) < MIN_REQUIRED_COLUMNS:
        raise ValueError(
            f"Malformed GDELT row: expected at least {MIN_REQUIRED_COLUMNS} columns, got {len(columns)}"
        )

    message = {
        "event_id": columns[0].strip(),
        "event_date": parse_event_date(columns[1]),
        "actor1": columns[6].strip() or None,
        "actor2": columns[16].strip() or None,
        "event_code": columns[26].strip(),
        "country": columns[53].strip() or None,
        "lat": parse_float(columns[56]),
        "lon": parse_float(columns[57]),
        "tone": parse_float(columns[34]),
        "num_mentions": parse_int(columns[31]),
        "num_sources": parse_int(columns[32]),
        "num_articles": parse_int(columns[33]),
        "is_root_event": parse_bool(columns[25]),
        "ts": ingest_timestamp(),
    }

    validate_message_schema(message)

    return message


def validate_message_schema(message: dict) -> None:
    actual_fields = list(message.keys())

    if actual_fields != EXPECTED_FIELDS:
        raise ValueError(
            f"Invalid message fields. Expected {EXPECTED_FIELDS}, got {actual_fields}"
        )