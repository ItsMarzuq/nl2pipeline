#!/usr/bin/env python3
"""
init_cassandra.py
-----------------
Initialises the Cassandra keyspace and fixed schema for NL2Pipeline.

Tables:
    processed_events    — generic sink for raw events from all source topics
    aggregated_results  — generic sink for Spark aggregation output
    pipeline_runs       — observability/evaluation log for all SLM generation attempts

Host:     Resolved from CASSANDRA_HOST env var (default: localhost)
Keyspace: nl2pipeline

Run this once at Docker startup before any producer or Spark job begins.

Requirements:
    pip install cassandra-driver
"""

import logging
import os
import sys
import time

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import OperationTimedOut

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE           = "nl2pipeline"
MAX_RETRIES        = 10
RETRY_INTERVAL_SEC = 5

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
# DDL
# ---------------------------------------------------------------------------

CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{
    'class': 'SimpleStrategy',
    'replication_factor': 1
}};
"""

# Generic event sink for raw records from all three source topics.
# Partitioned by (source_topic, event_date) so queries can efficiently
# retrieve all events from a given topic on a given day.
# event_id holds the natural key for each source:
#   gdelt-events-raw    → global_event_id
#   amazon-reviews-raw  → review_id
CREATE_PROCESSED_EVENTS = """
CREATE TABLE IF NOT EXISTS processed_events (
    source_topic    text,
    event_date      date,
    event_id        text,
    payload         text,
    ingested_at     timestamp,
    PRIMARY KEY ((source_topic, event_date), event_id)
) WITH CLUSTERING ORDER BY (event_id ASC);
"""

# Generic aggregation sink for Spark output.
# Partitioned by (source_topic, window_start) to avoid hotspotting —
# each topic's aggregations land in separate partitions.
# group_key holds the aggregation dimension (e.g. "actor1_country=USA")
# metric_name holds the measure (e.g. "avg_tone", "num_mentions")
CREATE_AGGREGATED_RESULTS = """
CREATE TABLE IF NOT EXISTS aggregated_results (
    source_topic    text,
    window_start    timestamp,
    group_key       text,
    metric_name     text,
    metric_value    float,
    window_end      timestamp,
    PRIMARY KEY ((source_topic, window_start), group_key, metric_name)
) WITH CLUSTERING ORDER BY (group_key ASC, metric_name ASC);
"""

# Observability and evaluation table.
# Records every SLM generation attempt — used for EX scoring,
# latency measurement, and SLM vs GPT-4o comparison in the final report.
# Not used by the error correction loop itself (that runs in memory)
# but persists results after each run for offline analysis.
CREATE_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          uuid,
    nl_prompt       text,
    generated_code  text,
    status          text,
    error_msg       text,
    attempt_number  int,
    model_used      text,
    latency_ms      int,
    created_at      timestamp,
    PRIMARY KEY (run_id)
);
"""

TABLES = [
    ("processed_events",   CREATE_PROCESSED_EVENTS),
    ("aggregated_results", CREATE_AGGREGATED_RESULTS),
    ("pipeline_runs",      CREATE_PIPELINE_RUNS),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_cassandra() -> tuple:
    """
    Poll Cassandra until it is ready, then return (cluster, session).
    Retries up to MAX_RETRIES times with RETRY_INTERVAL_SEC between attempts.
    Exits with code 1 if the broker never becomes available.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(
                f"Attempting to connect to Cassandra at "
                f"{CASSANDRA_HOST}:{CASSANDRA_PORT} "
                f"(attempt {attempt}/{MAX_RETRIES}) ..."
            )
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=10)
            session = cluster.connect()
            log.info("Connected to Cassandra successfully.")
            return cluster, session
        except (NoHostAvailable, OperationTimedOut, Exception) as e:
            log.warning(f"Cassandra not ready yet: {e}")
            if attempt == MAX_RETRIES:
                log.error("Max retries exceeded. Exiting.")
                sys.exit(1)
            time.sleep(RETRY_INTERVAL_SEC)


def create_keyspace(session) -> None:
    """Create the nl2pipeline keyspace if it does not already exist."""
    log.info(f"Creating keyspace '{KEYSPACE}' if it does not exist ...")
    session.execute(CREATE_KEYSPACE)
    session.set_keyspace(KEYSPACE)
    log.info(f"Using keyspace '{KEYSPACE}'.")


def create_tables(session) -> None:
    """Create all fixed tables if they do not already exist."""
    for table_name, ddl in TABLES:
        log.info(f"Creating table '{table_name}' if it does not exist ...")
        session.execute(ddl)
        log.info(f"Table '{table_name}' is ready.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("NL2Pipeline — Cassandra Schema Initialisation")
    log.info(f"Host: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    log.info("=" * 60)

    cluster, session = wait_for_cassandra()

    try:
        create_keyspace(session)
        create_tables(session)
        log.info("=" * 60)
        log.info("Schema initialisation complete. Ready for pipelines.")
        log.info("=" * 60)
    except Exception as e:
        log.error(f"Schema initialisation failed: {e}")
        sys.exit(1)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()