#!/usr/bin/env python3
"""
e2e_test.py
-----------
Self-contained end-to-end pipeline test for NL2Pipeline.
Runs inside Docker as part of docker-compose — no local Spark install needed.

Pipeline:
    1. Publish 100 synthetic GDELT events to Kafka
    2. Read from Kafka via Spark, aggregate avg_tone by country
    3. Write results to Cassandra aggregated_results table
    4. Verify rows landed and print summary

Usage (via docker-compose):
    docker compose run --rm e2e-test

Environment variables:
    KAFKA_BROKER          kafka:9092
    KAFKA_TOPIC           gdelt-events-raw
    CASSANDRA_HOST        cassandra
    CASSANDRA_PORT        9042
    CASSANDRA_KEYSPACE    nl2pipeline
    NUM_EVENTS            100
"""

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BROKER       = os.getenv("KAFKA_BROKER",       "kafka:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",         "gdelt-events-raw")
CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",      "cassandra")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT",  "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE",  "nl2pipeline")
CASSANDRA_TABLE    = "aggregated_results"
NUM_EVENTS         = int(os.getenv("NUM_EVENTS",      "100"))

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
# Synthetic data pools
# ---------------------------------------------------------------------------

COUNTRIES = ["USA", "GBR", "RUS", "CHN", "DEU", "FRA", "IND", "BRA", "AUS", "JPN"]
ACTORS    = ["GOVERNMENT", "PRESIDENT", "MILITARY", "POLICE", "REBEL", "CITIZEN", "MEDIA"]
EVENT_CODES = ["010", "020", "030", "040", "050", "060", "070", "080", "090", "100"]
LOCATIONS = {
    "USA": (38.9072,  -77.0369,  "Washington DC, USA"),
    "GBR": (51.5074,  -0.1278,   "London, UK"),
    "RUS": (55.7558,  37.6173,   "Moscow, Russia"),
    "CHN": (39.9042,  116.4074,  "Beijing, China"),
    "DEU": (52.5200,  13.4050,   "Berlin, Germany"),
    "FRA": (48.8566,  2.3522,    "Paris, France"),
    "IND": (28.6139,  77.2090,   "New Delhi, India"),
    "BRA": (-15.7975, -47.8919,  "Brasilia, Brazil"),
    "AUS": (-35.2809, 149.1300,  "Canberra, Australia"),
    "JPN": (35.6762,  139.6503,  "Tokyo, Japan"),
}

# ===========================================================================
# STEP 1 — Kafka Producer
# ===========================================================================

def make_event(event_id: int) -> dict:
    country = random.choice(COUNTRIES)
    lat, lon, _location = LOCATIONS[country]
    now = datetime.now(timezone.utc)
    return {
        "event_id":       str(event_id),
        "event_date":     now.strftime("%Y-%m-%d"),
        "actor1":         random.choice(ACTORS),
        "actor2":         random.choice(ACTORS),
        "event_code":     random.choice(EVENT_CODES),
        "country":        country,
        "lat":            lat,
        "lon":            lon,
        "tone":           round(random.uniform(-10.0, 10.0), 2),
        "num_mentions":   random.randint(1, 50),
        "num_sources":    random.randint(1, 10),
        "num_articles":   random.randint(1, 20),
        "is_root_event":  random.choice([True, False]),
        "ts":             now.isoformat().replace("+00:00", "Z"),
    }


def run_producer() -> None:
    log.info("=" * 60)
    log.info("STEP 1 — Kafka Producer")
    log.info(f"Broker: {KAFKA_BROKER}  Topic: {KAFKA_TOPIC}  Events: {NUM_EVENTS}")
    log.info("=" * 60)

    from confluent_kafka import Producer, KafkaException

    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if err:
            log.error(f"Delivery failed: {err}")
        else:
            delivered += 1

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    for i in range(1, NUM_EVENTS + 1):
        event = make_event(i)
        try:
            producer.produce(
                topic    = KAFKA_TOPIC,
                key      = event["event_id"],
                value    = json.dumps(event),
                callback = on_delivery,
            )
            producer.poll(0)
        except KafkaException as e:
            log.error(f"Failed to produce event {i}: {e}")
            sys.exit(1)

    producer.flush()
    log.info(f"STEP 1 PASSED — {delivered}/{NUM_EVENTS} events delivered to '{KAFKA_TOPIC}'")


# ===========================================================================
# STEP 2 — Spark Batch Job
# ===========================================================================

def run_spark_job() -> None:
    log.info("=" * 60)
    log.info("STEP 2 — Spark Batch Job")
    log.info(f"Kafka: {KAFKA_BROKER}/{KAFKA_TOPIC}  →  Cassandra: {CASSANDRA_TABLE}")
    log.info("=" * 60)

    log.info("Waiting 3s for Kafka offsets to commit ...")
    time.sleep(3)

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, avg, lit, current_timestamp
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, FloatType, IntegerType, BooleanType,
    )

    GDELT_SCHEMA = StructType([
        StructField("event_id",       StringType(),  False),
        StructField("event_date",     StringType(),  False),
        StructField("actor1",         StringType(),  True),
        StructField("actor2",         StringType(),  True),
        StructField("event_code",     StringType(),  True),
        StructField("country",        StringType(),  True),
        StructField("lat",            FloatType(),   True),
        StructField("lon",            FloatType(),   True),
        StructField("tone",           FloatType(),   True),
        StructField("num_mentions",   IntegerType(), True),
        StructField("num_sources",    IntegerType(), True),
        StructField("num_articles",   IntegerType(), True),
        StructField("is_root_event",  BooleanType(), True),
        StructField("ts",             StringType(),  False),
    ])

    spark = (
        SparkSession.builder
        .appName("NL2Pipeline-E2E-Test")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "earliest")
        .option("endingOffsets",           "latest")
        .load()
    )

    count = raw_df.count()
    log.info(f"Records read from Kafka: {count}")
    if count == 0:
        log.error("No records found in Kafka topic — did the producer run?")
        sys.exit(1)

    # Parse and aggregate
    agg_df = (
        raw_df
        .select(from_json(col("value").cast("string"), GDELT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("country").isNotNull())
        .filter(col("tone").isNotNull())
        .groupBy("country")
        .agg(avg("tone").alias("metric_value"))
        .withColumn("source_topic", lit(KAFKA_TOPIC))
        .withColumn("window_start", current_timestamp())
        .withColumn("window_end",   current_timestamp())
        .withColumn("metric_name",  lit("avg_tone"))
        .withColumn("group_key",    col("country"))
        .select(
            "source_topic", "window_start", "group_key",
            "metric_name", col("metric_value").cast("float"), "window_end",
        )
    )

    log.info("Aggregation results:")
    agg_df.show(truncate=False)

    # Write to Cassandra
    (
        agg_df.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table",    CASSANDRA_TABLE)
        .mode("append")
        .save()
    )

    spark.stop()
    log.info("STEP 2 PASSED — aggregated results written to Cassandra")


# ===========================================================================
# STEP 3 — Verify
# ===========================================================================

def run_verify() -> None:
    log.info("=" * 60)
    log.info("STEP 3 — Cassandra Verification")
    log.info("=" * 60)

    log.info("Waiting 2s for Cassandra write to propagate ...")
    time.sleep(2)

    from cassandra.cluster import Cluster, NoHostAvailable
    from cassandra import OperationTimedOut

    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
    except (NoHostAvailable, OperationTimedOut) as e:
        log.error(f"Could not connect to Cassandra: {e}")
        sys.exit(1)

    rows = list(session.execute("""
        SELECT group_key, metric_name, metric_value, window_start
        FROM aggregated_results
        WHERE source_topic = %s
        ALLOW FILTERING
    """, (KAFKA_TOPIC,)))

    if not rows:
        log.error("FAIL — No rows found in Cassandra.")
        sys.exit(1)

    log.info(f"{'Country':<15} {'Metric':<12} {'Value':>10}  Window Start")
    log.info("-" * 60)
    for row in rows:
        log.info(
            f"{row.group_key:<15} "
            f"{row.metric_name:<12} "
            f"{row.metric_value:>10.4f}  "
            f"{row.window_start}"
        )

    cluster.shutdown()
    log.info(f"STEP 3 PASSED — {len(rows)} country aggregations verified in Cassandra")


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    log.info("=" * 60)
    log.info("NL2Pipeline — End-to-End Pipeline Test")
    log.info("=" * 60)

    start = time.time()

    run_producer()
    run_spark_job()
    run_verify()

    elapsed = round(time.time() - start, 2)
    log.info("=" * 60)
    log.info(f"ALL STEPS PASSED — total time: {elapsed}s")
    log.info("=" * 60)


if __name__ == "__main__":
    main()