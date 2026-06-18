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
import uuid
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
    actor1_country = random.choice(COUNTRIES)
    actor2_country = random.choice(COUNTRIES)
    action_country = random.choice(COUNTRIES)
    lat, lon, location = LOCATIONS[action_country]
    now = datetime.now(timezone.utc)
    return {
        "global_event_id":  str(event_id),
        "event_date":       now.strftime("%Y-%m-%d"),
        "date_added":       now.strftime("%Y-%m-%d %H:%M:%S"),
        "actor1_country":   actor1_country,
        "actor1_name":      random.choice(ACTORS),
        "actor2_country":   actor2_country,
        "actor2_name":      random.choice(ACTORS),
        "event_code":       random.choice(EVENT_CODES),
        "event_base_code":  random.choice(EVENT_CODES),
        "event_root_code":  random.choice(EVENT_CODES),
        "quad_class":       random.randint(1, 4),
        "goldstein_scale":  round(random.uniform(-10.0, 10.0), 2),
        "num_mentions":     random.randint(1, 50),
        "num_sources":      random.randint(1, 10),
        "num_articles":     random.randint(1, 20),
        "avg_tone":         round(random.uniform(-10.0, 10.0), 2),
        "action_country":   action_country,
        "action_location":  location,
        "action_lat":       lat,
        "action_long":      lon,
        "source_url":       f"https://example-news.com/article/{uuid.uuid4().hex[:8]}",
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
                key      = event["global_event_id"],
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
        StringType, FloatType, IntegerType,
    )

    GDELT_SCHEMA = StructType([
        StructField("global_event_id",  StringType(),  False),
        StructField("event_date",       StringType(),  False),
        StructField("date_added",       StringType(),  False),
        StructField("actor1_country",   StringType(),  True),
        StructField("actor1_name",      StringType(),  True),
        StructField("actor2_country",   StringType(),  True),
        StructField("actor2_name",      StringType(),  True),
        StructField("event_code",       StringType(),  True),
        StructField("event_base_code",  StringType(),  True),
        StructField("event_root_code",  StringType(),  True),
        StructField("quad_class",       IntegerType(), True),
        StructField("goldstein_scale",  FloatType(),   True),
        StructField("num_mentions",     IntegerType(), True),
        StructField("num_sources",      IntegerType(), True),
        StructField("num_articles",     IntegerType(), True),
        StructField("avg_tone",         FloatType(),   True),
        StructField("action_country",   StringType(),  True),
        StructField("action_location",  StringType(),  True),
        StructField("action_lat",       FloatType(),   True),
        StructField("action_long",      FloatType(),   True),
        StructField("source_url",       StringType(),  True),
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
        .filter(col("action_country").isNotNull())
        .filter(col("avg_tone").isNotNull())
        .groupBy("action_country")
        .agg(avg("avg_tone").alias("metric_value"))
        .withColumn("source_topic", lit(KAFKA_TOPIC))
        .withColumn("window_start", current_timestamp())
        .withColumn("window_end",   current_timestamp())
        .withColumn("metric_name",  lit("avg_tone"))
        .withColumn("group_key",    col("action_country"))
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