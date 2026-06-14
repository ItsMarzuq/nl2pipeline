# NL2Pipeline

Natural Language-to-Code Framework for Big Data Orchestration

---

## Common Commands

```bash
# Start everything
docker compose up -d

# Stop everything (keep volumes)
docker compose down

# Stop and wipe all data (full reset)
docker compose down --volumes

# View logs for a specific service
docker logs nl2pipeline-kafka
docker logs nl2pipeline-cassandra
docker logs nl2pipeline-kafka-init
docker logs nl2pipeline-cassandra-init

# Rebuild a specific service after code changes
docker compose build --no-cache kafka-init
docker compose build --no-cache cassandra-init

# Run e2e test
docker compose --profile test run --rm e2e-test

# Check all service statuses
docker compose ps
```

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `gdelt-events-raw` | 6 | 7 days | GDELT global events stream |
| `pipeline-errors-dlq` | 1 | 30 days | Dead letter queue for failed messages |

---

## Cassandra Schema

Keyspace: `nl2pipeline`

| Table | Partition Key | Clustering Key | Purpose |
|---|---|---|---|
| `processed_events` | `(source_topic, event_date)` | `event_id` | Raw event sink for all source topics |
| `aggregated_results` | `(source_topic, window_start)` | `group_key, metric_name` | Spark aggregation output |
| `pipeline_runs` | `run_id` | — | SLM generation log — EX scoring and evaluation |