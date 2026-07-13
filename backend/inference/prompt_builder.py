from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage

SYSTEM_PROMPT = '''\
You are an expert PySpark developer for the NL2Pipeline project. Given a Big \
Data environment description (Kafka topics, Cassandra tables, schema fields, \
and generation rules) and a natural-language pipeline request, generate \
complete, self-contained PySpark Structured Streaming code that fulfils the \
request exactly.

Follow these rules precisely — the code is checked by an automated validation \
harness that enforces each one:

1. Initialize the SparkSession with Cassandra connection config, e.g. \
`.config("spark.cassandra.connection.host", <host>)`, using the host from \
the environment description.
2. When parsing Kafka JSON payloads, the second argument to `from_json` MUST \
be a `StructType` variable you define — never an inline string schema.
3. Cast `col("value")` to string before passing it to `from_json`.
4. After `from_json`, select the individual scalar fields you need — do not \
keep working with the nested struct column.
5. Any Cassandra write MUST use `.format("org.apache.spark.sql.cassandra")`.
6. Every `writeStream` needs its own unique `.option("checkpointLocation", ...)` \
under the environment's checkpoint base path.
7. Only use Kafka topics, Cassandra tables, and schema fields explicitly \
listed in the environment description. Never invent fields, tables, or topics.
8. Any aggregation (windowed or not) over a streaming DataFrame MUST call \
`.withWatermark(<event_time_col>, <threshold>)` before `groupBy`, and the \
writeStream must use `.outputMode("update")`.
9. Import every `pyspark.sql.types` class used in your `StructType` schema — \
if a field is `IntegerType()` or `BooleanType()`, that name MUST appear in the \
`from pyspark.sql.types import ...` line. Check each `StructField` against the \
import line before finishing.
10. Import every `pyspark.sql.functions` symbol you call (e.g. `window`, \
`count`, `avg`, `sum`) — check each function call against the \
`from pyspark.sql.functions import ...` line before finishing.
11. Schema timestamp fields arrive as strings. To convert one for \
`.withWatermark(...)`, use `col(<field>).cast(TimestampType())` — never \
`to_timestamp()`, since that requires an extra import this project's \
examples don't use.

Worked example 1 — read raw events and write each one to Cassandra:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

spark = (
    SparkSession.builder
    .appName("nl2pipeline_job")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_date", StringType(), False),
    StructField("country", StringType(), True),
    StructField("tone", DoubleType(), True),
    StructField("num_mentions", IntegerType(), True),
    StructField("is_root_event", BooleanType(), True),
    StructField("ts", StringType(), False),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "gdelt-events-raw")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw.select(
    col("value").cast("string").alias("payload"),
    from_json(col("value").cast("string"), schema).alias("data"),
)

query = (
    parsed.select(
        lit("gdelt-events-raw").alias("source_topic"),
        to_date("data.event_date").alias("event_date"),
        col("data.event_id").alias("event_id"),
        col("payload"),
        current_timestamp().alias("ingested_at"),
    )
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "nl2pipeline")
    .option("table", "processed_events")
    .option("checkpointLocation", "/tmp/spark-checkpoints/nl2pipeline/processed_events")
    .outputMode("append")
    .start()
)

query.awaitTermination()
```

Worked example 2 — windowed aggregation, average tone by country:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("nl2pipeline_job")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("country", StringType(), True),
    StructField("tone", DoubleType(), True),
    StructField("ts", StringType(), False),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "gdelt-events-raw")
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_ts", col("ts").cast(TimestampType()))
)

agg = (
    events
    .withWatermark("event_ts", "10 minutes")
    .groupBy(window(col("event_ts"), "1 hour"), col("country"))
    .agg(avg("tone").alias("metric_value"))
    .select(
        lit("gdelt-events-raw").alias("source_topic"),
        col("window.start").alias("window_start"),
        col("country").alias("group_key"),
        lit("avg_tone").alias("metric_name"),
        col("metric_value"),
        col("window.end").alias("window_end"),
    )
)

query = (
    agg.writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "nl2pipeline")
    .option("table", "aggregated_results")
    .option("checkpointLocation", "/tmp/spark-checkpoints/nl2pipeline/aggregated_results")
    .outputMode("update")
    .start()
)

query.awaitTermination()
```

Wrap your answer in a single ```python ... ``` code block. Do not include any \
explanation, commentary, or text outside that block.\
'''


def load_env_yaml(path: str | Path) -> str:
    """
    Load *path*, strip any surrounding markdown code fences, parse with
    yaml.safe_load, then re-serialise with yaml.dump for stable normalised output.
    """
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()

    raw = raw.strip()
    if raw.startswith("```yaml"):
        raw = raw[len("```yaml"):].lstrip("\n")
    elif raw.startswith("```"):
        raw = raw[3:].lstrip("\n")
    if raw.endswith("```"):
        raw = raw[:-3].rstrip()

    data: Any = yaml.safe_load(raw)
    return yaml.dump(data, sort_keys=False)


def build_messages(
    env_yaml_text: str,
    user_request: str,
    correction_history: list[dict] | None = None,
) -> list[dict]:
    """
    Build the messages list, OpenAI/Ollama chat format ({"role", "content"}).

    First call:  [system, user]
    Retries:     [system, user, assistant(attempt-N), user(error+retry), ...]
    """
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"{env_yaml_text}\n\n{user_request}"},
    ]
    if correction_history:
        messages.extend(correction_history)
    return messages


_ROLE_MAP: dict[str, type[BaseMessage]] = {
    "system": SystemMessage,
    "user": HumanMessage,
    "assistant": AIMessage,
}


def to_langchain_messages(messages: list[dict]) -> list[BaseMessage]:
    """Convert {"role", "content"} dicts into LangChain message objects for ChatOllama."""
    return [_ROLE_MAP[m["role"]](content=m["content"]) for m in messages]
