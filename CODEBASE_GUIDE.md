# NL2Pipeline — Codebase Guide

> **Audience:** Someone joining the project for the first time with Python + Docker familiarity but no prior context on the system.

---

## Table of Contents

1. [What Is This Project?](#1-what-is-this-project)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Directory Structure](#3-directory-structure)
4. [Data Infrastructure](#4-data-infrastructure)
   - [Kafka](#41-kafka)
   - [Cassandra](#42-cassandra)
   - [Spark](#43-spark)
5. [GDELT Data Ingestion](#5-gdelt-data-ingestion)
6. [The Backend — Core System](#6-the-backend--core-system)
   - [FastAPI Application](#61-fastapi-application-backendapppy)
   - [Configuration](#62-configuration-backendconfigpy--env)
   - [Environment Metadata](#63-environment-metadata-backendmetadatayaml)
   - [Model Loading](#64-model-loading-backendinterencemodel_loaderpy)
   - [Prompt Builder](#65-prompt-builder-backendinterenceprompt_builderpy)
   - [Generation Engine](#66-generation-engine-backendinterenceenginepy)
   - [Code Parser](#67-code-parser-backendinterencecode_parserpy)
   - [Validation Harness](#68-validation-harness-backendvalidationharnesspy)
7. [Docker Setup](#7-docker-setup)
   - [Services Overview](#71-services-overview)
   - [Backend Dockerfile](#72-backend-dockerfile)
   - [Other Dockerfiles](#73-other-dockerfiles)
   - [Startup Sequence](#74-startup-sequence)
8. [Dataset Generation](#8-dataset-generation)
9. [End-to-End Test](#9-end-to-end-test)
10. [Request Lifecycle (Full Walk-through)](#10-request-lifecycle-full-walk-through)
11. [Configuration Reference](#11-configuration-reference)
12. [Tech Stack Summary](#12-tech-stack-summary)
13. [Deployment Requirements](#13-deployment-requirements)
14. [Quick Start](#14-quick-start)

---

## 1. What Is This Project?

**NL2Pipeline** is a **Natural Language to PySpark Pipeline Generator**.

A user writes a plain-English description of a data processing task, for example:

> *"Read GDELT events from Kafka and aggregate average tone by country using 1-hour tumbling windows."*

The system automatically generates a complete, executable **PySpark Structured Streaming** script that:

- Reads from an **Apache Kafka** topic (`gdelt-events-raw`)
- Processes data using **Spark Structured Streaming**
- Writes results to **Apache Cassandra**

Before returning the code, the system runs it through a **3-stage validation harness** with **automatic self-correction** — if the generated code fails a stage, the error is fed back to the language model and it retries (up to 3 times).

The language model used is **Qwen3.5-4B**, served locally by **Ollama** (GGUF quantized, running on `llama.cpp` under the hood) rather than loaded in-process via HuggingFace `transformers`.

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Docker Compose Stack                           │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                        Data Layer                                │    │
│  │                                                                  │    │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐ │    │
│  │   │  Zookeeper   │───▶│    Kafka     │◀───│  GDELT Producer  │ │    │
│  │   │  (coord.)    │    │  (broker)    │    │  (event stream)  │ │    │
│  │   └──────────────┘    └──────┬───────┘    └──────────────────┘ │    │
│  │                              │ topic: gdelt-events-raw           │    │
│  │                              ▼                                   │    │
│  │   ┌──────────────────────────────────────┐                      │    │
│  │   │          Apache Spark Cluster        │                      │    │
│  │   │   spark-master (7077) + spark-worker │                      │    │
│  │   └──────────────────────────────────────┘                      │    │
│  │                              │                                   │    │
│  │                              ▼                                   │    │
│  │   ┌──────────────────────────────────────┐                      │    │
│  │   │          Apache Cassandra            │                      │    │
│  │   │     keyspace: nl2pipeline (9042)     │                      │    │
│  │   └──────────────────────────────────────┘                      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                   Backend (port 8000)                            │    │
│  │                                                                  │    │
│  │   FastAPI ──▶ GenerationEngine ──HTTP──▶ Ollama (11434)         │    │
│  │                      │                    Qwen3.5-4B (GGUF)      │    │
│  │                      ▼                                           │    │
│  │              Validation Harness                                  │    │
│  │         [Stage 1: Ruff lint]                                     │    │
│  │         [Stage 2: spark-submit (local[*])]                      │    │
│  │         [Stage 3: Cassandra schema check]                        │    │
│  │                      │                                           │    │
│  │              Pass ───┘                                           │    │
│  │              Fail ──▶ Append error ──▶ Retry (max 3x)           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                              ▲   │                                       │
│                    SSE stream│   │ POST /generate                        │
│                              │   ▼                                       │
│                          User / Frontend                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

**Note:** `ollama` is a separate container (own GPU reservation + model volume) — the backend talks to it over HTTP, it does not load the model in-process.

**Communication pattern:** The backend streams events back to the client using **Server-Sent Events (SSE)** — you get real-time progress updates (lint passed, spark running, done) rather than waiting for a single response.

---

## 3. Directory Structure

```
nl2pipeline/
│
├── backend/                        # Core FastAPI + LLM inference server
│   ├── app.py                      # FastAPI app, endpoint definitions
│   ├── config.py                   # Pydantic settings (reads from .env)
│   ├── metadata.yaml               # Source-of-truth: Kafka topics, Cassandra schema
│   ├── Dockerfile                  # python:3.11-slim (no CUDA/torch — Ollama owns that)
│   ├── requirements.txt
│   ├── cassandra_client.py         # Shared Cassandra session, non-fatal if unreachable
│   ├── inference/
│   │   ├── engine.py               # Self-correction loop, SSE event generator
│   │   ├── model_loader.py         # ChatOllama client (talks to the `ollama` service)
│   │   ├── prompt_builder.py       # System prompt + message assembly
│   │   └── code_parser.py          # Extract ```python block from LLM output
│   ├── validation/
│   │   ├── harness.py              # 3-stage validation pipeline
│   │   └── run_log.py              # Logs each generation run to pipeline_runs
│   └── tests/                      # Unit tests (code_parser, engine, harness)
│
├── gdelt_kafka_ingestion/          # GDELT event producer service
│   ├── app/
│   │   ├── producer.py             # Kafka producer (replay or live mode)
│   │   ├── downloader.py           # Downloads GDELT HTTP archives
│   │   ├── gdelt_mapper.py         # Maps 65-column TSV → 14-field JSON
│   │   ├── consumer_test.py        # Debug utility to consume and print events
│   │   ├── settings.py             # Config from environment variables
│   │   └── logging_config.py
│   ├── config/
│   │   └── environment.yaml        # GDELT field mapping definitions
│   ├── .env.example
│   └── Dockerfile
│
├── startup_scripts/                # One-shot init containers (run once at stack start)
│   ├── init_kafka_topics.py        # Creates Kafka topics (gdelt-events-raw, DLQ)
│   ├── init_cassandra.py           # Creates keyspace + 3 tables in Cassandra
│   └── Dockerfile
│
├── dataset_generation/             # Training data generation (offline tooling)
│   ├── generate_gdelt_pairs.py     # Uses OpenAI to generate NL ↔ PySpark pairs
│   ├── validate_gdelt_pairs.py     # Validates the generated JSONL dataset
│   ├── gdelt_pairs.jsonl           # Output dataset (NL/code pairs for fine-tuning)
│   └── gdelt_environment.yaml      # Environment template for synthetic data
│
├── dev/e2e_test/                   # Integration test (full stack, no backend)
│   ├── e2e_test.py                 # Produces events → Spark aggregation → Cassandra verify
│   └── Dockerfile
│
├── docker-compose.yaml             # Full 11-service stack orchestration
├── .env.example                    # Template for environment variables
└── .env                            # Your local config (gitignored)
```

---

## 4. Data Infrastructure

### 4.1 Kafka

**Role:** Message broker — the real-time pipe between the GDELT producer and any Spark streaming job.

**Services:**
- `zookeeper` — Kafka's coordination service (required by Confluent Kafka 7.x)
- `kafka` — The actual broker, listening on:
  - Port `9092` — internal Docker network (other containers use this)
  - Port `29092` — exposed to host (for local tools like `kcat`)

**Topics created by `startup_scripts/init_kafka_topics.py`:**

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `gdelt-events-raw` | 3 | 7 days | Source stream; consumed by generated Spark pipelines |
| `pipeline-errors-dlq` | 1 | 30 days | Dead-letter queue (reserved for future error routing) |

### 4.2 Cassandra

**Role:** Persistent storage for processed events and aggregation results.

**Keyspace:** `nl2pipeline` (SimpleStrategy, replication_factor=1 — single node for dev)

**Tables created by `startup_scripts/init_cassandra.py`:**

**`processed_events`** — stores raw ingested events from Kafka
```
PRIMARY KEY ((source_topic, event_date), event_id)

Columns:
  source_topic  TEXT      -- e.g. "gdelt-events-raw"
  event_date    DATE      -- partition key for time-based queries
  event_id      TEXT      -- clustering key (unique per event)
  payload       TEXT      -- full JSON of the event
  ingested_at   TIMESTAMP -- when Spark wrote this row
```

**`aggregated_results`** — stores Spark windowed aggregations
```
PRIMARY KEY ((source_topic, window_start), group_key, metric_name)

Columns:
  source_topic  TEXT      -- e.g. "gdelt-events-raw"
  window_start  TIMESTAMP -- partition key (start of time window)
  group_key     TEXT      -- e.g. country code "USA"
  metric_name   TEXT      -- e.g. "avg_tone"
  metric_value  FLOAT     -- the computed value
  window_end    TIMESTAMP -- end of time window
```

**`pipeline_runs`** — observability log of every generation attempt
```
PRIMARY KEY (run_id)

Columns:
  run_id          UUID
  nl_prompt       TEXT
  generated_code  TEXT
  status          TEXT      -- "success" | "failed"
  error_msg       TEXT
  attempt_number  INT
  model_used      TEXT
  latency_ms      INT
  created_at      TIMESTAMP
```

### 4.3 Spark

**Role:** Distributed stream processing — executes the generated PySpark code.

**Services:**
- `spark` — Master node (web UI on port 8081, cluster port 7077)
- `spark-worker` — Worker node (web UI on port 8082)

**Connectors pre-installed:**
- `spark-sql-kafka-0-10_2.12:3.5.1` — reads from Kafka
- `spark-cassandra-connector_2.12:3.5.1` — writes to Cassandra

The validation harness runs `spark-submit` in **`local[*]`** mode (single machine, all CPU cores) — separate from the Spark cluster. The cluster is available for actually running generated pipelines in production.

---

## 5. GDELT Data Ingestion

**GDELT (Global Database of Events, Language, and Tone)** is a free public dataset of world events extracted from news media. It produces a new export file every 15 minutes, with each row representing a news event.

The raw format is a 65-column TSV. The producer extracts 14 meaningful fields.

### GDELT Mapper (`gdelt_kafka_ingestion/app/gdelt_mapper.py`)

Maps column indices to fields:

```python
{
  "event_id":      columns[0],          # Unique event ID
  "event_date":    columns[1],          # YYYYMMDD → YYYY-MM-DD
  "actor1":        columns[6],          # Actor 1 name (e.g. "UNITED STATES")
  "actor2":        columns[16],         # Actor 2 name
  "event_code":    columns[26],         # CAMEO event code (e.g. "036")
  "country":       columns[53],         # Action geo country code
  "lat":           columns[56],         # Latitude (float)
  "lon":           columns[57],         # Longitude (float)
  "tone":          columns[34],         # Average tone (-100 to +100)
  "num_mentions":  columns[31],         # Number of news mentions
  "num_sources":   columns[32],         # Number of news sources
  "num_articles":  columns[33],         # Number of articles
  "is_root_event": columns[25],         # Whether this is a root event
  "ts":            utcnow().isoformat() # When the producer published it
}
```

### Producer Modes (`gdelt_kafka_ingestion/app/producer.py`)

**Replay Mode** (`PRODUCER_MODE=replay`) — default for development:
- Downloads GDELT archive files from the HTTP archive
- Parses TSV files from `/data/gdelt` volume
- Publishes at configurable rate (`PUBLISH_RATE`, default 10 events/sec)
- If `REPLAY_LOOP=true`: loops forever; otherwise exits after one pass

**Live Mode** (`PRODUCER_MODE=live`):
- Polls the GDELT 15-minute update feed every `LIVE_POLL_SECONDS` (default 900)
- Downloads and processes new export files as they appear
- Runs indefinitely

Messages are published to topic `gdelt-events-raw` with `event_id` as the Kafka key (ensures consistent partitioning per event).

---

## 6. The Backend — Core System

The backend is a **FastAPI application** (`backend/app.py`) that orchestrates the full pipeline: receive a user prompt → build a prompt → invoke the LLM → validate the output → return code via SSE stream.

### 6.1 FastAPI Application (`backend/app.py`)

**Port:** 8000

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Returns `{"status": "ok"}` — used by Docker healthcheck |
| `GET` | `/environments` | Returns the active `metadata.yaml` as a JSON object |
| `POST` | `/generate` | Main endpoint — streams code generation events via SSE |

**`/generate` request body:**
```json
{
  "prompt": "Aggregate average tone by country using 1-hour windows",
  "environment_override": null
}
```
`environment_override` is an optional YAML string — if provided, it overrides `metadata.yaml` for this request only. Useful for testing different schemas without restarting the server.

**Lifespan (startup/shutdown):**
```python
@asynccontextmanager
async def lifespan(app):
    # On startup:
    metadata = load_yaml("backend/metadata.yaml")
    app.state.metadata = metadata

    llm = load_llm(settings.OLLAMA_BASE_URL, settings.OLLAMA_MODEL,
                   settings.MAX_NEW_TOKENS, settings.OLLAMA_NUM_CTX)
    # Just wires up a ChatOllama HTTP client — the model itself is already
    # loaded and warm on the `ollama` service (pulled once by `ollama-pull`).

    cassandra_session = connect_cassandra(settings.CASSANDRA_HOST, ...)
    # Non-fatal — returns None if Cassandra is unreachable or the driver
    # isn't installed (see backend/cassandra_client.py).

    app.state.engine = GenerationEngine(llm, metadata, settings.MAX_ATTEMPTS,
                                         cassandra_session=cassandra_session)
    yield
    # On shutdown: close the Cassandra session if one was opened
    if cassandra_session is not None:
        cassandra_session.cluster.shutdown()
```

### 6.2 Configuration (`backend/config.py` + `.env`)

Uses **Pydantic Settings** — reads from environment variables (which come from `.env` via Docker Compose).

```python
class Settings(BaseSettings):
    OLLAMA_BASE_URL: str = "http://ollama:11434"  # Ollama server address
    OLLAMA_MODEL: str = "qwen3.5:4b"               # Model tag to run
    ENV_YAML_PATH: str = "backend/metadata.yaml"
    MAX_NEW_TOKENS: int = 1024            # Token budget per LLM call
    OLLAMA_NUM_CTX: int = 6144            # Context window (tokens) to load the model with
    MAX_ATTEMPTS: int = 3                 # Self-correction retry limit
    MOCK_LLM: bool = False               # Skip model load (for CI/testing)

    # Cassandra connection, shared by the Stage 3 harness check and the
    # pipeline_runs observability log (see backend/cassandra_client.py)
    CASSANDRA_HOST: str = "cassandra"
    CASSANDRA_PORT: int = 9042
    CASSANDRA_USERNAME: str = ""
    CASSANDRA_PASSWORD: str = ""
    CASSANDRA_KEYSPACE: str = "nl2pipeline"
```

When `MOCK_LLM=true`, the LLM is bypassed and a pre-written PySpark script is returned. This lets you test the validation harness and API without needing a GPU.

### 6.3 Environment Metadata (`backend/metadata.yaml`)

This is the **single source of truth** for what Kafka topics, Cassandra tables, and schema fields exist. It is:
- Injected into every LLM prompt (so the model knows what resources it can use)
- Used by the validation harness (to check Cassandra table references)
- Returned by the `/environments` endpoint

Key sections:
```yaml
kafka:
  bootstrap_servers: kafka:9092
  topics:
    - gdelt-events-raw

cassandra:
  contact_points: cassandra
  port: 9042
  keyspace: nl2pipeline

gdelt_schema:           # The 14-field schema the model must use
  - {name: event_id, type: string}
  - {name: tone, type: double}
  - ... (14 fields total)

cassandra_tables:       # The tables the model is allowed to write to
  - name: processed_events
    columns: {source_topic: text, event_date: date, ...}
  - name: aggregated_results
    columns: {source_topic: text, window_start: timestamp, ...}
```

### 6.4 Model Loading (`backend/inference/model_loader.py`)

**Function:** `load_llm(base_url, model, max_new_tokens) → ChatOllama`

The backend doesn't load model weights itself — the `ollama` container does that. `load_llm` just builds a `langchain_ollama.ChatOllama` HTTP client pointed at `OLLAMA_BASE_URL`, configured with `num_predict=max_new_tokens` and `temperature=0` (greedy, deterministic decoding).

- **Base model:** Qwen3.5-4B
- **Quantization / serving:** GGUF, served by Ollama (`llama.cpp` backend) — the `ollama-pull` init service pulls the `OLLAMA_MODEL` tag into a Docker volume once; the `ollama` service loads and caches it in memory, GPU-accelerated via its own CUDA reservation
- **Fine-tuning:** trained separately with `transformers` + `peft` (LoRA), then merged, converted to GGUF, and registered with `ollama create` as a new tag — `OLLAMA_MODEL` is swapped to that tag rather than a runtime `adapter_path`
- **Device:** the `ollama` service uses CUDA if the GPU reservation is available; falls back to CPU otherwise (slow)

Because the model is a separately-running, always-warm server, the *backend* has near-zero startup cost — connecting is just constructing an HTTP client. The one-time cost is Ollama loading the model into memory the first time it's called, and the one-time `ollama pull` download (~3.4 GB for `qwen3.5:4b`) on first stack start.

### 6.5 Prompt Builder (`backend/inference/prompt_builder.py`)

Constructs the full input for the LLM on each generation attempt.

**System Prompt** (hardcoded, embedded in the file) contains 8 rules the model must follow:

| Rule | Requirement |
|------|-------------|
| R1 | Always initialize SparkSession with Cassandra config |
| R2 | `from_json` second argument MUST be a `StructType` variable |
| R3 | Cast `col("value")` to string before `from_json` |
| R4 | Select individual scalar fields after `from_json` |
| R5 | Cassandra writeStream MUST use `.format("org.apache.spark.sql.cassandra")` |
| R6 | One unique `checkpointLocation` per `writeStream` |
| R7 | Only use Kafka topics / Cassandra tables / fields from the environment YAML |
| R8 | Aggregations MUST use `.withWatermark()` before `groupBy` + `.outputMode("update")` |

The system prompt also includes two **worked examples** (raw event ingestion and time-windowed aggregation) so the model has concrete patterns to follow.

**Message structure for each attempt:**
```python
messages = [
    {"role": "system", "content": SYSTEM_PROMPT},
    {"role": "user", "content": f"{env_yaml_string}\n\n{user_request}"},
    # On retry attempt 2+, correction history is appended:
    {"role": "assistant", "content": previous_code},
    {"role": "user", "content": "CORRECTION: Stage X failed:\n{error_message}"}
]
```

Unlike the previous HuggingFace `transformers` setup, there's no manual chat-template formatting step — `to_langchain_messages()` converts each `{"role", "content"}` dict into a LangChain `SystemMessage`/`HumanMessage`/`AIMessage`, and Ollama applies the model's chat template server-side when it receives the messages.

### 6.6 Generation Engine (`backend/inference/engine.py`)

**Class:** `GenerationEngine`
**Method:** `async def run(user_request, env_yaml) → AsyncGenerator[dict]`

This is an **async generator** that yields event dictionaries. The FastAPI endpoint converts these to SSE events and streams them to the client.

**Self-correction loop:**

```
for attempt in range(1, MAX_ATTEMPTS + 1):

    1. Build messages list (prompt builder)
    2. Convert to LangChain message objects (to_langchain_messages)
    3. Invoke ChatOllama → AIMessage.content as raw_output string
       → yield {"event": "inference_complete", "attempt": attempt, "code": ...}

    4. Parse code block from raw_output
       - If no ```python fence found:
           → append "wrap output in ```python block" to correction history
           → continue to next attempt

    5. Run validation harness (3 stages)

    6. Stage 1 result:
       - Pass → yield {"event": "lint_passed", ...}
       - Fail → yield {"event": "lint_failed", "error": ruff_output}
                 append error to correction_history, continue

    7. Stage 2 result:
       - Pass → yield {"event": "docker_passed", ...}
       - Fail → yield {"event": "docker_failed", "error": spark_stderr}
                 append error to correction_history, continue

    8. Stage 3 result:
       - Pass → yield {"event": "cassandra_passed", ...}
       - Fail → yield {"event": "cassandra_failed", "error": missing_tables}
                 append error to correction_history, continue

    9. All stages passed:
       → yield {"event": "done", "code": code, "attempts": attempt, "latency_ms": ...}
       → return

After max attempts exhausted:
    → yield {"event": "done", "code": last_code, "attempts": MAX_ATTEMPTS, "error": ...}
```

**Mock mode:** If `MOCK_LLM=true`, step 3 is replaced with returning a pre-written PySpark script, bypassing model inference entirely.

### 6.7 Code Parser (`backend/inference/code_parser.py`)

Simple regex extraction:

```python
def extract_code(text: str) -> str | None:
    match = re.search(r"```python\n(.*?)```", text, re.DOTALL)
    return match.group(1).strip() if match else None
```

If the model returns code without a fence (which happens occasionally), the engine catches `None` and feeds a correction message: *"Wrap your output in a ```python code block."*

### 6.8 Validation Harness (`backend/validation/harness.py`)

**Function:** `validate(code: str, metadata: dict) → ValidationResult`

Runs 3 stages in order, stopping on first failure.

---

**Stage 1: Ruff Lint** (timeout: 30 seconds)

```bash
ruff check --select=E,F,W --ignore=F401 /tmp/<uuid>.py
```

- Catches syntax errors, undefined names, import problems
- F401 (unused imports) is ignored — model often imports things it may need
- Returns exact Ruff output as the error message for correction

---

**Stage 2: `spark-submit`** (two-phase timeout)

```bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,...
  --conf spark.cassandra.connection.host=cassandra \
  /tmp/<uuid>.py
```

Uses a **two-phase timeout** strategy because Spark streaming jobs block forever on `awaitTermination()`:
- **Phase 1 — 15 seconds:** Wait for Spark to start up, import packages, connect to Kafka
  - If the process exits during this phase: it failed (import error, config error)
  - If still running after 15s: continue to phase 2
- **Phase 2 — 15 additional seconds:** Let the job run briefly to catch runtime errors
  - If the process exits: it failed (runtime exception)
  - If still running after both phases: treat as **pass** (it's blocked on `awaitTermination()`, which is correct behavior for a streaming job)

Stderr is captured and the first 50 lines are returned as the error message for correction.

---

**Stage 3: Cassandra Schema Check** (no subprocess, direct query)

```python
# Extract table references from code via regex
referenced = re.findall(r'\.table\("(\w+)"\)', code)

# Query live Cassandra for existing tables in the keyspace
existing = cassandra_session.execute(
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'nl2pipeline'"
)

# Report any referenced tables that don't exist
missing = [t for t in referenced if t not in existing]
```

If the Cassandra cluster is unreachable or the `cassandra-driver` package isn't installed, this stage is **non-fatal** — a warning is logged and validation passes.

---

## 7. Docker Setup

### 7.1 Services Overview

11 services are defined in `docker-compose.yaml` (plus a few profile-gated extras) and communicate over a shared bridge network `nl2pipeline-net`.

| Service | Image | Ports | Role |
|---------|-------|-------|------|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination (internal) |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 (internal), 29092 (host) | Event streaming broker |
| `cassandra` | cassandra:4.1 | 9042 | Database for results |
| `spark` | apache/spark:3.5.1 | 7077 (cluster), 8081 (web UI) | Spark master |
| `spark-worker` | apache/spark:latest | 8082 (web UI) | Spark worker |
| `kafka-init` | Custom | — | One-shot: creates Kafka topics |
| `cassandra-init` | Custom | — | One-shot: creates keyspace + tables |
| `gdelt-producer` | Custom | — | Continuous: publishes GDELT events |
| `ollama` | ollama/ollama:latest | 11434 (internal) | Serves Qwen3.5-4B (GGUF), owns the GPU reservation |
| `ollama-pull` | ollama/ollama:latest | — | One-shot: `ollama pull ${OLLAMA_MODEL}` |
| `backend` | Custom (python:3.11-slim) | 8000 | FastAPI + GenerationEngine (calls Ollama over HTTP) |
| `e2e-test` | Custom (Spark) | — | Profile `test`: integration test |

### 7.2 Backend Dockerfile

```dockerfile
# Base: plain Python — no CUDA/torch needed, the backend only speaks HTTP to Ollama
FROM python:3.11-slim

# System dependencies
RUN apt-get install -y gcc g++ python3-venv openjdk-17-jre-headless

# Java is required for spark-submit inside the harness
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
# Key packages:
#   langchain-core, langchain-ollama              (LLM client — model itself is served by `ollama`)
#   fastapi, uvicorn                               (API server)
#   ruff                                           (lint validation)
#   pyspark==3.5.1                                 (Spark validation)
#   cassandra-driver                               (schema validation)

# Pre-download Spark connector JARs at build time
# (so harness doesn't need internet access at runtime)
RUN spark-submit --packages org.apache.spark:spark-sql-kafka-... --dry-run /dev/null
# JARs cached to /opt/spark-jars

# Application code
COPY backend/ /app/backend/

EXPOSE 8000
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**GPU configuration in docker-compose.yaml:** the GPU reservation now lives on the `ollama` service, not `backend` — the backend container never touches CUDA:
```yaml
ollama:
  image: ollama/ollama:latest
  volumes:
    - ollama-data:/root/.ollama          # Pulled models, cached across restarts
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: 1
            capabilities: [gpu]

ollama-pull:
  image: ollama/ollama:latest
  depends_on:
    ollama:
      condition: service_healthy
  command: ["pull", "${OLLAMA_MODEL:-qwen3.5:4b}"]

backend:
  depends_on:
    ollama-pull:
      condition: service_completed_successfully
  environment:
    - OLLAMA_BASE_URL=http://ollama:11434
    - OLLAMA_MODEL=${OLLAMA_MODEL:-qwen3.5:4b}
```

**No Docker socket mount.** `spark-submit` in the validation harness runs as a plain subprocess *inside* the `backend` container itself (`local[*]` mode, against the container's own bundled Spark install) — it does not spin up sibling containers, so `backend` never needs `/var/run/docker.sock`.

### 7.3 Other Dockerfiles

**`startup_scripts/Dockerfile`:**
- Base: `python:3.11-slim`
- Installs `confluent-kafka` + `cassandra-driver`
- Runs `init_kafka_topics.py` or `init_cassandra.py` depending on entry point
- Exits with code 0 on success (Docker marks as `completed_successfully`)

**`gdelt_kafka_ingestion/Dockerfile`:**
- Base: `python:3.11-slim`
- Installs `confluent-kafka`, `requests`
- Runs `producer.py` (long-running process)

**`dev/e2e_test/Dockerfile`:**
- Base: `apache/spark:3.5.1`
- Adds `cassandra-driver`
- Profile: `test` — only starts when you run `docker compose --profile test ...`

### 7.4 Startup Sequence

Docker Compose `depends_on` + `healthcheck` enforces this order:

```
Step 1:  zookeeper starts
           ↓
Step 2:  kafka starts, waits for zookeeper healthcheck
           ↓
Step 3:  kafka-init runs (creates topics), exits 0
cassandra starts (takes ~60 seconds to be ready)
ollama starts, waits for healthcheck (`ollama list`)
           ↓
Step 4:  cassandra-init runs (creates keyspace + tables), exits 0
ollama-pull runs (`ollama pull ${OLLAMA_MODEL}`), exits 0
  — only downloads on first run; cached in the ollama-data volume after that
           ↓
Step 5:  gdelt-producer starts (waits for kafka healthy + kafka-init completed)
Step 6:  backend starts (waits for cassandra-init completed + kafka-init completed
         + ollama-pull completed)

Parallel: spark + spark-worker start independently
```

**Important:** Cassandra takes about 60 seconds to reach "healthy" state, and the first `ollama-pull` run downloads the model (~3.4 GB, one-time). The total stack startup is typically **3–5 minutes** on a warm cache, longer on the very first run.

---

## 8. Dataset Generation

The `dataset_generation/` folder contains offline tooling for creating training data for fine-tuning.

**`generate_gdelt_pairs.py`:**
- Uses the OpenAI API to generate synthetic (natural language prompt, PySpark code) pairs
- Each pair follows the same schema defined in `gdelt_environment.yaml`
- Output: `gdelt_pairs.jsonl` — one JSON object per line, format: `{"prompt": "...", "code": "..."}`
- This dataset can be used to fine-tune Qwen3.5-4B with LoRA/PEFT (not implemented in this repo, but the dataset is the input) — the resulting adapter would then be merged, converted to GGUF, and registered with Ollama (see the Fine-Tuning section in `README.md`)

**`validate_gdelt_pairs.py`:**
- Reads `gdelt_pairs.jsonl`
- Checks each code snippet passes the same Ruff lint stage used in the harness
- Outputs pass/fail statistics

This is completely offline — it does not touch the running stack.

---

## 9. End-to-End Test

**Location:** `dev/e2e_test/e2e_test.py`

**Purpose:** Validate the full data infrastructure (Kafka → Spark → Cassandra) without involving the backend or LLM. Run this to confirm the data layer is working correctly.

**Steps:**
1. **Produce:** Publishes 100 synthetic GDELT events to `gdelt-events-raw`
2. **Process:** Submits a hardcoded Spark job that reads from Kafka, computes average tone per country using 1-hour tumbling windows, writes to `aggregated_results`
3. **Verify:** Queries Cassandra and asserts that rows were written

**How to run:**
```bash
docker compose --profile test run --rm e2e-test
```

This service is only created when you use `--profile test`, so it doesn't affect normal operation.

---

## 10. Request Lifecycle (Full Walk-through)

Here is exactly what happens when a user calls `POST /generate`:

```
1. Client sends:
   POST http://localhost:8000/generate
   {"prompt": "aggregate average tone by country with 1-hour windows"}

2. FastAPI receives request, validates with Pydantic
   - Loads metadata.yaml (or uses environment_override if provided)
   - Calls engine.run(prompt, env_yaml)

3. GenerationEngine.run() — Attempt 1:

   a. PromptBuilder.build_messages() assembles:
      - System prompt (8 rules + worked examples)
      - User message: env YAML + user prompt

   b. to_langchain_messages() converts the {"role","content"} dicts to
      SystemMessage/HumanMessage/AIMessage objects
      → lc_messages (typically 2000-4000 tokens once templated by Ollama)

   c. ChatOllama.invoke(lc_messages) — Ollama applies Qwen3.5's chat
      template server-side and generates up to 1024 new tokens
      → response.content = "```python\nfrom pyspark.sql import..."

   d. CodeParser extracts the ```python block
      → code = "from pyspark.sql import SparkSession\n..."
      → yield {"event": "inference_complete", "attempt": 1, "code": "..."}

   e. Harness.validate(code, metadata):

      Stage 1 — Ruff:
        ruff check /tmp/abc123.py → return code 0 (pass)
        → yield {"event": "lint_passed", "attempt": 1}

      Stage 2 — spark-submit:
        spark-submit --master local[*] /tmp/abc123.py
        Phase 1 (15s): process still running → continue
        Phase 2 (15s): process still running → pass (streaming job running)
        → yield {"event": "docker_passed", "attempt": 1}

      Stage 3 — Cassandra schema:
        Extract ".table("aggregated_results")" from code
        Query system_schema.tables → table exists → pass
        → yield {"event": "cassandra_passed", "attempt": 1}

   f. All stages passed:
      → yield {"event": "done", "code": "...", "attempts": 1, "latency_ms": 23500}

4. FastAPI streams all yielded events as SSE:
   data: {"event": "inference_complete", "attempt": 1, ...}
   data: {"event": "lint_passed", ...}
   data: {"event": "docker_passed", ...}
   data: {"event": "cassandra_passed", ...}
   data: {"event": "done", "code": "from pyspark.sql import SparkSession\n..."}
```

**If Stage 2 fails (example — wrong Cassandra format string):**
```
   spark-submit runs, throws:
   "AnalysisException: Failed to find data source: cassandra"
   (stderr, first 50 lines)

   → yield {"event": "docker_failed", "attempt": 1, "error": "AnalysisException..."}

   Correction history gets appended:
   {"role": "assistant", "content": <broken code>}
   {"role": "user", "content": "CORRECTION: Stage 2 spark-submit failed:\nAnalysisException..."}

   → Loop continues with Attempt 2
   → Model sees the error and (hopefully) fixes: .format("org.apache.spark.sql.cassandra")
   → Validation passes on Attempt 2
   → yield {"event": "done", "attempts": 2, ...}
```

---

## 11. Configuration Reference

All configuration is via environment variables, loaded from `.env` by Docker Compose.

| Variable | Default | Service | Description |
|----------|---------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://ollama:11434` | backend | Ollama server address |
| `OLLAMA_MODEL` | `qwen3.5:4b` | backend, ollama-pull | Model tag to pull and serve |
| `MAX_NEW_TOKENS` | `1024` | backend | Token budget per LLM call |
| `OLLAMA_NUM_CTX` | `6144` | backend | Context window (tokens) the model is loaded with |
| `OLLAMA_FLASH_ATTENTION` | `1` | ollama | Enables flash attention (VRAM efficiency) |
| `OLLAMA_KV_CACHE_TYPE` | `q8_0` | ollama | KV cache quantization |
| `OLLAMA_KEEP_ALIVE` | `30m` | ollama | How long to keep the model warm in memory after last use |
| `MAX_ATTEMPTS` | `3` | backend | Self-correction retry limit |
| `MOCK_LLM` | `false` | backend | Skip model load (for testing) |
| `KAFKA_BROKER_ID` | `1` | kafka | Kafka broker ID |
| `KAFKA_HOST` | `kafka:9092` | various | Internal Kafka address |
| `KAFKA_EXTERNAL_PORT` | `29092` | kafka | Host-exposed Kafka port |
| `CASSANDRA_CLUSTER_NAME` | `nl2pipeline` | cassandra | Cluster display name |
| `CASSANDRA_PORT` | `9042` | cassandra | CQL port |
| `CASSANDRA_KEYSPACE` | `nl2pipeline` | backend | Keyspace used for the schema check + pipeline_runs log |
| `SPARK_MASTER_PORT` | `7077` | spark | Spark cluster port |
| `SPARK_MASTER_WEBUI_PORT` | `8081` | spark | Master web UI port |
| `SPARK_WORKER_WEBUI_PORT` | `8082` | spark-worker | Worker web UI port |
| `SPARK_WORKER_MEMORY` | `2g` | spark-worker | Memory per worker |
| `SPARK_WORKER_CORES` | `2` | spark-worker | CPU cores per worker |
| `PRODUCER_MODE` | `replay` | gdelt-producer | `replay` or `live` |
| `PUBLISH_RATE` | `10` | gdelt-producer | Events per second (replay mode) |
| `REPLAY_LOOP` | `true` | gdelt-producer | Loop replay forever |
| `LIVE_POLL_SECONDS` | `900` | gdelt-producer | Polling interval (live mode) |
| `BACKEND_PORT` | `8000` | backend | API server port |

---

## 12. Tech Stack Summary

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| API framework | FastAPI + Uvicorn | 0.111+ | REST API, SSE streaming |
| Language model | Qwen3.5-4B | 4B params | PySpark code generation |
| Model serving | Ollama (llama.cpp, GGUF) | latest | Separate container, HTTP API on 11434 |
| LLM client | langchain-ollama / langchain-core | 0.1+ / 0.2+ | `ChatOllama` client used by the backend |
| Fine-tuning | PEFT / LoRA, offline | 0.10+ | Merge → GGUF convert → `ollama create` to serve |
| Stream processing | Apache Spark | 3.5.1 | Generated pipeline execution + validation |
| Message broker | Apache Kafka | 7.5.0 (Confluent) | Real-time event streaming |
| Coordination | Apache Zookeeper | 7.5.0 | Kafka cluster coordination |
| Database | Apache Cassandra | 4.1 | Time-series result storage |
| Linting | Ruff | 0.4+ | Generated code validation (Stage 1) |
| Containerization | Docker + Compose | — | Service orchestration |
| Language | Python | 3.11 | All application code |
| JVM | OpenJDK | 17 | Spark, Kafka, Cassandra |
| GPU runtime | NVIDIA CUDA | 12.6 | Used by the `ollama` container for inference |

---

## 13. Deployment Requirements

**Hardware:**
- NVIDIA GPU with ≥ 4 GB VRAM (tested on RTX 3050) — used by the `ollama` container
- ≥ 16 GB RAM (Cassandra + Spark are memory-hungry)
- ≥ 15 GB disk (model weights ~3.4 GB via the Ollama volume, Docker images ~7 GB)

**Software:**
- Docker Desktop (or Docker Engine + Compose plugin)
- NVIDIA Container Toolkit (for GPU passthrough to the `ollama` container)

---

## 14. Quick Start

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env if needed — OLLAMA_MODEL defaults to qwen3.5:4b

# 2. Start the full stack (takes 3-5 minutes first time; the ollama-pull
#    service downloads the model, ~3.4 GB, on first run only)
docker compose up -d

# 3. Monitor startup progress
docker compose logs -f ollama-pull backend

# 4. Check the backend is ready
curl http://localhost:8000/health
# → {"status": "ok"}

# 5. Inspect the active schema
curl http://localhost:8000/environments

# 6. Generate a pipeline
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -N \
  -d '{"prompt": "Read GDELT events and write raw events to the processed_events table", "environment_override": null}'
# (use -N for SSE streaming output)

# 7. Run the integration test (optional)
docker compose --profile test run --rm e2e-test

# 8. Useful web UIs (once running)
# Spark Master UI:  http://localhost:8081
# Spark Worker UI:  http://localhost:8082

# 9. Stop everything
docker compose down

# 10. Stop and remove volumes (full reset, re-downloads the model next start)
docker compose down -v
```
