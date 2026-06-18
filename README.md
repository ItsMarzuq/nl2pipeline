# NL2Pipeline

**Natural Language to PySpark Pipeline Generator**

Describe a data processing task in plain English. NL2Pipeline uses a locally-hosted Small Language Model (Microsoft Phi-4-mini-instruct, 3.8B params) to generate executable PySpark code that reads from Kafka, processes with Spark Structured Streaming, and writes results to Cassandra. Every generated pipeline passes through a 3-stage validation harness with automatic self-correction before being returned.

---

## Architecture

```
User (browser / API client)
        │
        │ POST /generate  (SSE stream)
        ▼
┌──────────────────────────────────┐
│  Backend  (FastAPI, port 8000)   │
│  ┌────────────────────────────┐  │
│  │  Phi-4-mini-instruct (4-bit NF4)  │  │
│  │  Self-Correction Loop (≤3x)│  │
│  └────────────────────────────┘  │
│  ┌────────────────────────────┐  │
│  │  Validation Harness        │  │
│  │  Stage 1: Ruff lint        │  │
│  │  Stage 2: spark-submit     │  │
│  │  Stage 3: Cassandra schema │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
        │
        ├──────────────────┬──────────────────┐
        ▼                  ▼                  ▼
  ┌──────────┐      ┌────────────┐    ┌──────────────┐
  │  Kafka   │      │   Spark    │    │  Cassandra   │
  │ Broker   │      │  Cluster   │    │  (port 9042) │
  │(port9092)│      │(port 7077) │    └──────────────┘
  └──────────┘      └────────────┘
        ▲
  ┌──────────────┐
  │GDELT Producer│  (continuously publishes global news events)
  └──────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| API | FastAPI + Uvicorn | 0.111+ |
| LLM | Microsoft Phi-4-mini-instruct + bitsandbytes (4-bit) | 3.8B params |
| Fine-tuning | PEFT / LoRA (optional adapter) | 0.10+ |
| Stream processing | Apache Spark Structured Streaming | 3.5.1 |
| Event streaming | Apache Kafka + Zookeeper | 7.5.0 |
| Storage | Apache Cassandra | 4.1 |
| Lint validation | Ruff | 0.4+ |
| GPU | NVIDIA CUDA | 12.6 |
| Containerisation | Docker Compose | — |
| Language | Python 3.11, Java 17 | — |

---

## Repository Layout

```
nl2pipeline/
├── backend/                      # Inference server
│   ├── app.py                    # FastAPI app, SSE endpoint
│   ├── config.py                 # Environment-variable settings
│   ├── metadata.yaml             # Single source of truth: Kafka topics, Cassandra tables, schema, generation rules
│   ├── inference/
│   │   ├── engine.py             # GenerationEngine — self-correction loop
│   │   ├── model_loader.py       # Load Phi-4 + optional LoRA adapter
│   │   ├── prompt_builder.py     # Chat-template prompt construction
│   │   └── code_parser.py        # Extract ```python fences from model output
│   └── validation/
│       └── harness.py            # 3-stage validation harness
│
├── gdelt_kafka_ingestion/        # GDELT event producer
│   └── app/
│       ├── producer.py           # Kafka producer (replay or live 15-min poll)
│       ├── downloader.py         # Download from GDELT HTTP archive
│       └── gdelt_mapper.py       # Map 65-column TSV → 14-field JSON
│
├── startup_scripts/              # One-shot init containers
│   ├── init_kafka_topics.py      # Create Kafka topics
│   └── init_cassandra.py         # Create keyspace + tables
│
├── dev/
│   ├── e2e_test/e2e_test.py      # Full integration test
│   └── test_client.html          # Browser UI for /generate endpoint
│
├── dataset_generation/           # Training data for fine-tuning
│   ├── generate_gdelt_pairs.py
│   ├── validate_gdelt_pairs.py
│   └── gdelt_pairs.jsonl         # Generated NL → code dataset
│
├── models/                       # Model weights (bind-mounted, gitignored)
│   └── phi4-mini/                # ~8 GB Phi-4-mini-instruct
│
├── docker-compose.yaml
├── .env                          # Runtime config (copy from .env.example)
└── .env.example
```

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- NVIDIA GPU with ≥ 4 GB VRAM (tested on RTX 3050 4 GB) and NVIDIA Container Toolkit
- HuggingFace account + access token

### 1 — Download model weights

```bash
pip install huggingface_hub
hf auth login          # paste your HF token
hf download microsoft/Phi-4-mini-instruct --local-dir ./models/phi4-mini
```

This downloads ~8 GB into `models/phi4-mini/`.

### 2 — Configure environment

```bash
cp .env.example .env
# Edit .env if you need to change ports or model path
```

### 3 — Start the stack

```bash
docker compose up -d
```

Services start in dependency order. Allow ~2-3 minutes for Cassandra and Spark to become healthy. The backend takes an additional ~45 seconds to load all 194 model shards.

### 4 — Verify health

```bash
docker compose ps
curl http://localhost:8000/health
# {"status": "ok"}
```

### 5 — Run end-to-end test (optional)

```bash
docker compose --profile test run --rm e2e-test
```

Publishes 100 synthetic GDELT events, runs a Spark aggregation, verifies results land in Cassandra.

---

## Environment Variables

Key variables from `.env.example`:

| Variable | Default | Description |
|---|---|---|
| `BASE_MODEL_ID` | `/model` | Path inside the container to model weights |
| `ADAPTER_PATH` | _(empty)_ | Optional LoRA fine-tune adapter path |
| `MAX_NEW_TOKENS` | `1024` | Token budget per generation attempt |
| `MAX_ATTEMPTS` | `3` | Max self-correction retries |
| `MOCK_LLM` | `false` | Skip model load (returns canned code, useful for CI) |
| `HF_TOKEN` | _(empty)_ | HuggingFace token for gated models |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `CASSANDRA_HOST` | `cassandra` | Cassandra host |
| `SPARK_MASTER_PORT` | `7077` | Spark master port |
| `BACKEND_PORT` | `8000` | FastAPI listen port |

---

## API Reference

### `POST /generate`

Generates a PySpark pipeline from a natural language prompt. Returns a **Server-Sent Events (SSE)** stream so the client receives live progress.

**Request body:**
```json
{
  "prompt": "Read GDELT events and aggregate average tone by country over 1-hour windows",
  "environment_override": null
}
```

`environment_override` accepts a YAML string to substitute the default `metadata.yaml` for per-request environments.

**SSE event sequence:**

| Event | Meaning |
|---|---|
| `metadata_loaded` | Environment YAML parsed |
| `inference_complete` | Model returned code (attempt N) |
| `lint_passed` / `lint_failed` | Stage 1 result |
| `docker_passed` / `docker_failed` | Stage 2 result (spark-submit) |
| `cassandra_passed` / `cassandra_failed` | Stage 3 result |
| `done` | Final code + attempt log + latency |

**Example `done` payload:**
```json
{
  "code": "from pyspark.sql import SparkSession\n...",
  "attempts": 1,
  "latency_ms": 8240,
  "attempt_log": [{"attempt": 1, "stages_passed": ["V1 lint", "V2 spark", "V3 cassandra"]}]
}
```

### `GET /environments`

Returns the active `metadata.yaml` as JSON (topics, tables, schema, generation rules).

### `GET /health`

Returns `{"status": "ok"}` when the backend is ready.

---

## How Code Generation Works

1. **Prompt construction** — `prompt_builder.py` combines a system prompt, the full `metadata.yaml` environment (topics, schema, allowed operations, rules), and the user's natural language request into a chat-template message list.
2. **Inference** — Phi-4-mini-instruct generates output; `code_parser.py` extracts the ` ```python ` fence.
3. **3-stage validation harness:**
   - **Stage 1 — Ruff lint:** syntax and style check (30 s timeout)
   - **Stage 2 — spark-submit:** launches the code in `local[*]` mode; a two-phase timeout (30 s startup + 30 s stability) distinguishes import errors from valid streaming jobs blocked on `awaitTermination()`
   - **Stage 3 — Cassandra schema:** verifies every table referenced in the code actually exists in the `nl2pipeline` keyspace
4. **Self-correction loop** — on failure the engine appends a correction message (with the exact error) and retries, up to `MAX_ATTEMPTS` (default 3). Failure modes map to targeted corrections:
   - Parse error → "wrap output in a code fence"
   - Lint error → exact Ruff output
   - Spark error → exact stderr
   - Schema error → list of valid tables
5. **Response** — the final code (best attempt) is streamed back with the full attempt log.

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `gdelt-events-raw` | 6 | 7 days | GDELT global event stream (source) |
| `pipeline-errors-dlq` | 1 | 30 days | Dead-letter queue for failed messages |

---

## Cassandra Schema

Keyspace: `nl2pipeline`

| Table | Partition Key | Clustering Key | Purpose |
|---|---|---|---|
| `processed_events` | `(source_topic, event_date)` | `event_id` | Generic raw event sink |
| `aggregated_results` | `(source_topic, window_start)` | `group_key, metric_name` | Spark aggregation output |
| `pipeline_runs` | `run_id` | — | Generation log for evaluation / EX scoring |

---

## GDELT Data Ingestion

The `gdelt_kafka_ingestion` service ingests GDELT (Global Database of Events, Language, and Tone) news event data.

**Two modes** (configured via `.env` in `gdelt_kafka_ingestion/`):

- **Replay mode** — downloads historical GDELT archives and publishes at a configurable rate (default 10 events/s), then loops
- **Live mode** — polls the GDELT 15-minute update feed and publishes new records in near-real time

The mapper converts the raw 65-column GDELT TSV format into a 14-field JSON schema: `event_id`, `event_date`, `actor1`, `actor2`, `event_code`, `country`, `lat`, `lon`, `tone`, `num_mentions`, `num_sources`, `num_articles`, `is_root_event`, `ts`.

---

## Fine-Tuning (Optional)

Training data is generated by `dataset_generation/generate_gdelt_pairs.py`, which produces NL-prompt + PySpark-code pairs using the `gdelt_environment.yaml` template. The output `gdelt_pairs.jsonl` can be used for supervised fine-tuning with LoRA/PEFT. To use an adapter, set `ADAPTER_PATH` in `.env` before starting the stack.

---

## Useful Commands

```bash
# Start the full stack
docker compose up -d

# Follow backend logs
docker compose logs backend -f

# Open Cassandra shell
docker compose exec cassandra cqlsh

# Spark UI
open http://localhost:8081

# Browser test client
open dev/test_client.html

# Run e2e test
docker compose --profile test run --rm e2e-test

# Stop everything
docker compose down
```
