# NL2Pipeline

**Natural Language to PySpark Pipeline Generator**

Describe a data processing task in plain English. NL2Pipeline uses a locally-hosted Small Language Model (Qwen3.5-4B, served via [Ollama](https://ollama.com)) to generate executable PySpark code that reads from Kafka, processes with Spark Structured Streaming, and writes results to Cassandra. Every generated pipeline passes through a 3-stage validation harness with automatic self-correction before being returned.

---

## Architecture

```
User (browser / API client)
        │
        │ POST /generate  (SSE stream)
        ▼
┌──────────────────────────────────┐      ┌───────────────────────┐
│  Backend  (FastAPI, port 8000)   │◀────▶│  Ollama (port 11434)  │
│  ┌────────────────────────────┐  │      │  Qwen3.5-4B (GGUF)    │
│  │  GenerationEngine           │  │      └───────────────────────┘
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
| LLM serving | Ollama (GGUF, llama.cpp backend) | latest |
| LLM | Qwen3.5-4B | 4B params |
| Fine-tuning | PEFT / LoRA, offline, merged + converted to GGUF for serving | 0.10+ |
| Stream processing | Apache Spark Structured Streaming | 3.5.1 |
| Event streaming | Apache Kafka + Zookeeper | 7.5.0 |
| Storage | Apache Cassandra | 4.1 |
| Lint validation | Ruff | 0.4+ |
| GPU | NVIDIA CUDA (used by the `ollama` service) | 12.6 |
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
│   ├── cassandra_client.py       # Shared Cassandra session (non-fatal if unreachable)
│   ├── inference/
│   │   ├── engine.py             # GenerationEngine — self-correction loop
│   │   ├── model_loader.py       # ChatOllama client (Ollama serves the model)
│   │   ├── prompt_builder.py     # Chat message construction
│   │   └── code_parser.py        # Extract ```python fences from model output
│   ├── validation/
│   │   ├── harness.py            # 3-stage validation harness
│   │   └── run_log.py            # Logs each generation run to the pipeline_runs table
│   └── tests/                    # Unit tests (code_parser, engine, harness)
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
├── docker-compose.yaml
├── .env                          # Runtime config (copy from .env.example)
└── .env.example
```

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- NVIDIA GPU with ≥ 4 GB VRAM (tested on RTX 3050 4 GB) and NVIDIA Container Toolkit

### 1 — Configure environment

```bash
cp .env.example .env
# Edit .env if you need to change ports or the OLLAMA_MODEL tag
```

### 2 — Start the stack

```bash
docker compose up -d
```

Services start in dependency order. Allow ~2-3 minutes for Cassandra and Spark to become healthy. On first start, the `ollama-pull` service downloads the `OLLAMA_MODEL` tag (Qwen3.5-4B, ~3.4 GB) into a Docker volume before the backend starts — this only happens once, subsequent restarts reuse the cached model.

> **Note:** `gdelt-producer` defaults to replay mode, which needs data downloaded first via `docker compose --profile init run --rm gdelt-downloader` (see [GDELT Data Ingestion](#gdelt-data-ingestion)) — otherwise it'll fail on startup with "No GDELT TSV files found."

### 3 — Verify health

```bash
docker compose ps
curl http://localhost:8000/health
# {"status": "ok"}
```

### 4 — Run end-to-end test (optional)

```bash
docker compose --profile test run --rm e2e-test
```

Publishes 100 synthetic GDELT events, runs a Spark aggregation, verifies results land in Cassandra.

---

## Environment Variables

Key variables from `.env.example`:

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Base URL of the Ollama server |
| `OLLAMA_MODEL` | `qwen3.5:4b` | Ollama model tag to run (swap to a fine-tuned tag once available) |
| `MAX_NEW_TOKENS` | `1024` | Token budget per generation attempt |
| `OLLAMA_NUM_CTX` | `6144` | Context window size (tokens) the model is loaded with |
| `MAX_ATTEMPTS` | `3` | Max self-correction retries |
| `MOCK_LLM` | `false` | Skip model load (returns canned code, useful for CI) |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `CASSANDRA_HOST` | `cassandra` | Cassandra host |
| `CASSANDRA_PORT` | `9042` | Cassandra CQL port |
| `CASSANDRA_KEYSPACE` | `nl2pipeline` | Cassandra keyspace used for the schema check + pipeline_runs log |
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
  "attempt_log": [{"attempt": 1, "status": "pass", "llm_latency_ms": 6100, "harness_latency_ms": 2050}]
}
```

### `GET /environments`

Returns the active `metadata.yaml` as JSON (topics, tables, schema, generation rules).

### `GET /health`

Returns `{"status": "ok"}` when the backend is ready.

---

## How Code Generation Works

1. **Prompt construction** — `prompt_builder.py` combines a system prompt, the full `metadata.yaml` environment (topics, schema, allowed operations, rules), and the user's natural language request into a chat message list.
2. **Inference** — the messages are sent to Qwen3.5-4B via the Ollama HTTP API (`ChatOllama`); `code_parser.py` extracts the ` ```python ` fence from the response.
3. **3-stage validation harness:**
   - **Stage 1 — Ruff lint:** syntax and style check (30 s timeout)
   - **Stage 2 — spark-submit:** launches the code in `local[*]` mode; a two-phase timeout (15 s startup + 15 s stability) distinguishes import errors from valid streaming jobs blocked on `awaitTermination()`
   - **Stage 3 — Cassandra schema:** verifies every table referenced in the code actually exists in the `nl2pipeline` keyspace
4. **Self-correction loop** — on failure the engine appends a correction message (with the exact error) and retries, up to `MAX_ATTEMPTS` (default 3). Failure modes map to targeted corrections:
   - Parse error → "wrap output in a code fence"
   - Lint error → exact Ruff output
   - Spark error → exact stderr
   - Schema error → list of valid tables
5. **Response** — the final code (best attempt) is streamed back with the full attempt log.

---

## Known Limitations

- **Unsandboxed code execution.** Stage 2 of the validation harness runs
  whatever PySpark code the model produces as a subprocess inside the
  `backend` container itself (`spark-submit --master local[*]`), with no
  sandboxing beyond the container boundary (no seccomp profile, network
  policy, or resource cap specific to the generated job). Acceptable for a
  local research prototype behind no auth; would need real sandboxing
  (e.g. a disposable per-attempt container, gVisor, or a dedicated execution
  cluster with restricted credentials) before being exposed beyond localhost.

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `gdelt-events-raw` | 3 | 7 days | GDELT global event stream (source) |
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

- **Replay mode** (`PRODUCER_MODE=replay`, default) — reads pre-downloaded GDELT archive files from the `gdelt-data` volume and publishes them at a configurable rate (default 10 events/s), then loops. This mode needs data downloaded first — `gdelt-downloader` is a profile-gated service, so it does **not** run automatically with `docker compose up -d`:
  ```bash
  docker compose --profile init run --rm gdelt-downloader
  ```
  Run this once before starting the stack (or before starting `gdelt-producer`) — otherwise the producer will fail with "No GDELT TSV files found." Re-run it if you change `START_DATE`/`END_DATE` in `gdelt_kafka_ingestion/.env`.
- **Live mode** (`PRODUCER_MODE=live`) — polls the GDELT 15-minute update feed and downloads/publishes new records itself in near-real time; doesn't need the downloader at all.

The mapper converts the raw 65-column GDELT TSV format into a 14-field JSON schema: `event_id`, `event_date`, `actor1`, `actor2`, `event_code`, `country`, `lat`, `lon`, `tone`, `num_mentions`, `num_sources`, `num_articles`, `is_root_event`, `ts`.

---

## Fine-Tuning (Optional)

Training data is generated by `dataset_generation/generate_gdelt_pairs.py`, which produces NL-prompt + PySpark-code pairs using the `gdelt_environment.yaml` template. The output `gdelt_pairs.jsonl` can be used for supervised fine-tuning with LoRA/PEFT (`transformers` + `peft`, offline — not part of this repo's serving path).

Ollama serves GGUF, not raw HF/PEFT checkpoints, so a fine-tuned adapter has to be merged and converted before it can run here:

1. `PeftModel.merge_and_unload()` the trained LoRA adapter into the base model weights.
2. Convert the merged model to GGUF with llama.cpp's `convert_hf_to_gguf.py`, then quantize (e.g. `Q4_K_M`).
3. Register it with Ollama: `ollama create nl2pipeline-finetuned -f Modelfile` (Modelfile points `FROM` at the GGUF file).
4. Set `OLLAMA_MODEL=nl2pipeline-finetuned` in `.env` and restart the stack.

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
