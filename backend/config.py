from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Base URL of the Ollama server (the `ollama` service in docker-compose).
    OLLAMA_BASE_URL: str = "http://ollama:11434"

    # Ollama model tag to run, e.g. "qwen3.5:4b" or a custom tag created from
    # a fine-tuned checkpoint via `ollama create`.
    OLLAMA_MODEL: str = "qwen3.5:4b"

    # Path to the environment metadata file, relative to the project root.
    ENV_YAML_PATH: str = "backend/metadata.yaml"

    # Token generation budget per inference call. Generated PySpark scripts
    # typically run 300-600 tokens (up to ~1050 with more complex aggregation
    # code); kept modest to cap worst-case decode time while leaving headroom.
    MAX_NEW_TOKENS: int = 1024

    # Context window (in tokens) to load the model with. The self-correction
    # loop appends prior attempts + errors to the conversation, so this needs
    # headroom beyond a single prompt + response. Qwen 3.5 4B supports up to
    # 262144 natively; Ollama defaults to 4096 if left unset. Kept modest here
    # since a larger KV cache competes with model weights for limited VRAM.
    OLLAMA_NUM_CTX: int = 6144

    # Maximum self-correction retries before returning best-effort code.
    MAX_ATTEMPTS: int = 3

    # Skip model loading and return a canned PySpark script.
    # Use during development / CI when GPU / model weights are unavailable.
    MOCK_LLM: bool = False

    # Cassandra connection, used for both the Stage 3 harness schema check
    # and the pipeline_runs observability log. A single session is opened
    # once at startup and shared by both (see backend/cassandra_client.py).
    CASSANDRA_HOST: str = "cassandra"
    CASSANDRA_PORT: int = 9042
    CASSANDRA_USERNAME: str = ""
    CASSANDRA_PASSWORD: str = ""
    CASSANDRA_KEYSPACE: str = "nl2pipeline"

    # extra="ignore": docker-compose's shared root .env carries infra vars
    # (KAFKA_*, CASSANDRA_*, SPARK_*, ...) that aren't part of this model —
    # pydantic-settings defaults to extra="forbid", which would otherwise
    # make Settings() raise whenever a developer runs the backend outside
    # Docker with that .env present in the cwd.
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}
