from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # HuggingFace model hub ID or local path (e.g. /model for bind-mount).
    BASE_MODEL_ID: str = "microsoft/Phi-4-mini-instruct"

    # Path to the LoRA adapter directory produced by fine-tuning.
    # Set to None to run with the base model only.
    ADAPTER_PATH: str | None = None

    # Path to the environment metadata file, relative to the project root.
    ENV_YAML_PATH: str = "backend/metadata.yaml"

    # Token generation budget per inference call.
    MAX_NEW_TOKENS: int = 1024

    # Maximum self-correction retries before returning best-effort code.
    MAX_ATTEMPTS: int = 3

    # Skip model loading and return a canned PySpark script.
    # Use during development / CI when GPU / model weights are unavailable.
    MOCK_LLM: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
