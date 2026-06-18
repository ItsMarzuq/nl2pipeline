from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .config import Settings
from .inference.model_loader import load_llm
from .inference.engine import GenerationEngine
from .inference.prompt_builder import load_env_yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

settings = Settings()


# ---------------------------------------------------------------------------
# Lifespan — load model and env once, attach to app.state
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Loading environment.yaml from %s", settings.ENV_YAML_PATH)
    env_yaml = load_env_yaml(Path(settings.ENV_YAML_PATH))
    log.info("environment.yaml loaded (%d chars)", len(env_yaml))

    if settings.MOCK_LLM:
        log.warning("MOCK_LLM=true — skipping model load, returning canned responses")
        tokenizer, llm = None, None
    else:
        log.info("Loading model: %s", settings.BASE_MODEL_ID)
        tokenizer, llm = await asyncio.to_thread(
            load_llm,
            settings.BASE_MODEL_ID,
            settings.ADAPTER_PATH,
            settings.MAX_NEW_TOKENS,
        )
        log.info("Model ready")

    app.state.engine = GenerationEngine(
        tokenizer=tokenizer,
        llm=llm,
        env_yaml=env_yaml,
        max_attempts=settings.MAX_ATTEMPTS,
        mock=settings.MOCK_LLM,
    )

    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="NL2Pipeline API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class GenerateRequest(BaseModel):
    prompt: str
    environment_override: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@app.post("/generate")
async def generate(req: GenerateRequest, request: Request):
    engine: GenerationEngine = request.app.state.engine

    async def stream():
        async for item in engine.run(req.prompt, req.environment_override):
            yield _sse(item["event"], item["data"])

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/environments")
async def get_environments():
    """Return the active environment.yaml as parsed JSON for the React UI."""
    with open(settings.ENV_YAML_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@app.get("/health")
async def health():
    return {"status": "ok"}
