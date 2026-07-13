from __future__ import annotations

import logging

from langchain_ollama import ChatOllama

log = logging.getLogger(__name__)


def load_llm(base_url: str, model: str, max_new_tokens: int = 1024, num_ctx: int = 6144) -> ChatOllama:
    """
    Build a ChatOllama client pointed at an already-running Ollama server.

    Ollama owns model loading, caching, and quantization on its side (GGUF
    via llama.cpp) — this just wires up the HTTP client. The server must
    already have `model` pulled (see the `ollama-pull` init step in
    docker-compose.yaml).
    """
    log.info("Connecting to Ollama at %s (model=%s)", base_url, model)
    llm = ChatOllama(
        base_url=base_url,
        model=model,
        num_predict=max_new_tokens,
        num_ctx=num_ctx,
        temperature=0,
        # qwen3.5 is a hybrid-reasoning model that otherwise spends its token
        # budget on hidden chain-of-thought before emitting the code block —
        # tested with reasoning enabled and it still hallucinated PySpark API
        # names, just slower (see engine debug session), so keep it off.
        reasoning=False,
    )
    log.info(
        "Ollama client ready — model=%s  max_new_tokens=%d  num_ctx=%d",
        model,
        max_new_tokens,
        num_ctx,
    )
    return llm
