from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, AsyncGenerator

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from .code_parser import ParseError, extract_python
from .prompt_builder import get_prompt_template
from ..validation.harness import run_harness, STAGE_EVENT_MAP
from ..validation.run_log import log_run

log = logging.getLogger(__name__)

_MOCK_CODE = '''\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MockPipeline").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092")
    .option("subscribe", "clicks_raw")
    .load()
)

df.writeStream.format("console").start().awaitTermination()
'''

_SESSION_WINDOW = 10  # max messages kept per session (5 prompt/code pairs)


class GenerationEngine:
    """
    Inference + self-correction loop, yielding structured SSE event dicts.

    Uses an LCEL chain  (ChatPromptTemplate | ChatOllama)  with two memory layers:

    correction_messages — list[BaseMessage], reset each run().
                          Tracks (code attempt, validation error) pairs so the LLM
                          can self-correct within a single /generate request.

    session_store       — dict[session_id, list[BaseMessage]], lives in app.state.
                          Keeps the last 5 (prompt, code) pairs so the model builds
                          context across a user session (windowed to avoid blowing
                          the context budget).
    """

    def __init__(
        self,
        llm: Any,
        env_yaml: str,
        max_attempts: int,
        session_store: dict[str, list[BaseMessage]],
        mock: bool = False,
        cassandra_session: Any = None,
    ) -> None:
        self._llm = llm
        self._env_yaml = env_yaml
        self._max_attempts = max_attempts
        self._session_store = session_store
        self._mock = mock
        self._cassandra_session = cassandra_session

        if not mock:
            self._chain = get_prompt_template() | llm

    async def run(
        self,
        prompt: str,
        environment_override: str | None = None,
        session_id: str | None = None,
    ) -> AsyncGenerator[dict, None]:
        env_yaml = environment_override or self._env_yaml
        start_ms = int(time.time() * 1000)

        yield {"event": "metadata_loaded", "data": {"env_loaded": True, "env_chars": len(env_yaml)}}

        if self._mock:
            latency_ms = int(time.time() * 1000) - start_ms
            yield {"event": "inference_complete", "data": {"attempt": 1, "code": _MOCK_CODE}}
            log_run(
                self._cassandra_session,
                run_id=uuid.uuid4(),
                nl_prompt=prompt,
                generated_code=_MOCK_CODE,
                status="pass",
                error_msg=None,
                attempt_number=1,
                model_used="mock",
                latency_ms=latency_ms,
            )
            yield {
                "event": "done",
                "data": {
                    "code": _MOCK_CODE,
                    "attempts": 1,
                    "latency_ms": latency_ms,
                    "attempt_log": [{"attempt": 1, "status": "pass"}],
                },
            }
            return

        # Per-request correction memory — reset every run()
        correction_messages: list[BaseMessage] = []

        # Cross-request session memory — persists in app.state
        session_messages: list[BaseMessage] = (
            self._session_store.get(session_id, []) if session_id else []
        )

        final_code = ""
        attempt_log: list[dict] = []

        for attempt in range(1, self._max_attempts + 1):
            inputs = {
                "env_yaml": env_yaml,
                "user_request": prompt,
                "session_history": session_messages,
                "correction_history": correction_messages,
            }

            llm_start_ms = int(time.time() * 1000)
            response = await asyncio.to_thread(self._chain.invoke, inputs)
            llm_latency_ms = int(time.time() * 1000) - llm_start_ms

            meta = response.response_metadata or {}
            eval_count = meta.get("eval_count")
            eval_duration_ns = meta.get("eval_duration")
            if eval_count and eval_duration_ns:
                tok_per_s = eval_count / (eval_duration_ns / 1e9)
                log.info(
                    "LLM invoke attempt %d took %dms (wall) — Ollama reports %d tokens "
                    "in %.2fs = %.1f tok/s (prompt_eval_count=%s, load_duration=%sns)",
                    attempt, llm_latency_ms, eval_count, eval_duration_ns / 1e9, tok_per_s,
                    meta.get("prompt_eval_count"), meta.get("load_duration"),
                )
            else:
                log.info("LLM invoke attempt %d took %dms", attempt, llm_latency_ms)

            raw_output: str = response.content

            try:
                code = extract_python(raw_output)
            except ParseError as exc:
                log.warning("ParseError on attempt %d: %s", attempt, exc)
                attempt_log.append({
                    "attempt": attempt,
                    "status": "fail",
                    "error": str(exc),
                    "llm_latency_ms": llm_latency_ms,
                })
                correction_messages.append(AIMessage(content=raw_output))
                correction_messages.append(HumanMessage(
                    content=(
                        "Your response did not contain a ```python code block.\n"
                        "Return only a ```python ... ``` fenced block with no other text."
                    )
                ))
                if attempt == self._max_attempts:
                    break
                continue

            yield {
                "event": "inference_complete",
                "data": {"attempt": attempt, "code": code, "llm_latency_ms": llm_latency_ms},
            }

            harness_start_ms = int(time.time() * 1000)
            stage_results = await asyncio.to_thread(run_harness, code, attempt, self._cassandra_session)
            harness_latency_ms = int(time.time() * 1000) - harness_start_ms
            log.info("Harness attempt %d took %dms", attempt, harness_latency_ms)

            harness_error: str | None = None
            for label, error in stage_results:
                pass_event, fail_event = STAGE_EVENT_MAP[label]
                if error:
                    yield {"event": fail_event, "data": {"attempt": attempt, "stage": label, "error": error}}
                    harness_error = error
                else:
                    yield {"event": pass_event, "data": {"attempt": attempt, "stage": label}}

            if harness_error is None:
                final_code = code
                attempt_log.append({
                    "attempt": attempt,
                    "status": "pass",
                    "llm_latency_ms": llm_latency_ms,
                    "harness_latency_ms": harness_latency_ms,
                })
                self._update_session(session_id, prompt, final_code)
                break

            log.warning("Harness error on attempt %d: %s", attempt, harness_error)
            attempt_log.append({
                "attempt": attempt,
                "status": "fail",
                "error": harness_error,
                "llm_latency_ms": llm_latency_ms,
                "harness_latency_ms": harness_latency_ms,
            })
            correction_messages.append(AIMessage(content=raw_output))
            correction_messages.append(HumanMessage(
                content=(
                    f"The generated code failed validation:\n\n{harness_error}\n\n"
                    "Please fix the issues and return only the corrected code "
                    "inside a ```python block."
                )
            ))

            if attempt == self._max_attempts:
                final_code = code

        total_latency_ms = int(time.time() * 1000) - start_ms
        last_attempt = attempt_log[-1] if attempt_log else None
        log_run(
            self._cassandra_session,
            run_id=uuid.uuid4(),
            nl_prompt=prompt,
            generated_code=final_code,
            status=last_attempt["status"] if last_attempt else "fail",
            error_msg=last_attempt.get("error") if last_attempt else None,
            attempt_number=len(attempt_log),
            model_used=getattr(self._llm, "model", "unknown"),
            latency_ms=total_latency_ms,
        )

        yield {
            "event": "done",
            "data": {
                "code": final_code,
                "attempts": len(attempt_log),
                "latency_ms": total_latency_ms,
                "attempt_log": attempt_log,
            },
        }

    def _update_session(self, session_id: str | None, prompt: str, code: str) -> None:
        """Persist the successful (prompt, code) pair into cross-request session memory."""
        if not session_id:
            return
        msgs = self._session_store.setdefault(session_id, [])
        msgs.append(HumanMessage(content=prompt))
        msgs.append(AIMessage(content=f"Generated pipeline:\n```python\n{code}\n```"))
        # Keep only the last _SESSION_WINDOW messages (sliding window)
        if len(msgs) > _SESSION_WINDOW:
            del msgs[:-_SESSION_WINDOW]
        log.info("Session %s updated — %d message(s) stored", session_id, len(msgs))
