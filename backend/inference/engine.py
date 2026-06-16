from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncGenerator

from .code_parser import ParseError, extract_python
from .prompt_builder import apply_template, build_messages
from ..validation.harness import run_harness, STAGE_EVENT_MAP

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


class GenerationEngine:
    """
    Owns the inference + self-correction loop and yields structured event dicts.

    The caller (app.py) serialises these dicts to SSE frames — keeping SSE
    formatting out of the engine makes it straightforward to unit test.

    Event dict shape:
        {"event": "<name>", "data": { ... }}
    """

    def __init__(
        self,
        tokenizer: Any,
        llm: Any,
        env_yaml: str,
        max_attempts: int,
        mock: bool = False,
    ) -> None:
        self._tokenizer = tokenizer
        self._llm = llm
        self._env_yaml = env_yaml
        self._max_attempts = max_attempts
        self._mock = mock

    async def run(
        self,
        prompt: str,
        environment_override: str | None = None,
    ) -> AsyncGenerator[dict, None]:
        env_yaml = environment_override or self._env_yaml
        start_ms = int(time.time() * 1000)

        yield {"event": "metadata_loaded", "data": {"env_loaded": True, "env_chars": len(env_yaml)}}

        if self._mock:
            yield {"event": "inference_complete", "data": {"attempt": 1, "code": _MOCK_CODE}}
            yield {
                "event": "done",
                "data": {
                    "code": _MOCK_CODE,
                    "attempts": 1,
                    "latency_ms": int(time.time() * 1000) - start_ms,
                    "attempt_log": [{"attempt": 1, "status": "pass"}],
                },
            }
            return

        correction_history: list[dict] = []
        final_code = ""
        attempt_log: list[dict] = []

        for attempt in range(1, self._max_attempts + 1):
            messages = build_messages(env_yaml, prompt, correction_history)
            formatted_prompt = apply_template(self._tokenizer, messages)

            raw_output: str = await asyncio.to_thread(self._llm.invoke, formatted_prompt)

            try:
                code = extract_python(raw_output)
            except ParseError as exc:
                log.warning("ParseError on attempt %d: %s", attempt, exc)
                attempt_log.append({"attempt": attempt, "status": "fail", "error": str(exc)})
                correction_history.extend([
                    {"role": "assistant", "content": raw_output},
                    {
                        "role": "user",
                        "content": (
                            "Your response did not contain a ```python code block.\n"
                            "Return only a ```python ... ``` fenced block with no other text."
                        ),
                    },
                ])
                if attempt == self._max_attempts:
                    yield {
                        "event": "done",
                        "data": {
                            "code": "",
                            "attempts": len(attempt_log),
                            "latency_ms": int(time.time() * 1000) - start_ms,
                            "attempt_log": attempt_log,
                        },
                    }
                continue

            yield {"event": "inference_complete", "data": {"attempt": attempt, "code": code}}

            stage_results = await asyncio.to_thread(run_harness, code, attempt)

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
                attempt_log.append({"attempt": attempt, "status": "pass"})
                break

            log.warning("Harness error on attempt %d: %s", attempt, harness_error)
            attempt_log.append({"attempt": attempt, "status": "fail", "error": harness_error})
            correction_history.extend([
                {"role": "assistant", "content": raw_output},
                {
                    "role": "user",
                    "content": (
                        f"The generated code failed validation:\n\n{harness_error}\n\n"
                        "Please fix the issues and return only the corrected code "
                        "inside a ```python block."
                    ),
                },
            ])

            if attempt == self._max_attempts:
                final_code = code

        yield {
            "event": "done",
            "data": {
                "code": final_code,
                "attempts": len(attempt_log),
                "latency_ms": int(time.time() * 1000) - start_ms,
                "attempt_log": attempt_log,
            },
        }
