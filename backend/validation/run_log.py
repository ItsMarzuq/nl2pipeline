from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


def log_run(
    session: Any,
    *,
    run_id: uuid.UUID,
    nl_prompt: str,
    generated_code: str,
    status: str,
    error_msg: str | None,
    attempt_number: int,
    model_used: str,
    latency_ms: int,
) -> None:
    """
    Insert one row per completed generation run into pipeline_runs.

    Best-effort: a Cassandra hiccup must never break the /generate response,
    so failures here are logged and swallowed, not raised.
    """
    if session is None:
        return

    try:
        session.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, nl_prompt, generated_code, status, error_msg,
                 attempt_number, model_used, latency_ms, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                nl_prompt,
                generated_code,
                status,
                error_msg,
                attempt_number,
                model_used,
                latency_ms,
                datetime.now(timezone.utc),
            ),
        )
    except Exception as exc:
        log.warning("Failed to log run %s to pipeline_runs: %s", run_id, exc)
