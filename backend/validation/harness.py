from __future__ import annotations

import logging
import os
import re
import signal
import subprocess
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Maps stage label -> (pass_event, fail_event) consumed by engine.py for SSE
STAGE_EVENT_MAP: dict[str, tuple[str, str]] = {
    "V1 lint":      ("lint_passed",      "lint_failed"),
    "V2 structure": ("docker_passed",    "docker_failed"),
    "V3 cassandra": ("cassandra_passed", "cassandra_failed"),
}

_SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
])


def run_harness(code: str, attempt: int, cassandra_session: Any = None) -> list[tuple[str, str | None]]:
    """
    Three-stage validation harness.

    V1 — Ruff lint    : static analysis, catches syntax + style errors
    V2 — spark-submit : runs spark-submit as a local subprocess in this
                         container (local[*]), not against the dedicated
                         Spark cluster — a smoke test, not a cluster deploy
    V3 — Cassandra    : verifies referenced tables exist in the live cluster,
                         using the single session opened at app startup
                         (see backend/app.py) rather than a new connection
                         per attempt.

    Returns a list of (stage_label, error_or_None) for each stage executed.
    Stops at the first failure so remaining stages are not run.
    """
    log.info("Harness running (attempt %d)", attempt)

    results: list[tuple[str, str | None]] = []
    for label, stage_fn in [
        ("V1 lint",      lambda c: _stage1_lint(c)),
        ("V2 structure", lambda c: _stage2_spark_submit(c)),
        ("V3 cassandra", lambda c: _stage3_cassandra(c, cassandra_session)),
    ]:
        error = stage_fn(code)
        results.append((label, error))
        if error:
            log.warning("Harness %s failed: %s", label, str(error)[:120])
            return results

    log.info("All harness stages passed on attempt %d", attempt)
    return results


# ---------------------------------------------------------------------------
# Stage 1 — Ruff lint
# ---------------------------------------------------------------------------

def _stage1_lint(code: str) -> str | None:
    """Run Ruff on the generated code. Skips gracefully if Ruff is not on PATH."""
    with tempfile.NamedTemporaryFile(
        suffix=".py", mode="w", delete=False, encoding="utf-8"
    ) as fh:
        fh.write(code)
        tmp = fh.name

    try:
        result = subprocess.run(
            ["ruff", "check", "--output-format=concise", "--ignore=F401", tmp],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return result.stdout or result.stderr
        return None
    except FileNotFoundError:
        log.warning("Ruff not found on PATH — skipping lint stage.")
        return None
    except subprocess.TimeoutExpired:
        return "Ruff timed out after 30 seconds."
    finally:
        Path(tmp).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Stage 2 — spark-submit via local subprocess
# ---------------------------------------------------------------------------

def _extract_spark_errors(stdout: str, stderr: str) -> str:
    """Return error-relevant lines from spark-submit output."""
    # Python tracebacks go to stderr — show it first. WARN lines are excluded
    # deliberately: normal Spark/Kafka-connector warnings would otherwise be
    # fed back to the model as if they were failures, wasting a correction
    # attempt on noise.
    error_lines = [
        line for line in (stdout + "\n" + stderr).splitlines()
        if any(kw in line for kw in ("ERROR", "Exception", "Traceback", "Error:", "raise "))
    ]
    if error_lines:
        return "\n".join(error_lines[:50])
    # fallback: stderr in full (Python exception lives here)
    return stderr[:3000] or stdout[:3000]


_SPARK_STARTUP_TIMEOUT = 15   # seconds to wait for JVM + Kafka connect
_SPARK_STABILITY_TIMEOUT = 15  # extra seconds to catch post-connect crashes
_SPARK_JAR_CACHE = "/opt/spark-jars"


def _stage2_spark_submit(code: str) -> str | None:
    """
    Write the generated code to a temp file and run spark-submit as a local
    subprocess inside the backend container (local[*] mode) — a smoke test
    against this container's own Spark install, not the dedicated `spark`/
    `spark-worker` cluster in docker-compose.

    Two-phase timeout:
      Phase 1 (15s) — catches import/syntax/config errors before Kafka connect.
      Phase 2 (15s) — catches crashes after Kafka connects (e.g. schema mismatch).
      Still running after both phases → pass (streaming job on awaitTermination) —
      the process group is then terminated in `finally` since the harness only
      needs proof it didn't crash, not for the job to keep running.

    spark-submit's exec chain ends in a JVM which, for a PySpark job, forks a
    *separate* Python driver subprocess connected back via Py4J. Killing only
    the top-level PID (especially via SIGKILL, which gives the JVM no chance
    to run shutdown hooks) leaves that forked Python process orphaned. The
    job is launched in its own process group (`start_new_session=True`) so
    cleanup can signal the whole group at once via os.killpg.
    """
    import threading

    with tempfile.NamedTemporaryFile(
        suffix=".py", mode="w", delete=False, encoding="utf-8"
    ) as fh:
        fh.write(code)
        job_path = fh.name

    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--packages", _SPARK_PACKAGES,
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.sql.streaming.stopTimeout=5000",
        "--conf", f"spark.jars.ivy={_SPARK_JAR_CACHE}",
        job_path,
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    result_holder: dict = {}
    done_event = threading.Event()

    def _wait() -> None:
        result_holder["stdout"], result_holder["stderr"] = proc.communicate()
        done_event.set()

    try:
        thread = threading.Thread(target=_wait, daemon=True)
        thread.start()

        # Phase 1 — startup errors
        if done_event.wait(timeout=_SPARK_STARTUP_TIMEOUT):
            log.info("spark-submit exited with returncode=%d", proc.returncode)
            log.debug("spark stderr: %s", result_holder["stderr"][-1000:])
            if proc.returncode != 0:
                return f"spark-submit failed at startup:\n{_extract_spark_errors(result_holder['stdout'], result_holder['stderr'])}"
            return None

        log.info("spark-submit survived startup (%ds) — checking post-Kafka-connect stability", _SPARK_STARTUP_TIMEOUT)

        # Phase 2 — post-Kafka-connect crashes
        if done_event.wait(timeout=_SPARK_STABILITY_TIMEOUT):
            if proc.returncode != 0:
                return f"spark-submit crashed after Kafka connect:\n{_extract_spark_errors(result_holder['stdout'], result_holder['stderr'])}"
            return None

        log.info(
            "spark-submit stable after %ds — treating as pass, terminating process",
            _SPARK_STARTUP_TIMEOUT + _SPARK_STABILITY_TIMEOUT,
        )
        return None

    finally:
        if proc.poll() is None:
            _kill_process_group(proc)
        Path(job_path).unlink(missing_ok=True)


def _kill_process_group(proc: subprocess.Popen) -> None:
    """
    Terminate *proc* and every process it forked (the JVM's own PySpark
    driver child included) by signalling the whole process group rather
    than just the top-level PID — see _stage2_spark_submit's docstring.
    """
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return

    try:
        os.killpg(pgid, signal.SIGTERM)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            log.warning("spark-submit process group %d did not die after SIGKILL", pgid)
    except ProcessLookupError:
        pass


# ---------------------------------------------------------------------------
# Stage 3 — Cassandra schema check
# ---------------------------------------------------------------------------

def _stage3_cassandra(code: str, session: Any) -> str | None:
    """
    Extract Cassandra keyspace + table references from the generated code
    and verify they exist in the live cluster, using the shared session
    opened once at app startup (backend/app.py lifespan via
    backend/cassandra_client.py) instead of opening a new connection per
    validation attempt.

    Falls back to the session's bound keyspace when the generated code
    doesn't specify one explicitly via `.option("keyspace", ...)`.

    Returns None (pass) if:
      - No Cassandra table references found in the code
      - No session is available (cluster unreachable at startup, or
        cassandra-driver not installed) — non-fatal
    """
    keyspaces = re.findall(
        r'\.option\s*\(\s*["\']keyspace["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code
    )
    tables = re.findall(
        r'\.option\s*\(\s*["\']table["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code
    )

    if not tables:
        return None

    if session is None:
        log.warning("Cassandra session unavailable — skipping schema check.")
        return None

    try:
        existing = {
            (row.keyspace_name, row.table_name)
            for row in session.execute(
                "SELECT keyspace_name, table_name FROM system_schema.tables"
            )
        }

        keyspace = keyspaces[0] if keyspaces else (session.keyspace or "nl2pipeline")

        missing = [
            f"{keyspace}.{tbl}"
            for tbl in tables
            if (keyspace, tbl) not in existing
        ]

        if missing:
            return (
                f"Cassandra table(s) not found in cluster: {', '.join(missing)}.\n"
                "Use only table names defined in the environment YAML."
            )
        return None

    except Exception as exc:
        log.warning("Cassandra schema check skipped (query failed): %s", exc)
        return None
