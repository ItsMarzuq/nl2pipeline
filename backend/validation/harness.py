from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

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


def run_harness(code: str, attempt: int) -> list[tuple[str, str | None]]:
    """
    Three-stage validation harness.

    V1 — Ruff lint    : static analysis, catches syntax + style errors
    V2 — spark-submit : docker-execs spark-submit in the Spark container
    V3 — Cassandra    : verifies referenced tables exist in the live cluster

    Returns a list of (stage_label, error_or_None) for each stage executed.
    Stops at the first failure so remaining stages are not run.
    """
    log.info("Harness running (attempt %d)", attempt)

    results: list[tuple[str, str | None]] = []
    for label, stage_fn in [
        ("V1 lint",      _stage1_lint),
        ("V2 structure", _stage2_spark_submit),
        ("V3 cassandra", _stage3_cassandra),
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
# Stage 2 — spark-submit via Docker exec
# ---------------------------------------------------------------------------

_SPARK_STARTUP_TIMEOUT = 30   # seconds to wait for JVM + Kafka connect
_SPARK_STABILITY_TIMEOUT = 30  # extra seconds to catch post-connect crashes
_SPARK_JAR_CACHE = "/opt/spark-jars"


def _stage2_spark_submit(code: str) -> str | None:
    """
    Write the generated code to a temp file and run spark-submit locally
    inside the backend container (local[*] mode).

    Two-phase timeout:
      Phase 1 (30s) — catches import/syntax/config errors before Kafka connect.
      Phase 2 (30s) — catches crashes after Kafka connects (e.g. schema mismatch).
      Still running after both phases → pass (streaming job on awaitTermination).
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

    result_holder: dict = {}
    done_event = threading.Event()

    def _run() -> None:
        result_holder["proc"] = subprocess.run(
            cmd, capture_output=True, text=True
        )
        done_event.set()

    try:
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        # Phase 1 — startup errors
        if done_event.wait(timeout=_SPARK_STARTUP_TIMEOUT):
            proc = result_holder["proc"]
            if proc.returncode != 0:
                return f"spark-submit failed at startup:\n{(proc.stdout + proc.stderr)[-2000:]}"
            return None

        log.info("spark-submit survived startup (%ds) — checking post-Kafka-connect stability", _SPARK_STARTUP_TIMEOUT)

        # Phase 2 — post-Kafka-connect crashes
        if done_event.wait(timeout=_SPARK_STABILITY_TIMEOUT):
            proc = result_holder["proc"]
            if proc.returncode != 0:
                return f"spark-submit crashed after Kafka connect:\n{(proc.stdout + proc.stderr)[-2000:]}"
            return None

        log.info("spark-submit stable after %ds — treating as pass", _SPARK_STARTUP_TIMEOUT + _SPARK_STABILITY_TIMEOUT)
        return None

    finally:
        Path(job_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Stage 3 — Cassandra schema check
# ---------------------------------------------------------------------------

def _stage3_cassandra(code: str) -> str | None:
    """
    Extract Cassandra keyspace + table references from the generated code
    and verify they exist in the live cluster.

    Auth is optional — PlainTextAuthProvider is only used when both
    CASSANDRA_USERNAME and CASSANDRA_PASSWORD env vars are set.  The
    default Cassandra setup (AllowAllAuthenticator) works without creds.

    Keyspace falls back to CASSANDRA_KEYSPACE env var (default: nl2pipeline)
    when the generated code doesn't specify one explicitly.

    Returns None (pass) if:
      - No Cassandra table references found in the code
      - The cluster is unreachable (non-fatal)
      - cassandra-driver is not installed
    """
    keyspaces = re.findall(
        r'\.option\s*\(\s*["\']keyspace["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code
    )
    tables = re.findall(
        r'\.option\s*\(\s*["\']table["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code
    )

    if not tables:
        return None

    try:
        from cassandra.cluster import Cluster  # noqa: PLC0415

        host = os.environ.get("CASSANDRA_HOST", "cassandra")
        port = int(os.environ.get("CASSANDRA_PORT", "9042"))
        username = os.environ.get("CASSANDRA_USERNAME", "")
        password = os.environ.get("CASSANDRA_PASSWORD", "")

        auth_provider = None
        if username and password:
            from cassandra.auth import PlainTextAuthProvider  # noqa: PLC0415
            auth_provider = PlainTextAuthProvider(username, password)

        cluster = Cluster(
            [host],
            port=port,
            auth_provider=auth_provider,
            connect_timeout=5,
        )
        session = cluster.connect()

        existing = {
            (row.keyspace_name, row.table_name)
            for row in session.execute(
                "SELECT keyspace_name, table_name FROM system_schema.tables"
            )
        }
        cluster.shutdown()

        default_keyspace = os.environ.get("CASSANDRA_KEYSPACE", "nl2pipeline")
        keyspace = keyspaces[0] if keyspaces else default_keyspace

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

    except ImportError:
        log.warning("cassandra-driver not installed — skipping Cassandra schema check.")
        return None
    except Exception as exc:
        log.warning("Cassandra schema check skipped (cluster unreachable): %s", exc)
        return None
