from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def connect(
    host: str,
    port: int,
    username: str = "",
    password: str = "",
    keyspace: str = "nl2pipeline",
) -> Any:
    """
    Open a single Cassandra session shared by the pipeline_runs observability
    log and the harness's Stage 3 schema check (see backend/app.py lifespan).

    Auth is optional — PlainTextAuthProvider is only used when both username
    and password are non-empty; the default Cassandra setup
    (AllowAllAuthenticator) works without creds.

    Returns None (features relying on Cassandra become non-fatal no-ops) if
    cassandra-driver isn't installed or the cluster is unreachable at startup.
    """
    try:
        from cassandra.cluster import Cluster  # noqa: PLC0415
    except ImportError:
        log.warning("cassandra-driver not installed — Cassandra features disabled.")
        return None

    auth_provider = None
    if username and password:
        from cassandra.auth import PlainTextAuthProvider  # noqa: PLC0415
        auth_provider = PlainTextAuthProvider(username, password)

    try:
        cluster = Cluster([host], port=port, auth_provider=auth_provider, connect_timeout=5)
        session = cluster.connect(keyspace)
        log.info("Connected to Cassandra at %s:%d (keyspace=%s)", host, port, keyspace)
        return session
    except Exception as exc:
        log.warning("Cassandra unreachable at %s:%d — features disabled: %s", host, port, exc)
        return None
