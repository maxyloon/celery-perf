"""Celery application and benchmark task suite for benchlib.

The Celery app is configured via environment variables so the same code runs
in every benchmark environment without modification:

    CELERY_BROKER_URL         — broker URL (default: ``memory://``)
    CELERY_RESULT_BACKEND     — result backend URL (default: ``cache+memory://``)
    CELERY_POOL               — concurrency pool (default: ``solo``)
    CELERY_CONCURRENCY        — worker concurrency (default: ``1``)
    CELERY_WORKER_PREFETCH_MULTIPLIER — prefetch (default: ``1``)
    CELERY_TASK_SERIALIZER    — task serialiser (default: ``json``)
    CELERY_RESULT_SERIALIZER  — result serialiser (default: ``json``)
    OTEL_EXPORTER_OTLP_ENDPOINT — OTel endpoint; omit to use console exporter

Task suite
----------
All task names are prefixed ``bench.`` to avoid collisions.

    noop          — no payload, no result; measures broker + Celery overhead
    echo          — returns the payload; exercises serialisation + result backend
    sleep_task    — sleeps for ``ms`` milliseconds; measures queue fairness
    cpu_task      — busy-waits for ~``n`` milliseconds; measures pool CPU behaviour
    fanout_task   — fires a group of ``n`` noop tasks; measures canvas overhead
    chain_task    — fires a chain of ``n`` echo tasks; measures orchestration overhead
    retry_once    — retries once, succeeds on second attempt
    fail_task     — always raises ValueError; exercises failure path
"""
from __future__ import annotations

import os
import time

from celery import Celery, group, chain

# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "memory://")
_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "cache+memory://")

app = Celery(
    "benchlib",
    broker=_BROKER_URL,
    backend=_RESULT_BACKEND,
)

app.conf.update(
    # Serialisation
    task_serializer=os.environ.get("CELERY_TASK_SERIALIZER", "json"),
    result_serializer=os.environ.get("CELERY_RESULT_SERIALIZER", "json"),
    accept_content=["json", "msgpack", "pickle"],
    # Worker
    worker_prefetch_multiplier=int(
        os.environ.get("CELERY_WORKER_PREFETCH_MULTIPLIER", "1")
    ),
    worker_hijack_root_logger=False,
    worker_log_color=False,
    # Results
    result_expires=300,
    # Reduce verbosity in benchmark runs
    worker_redirect_stdouts=False,
)


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------


@app.task(name="bench.noop", ignore_result=True)
def noop() -> None:
    """No-op task — measures broker + Celery overhead floor."""


@app.task(name="bench.echo")
def echo(payload: bytes | str | None = None) -> bytes | str | None:
    """Echo the payload back — exercises serialisation + result backend."""
    return payload


@app.task(name="bench.sleep_task")
def sleep_task(ms: int = 10) -> None:
    """Sleep for ``ms`` milliseconds — measures queue fairness / scheduling."""
    time.sleep(ms / 1000.0)


@app.task(name="bench.cpu_task")
def cpu_task(n: int = 10) -> int:
    """Busy-wait for approximately ``n`` milliseconds.

    Returns the iteration count so the result is not optimised away.
    """
    deadline = time.monotonic() + n / 1000.0
    i = 0
    while time.monotonic() < deadline:
        i += 1
    return i


@app.task(name="bench.fanout_task")
def fanout_task(n: int = 4) -> object:
    """Fire a group of ``n`` noop tasks and return the GroupResult."""
    result = group(noop.s() for _ in range(n)).apply_async()
    result.get(timeout=60)
    return n


@app.task(name="bench.chain_task")
def chain_task(n: int = 3, payload: str = "x") -> object:
    """Fire a chain of ``n`` echo tasks and return the final result."""
    if n < 1:
        return payload
    tasks = chain(echo.s(payload) for _ in range(n))
    return tasks.apply_async().get(timeout=60)


@app.task(name="bench.retry_once", bind=True, max_retries=1)
def retry_once(self) -> str:
    """Retry once on first invocation, succeed on second."""
    if self.request.retries == 0:
        raise self.retry(countdown=0)
    return "ok"


@app.task(name="bench.fail_task")
def fail_task() -> None:
    """Always raises — exercises the failure path and result backend error write."""
    raise ValueError("intentional benchmark failure")
