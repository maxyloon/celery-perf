"""Celery task definitions for the NATS benchmark.

All tasks are intentionally simple so that timings reflect transport overhead,
not task logic.

Import path for the worker:  examples.nats_celery.tasks
"""
from __future__ import annotations

import time

from celery import Celery
from nats.js.api import StorageType

BROKER_URL = "nats://localhost:4222"
RESULT_BACKEND = "rpc://"

# Transport options shared by broker and result backend.
TRANSPORT_OPTIONS: dict = {
    "stream_config": {"storage": StorageType.MEMORY},
    # Poll frequently so chain steps are picked up quickly (default is 5 s).
    "wait_time_seconds": 0.1,
}

app = Celery(
    "nats_bench",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)

app.conf.update(
    # Result backend
    result_backend=RESULT_BACKEND,
    # Fair dispatch — one message prefetched per worker slot
    worker_prefetch_multiplier=1,
    # Solo pool: single-threaded, no fork overhead — good for benchmarks
    worker_pool="solo",
    worker_concurrency=1,
    # Transport
    broker_transport_options=TRANSPORT_OPTIONS,
    result_transport_options=TRANSPORT_OPTIONS,
    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Reduce console noise during benchmarks
    worker_hijack_root_logger=False,
    worker_log_color=False,
    # Result expiry — keep them long enough for benchmarking
    result_expires=300,
)


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

@app.task(name="bench.add")
def add(x: int, y: int) -> int:
    """Add two integers.  Used by throughput, chain, and fan-out scenarios."""
    return x + y


@app.task(name="bench.process_payload")
def process_payload(data: str) -> int:
    """Consume a string payload and return its length.

    The benchmark passes payloads of different sizes to measure how
    payload size affects throughput.
    """
    return len(data)


@app.task(
    name="bench.flaky",
    bind=True,
    max_retries=5,
    default_retry_delay=0.05,  # 50 ms between retries
)
def flaky(self, fail_count: int = 1) -> str:
    """Raise an exception until *fail_count* retries have been exhausted.

    Used to measure retry overhead.  ``fail_count=1`` means the task
    fails once, then succeeds on the first retry.
    """
    if self.request.retries < fail_count:
        raise self.retry(exc=ValueError(f"deliberate failure #{self.request.retries + 1}"))
    return "ok"
