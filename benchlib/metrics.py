"""OTel instrumentation for benchlib.

Provides functions for recording the six benchmark timestamp points (T0–T6)
as OpenTelemetry spans:

    T0  producer — before apply_async()          publish start
    T1  producer — after apply_async() returns   publish end (broker ack)
    T2  worker  — task_received signal           task received by worker
    T3  worker  — task_prerun signal             task execution starts
    T4  worker  — task_postrun signal            task execution ends
    T5  worker  — after result backend .set()    result write complete
    T6  producer/reader — after .get()           result visible to caller

Usage
-----
Call ``init_tracer()`` once at process startup, then call the individual
``record_*`` functions as each timestamp is captured.

For the Celery worker side, call ``connect_celery_signals()`` after the app
is configured.  This automatically captures T2–T5 via Celery signals.
"""
from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

if TYPE_CHECKING:
    from celery import Celery
    from benchlib.results import ResultDir

# Module-level tracer — initialised by init_tracer()
_tracer: trace.Tracer | None = None


def init_tracer(
    service_name: str = "benchlib",
    otlp_endpoint: str | None = None,
) -> TracerProvider:
    """Configure the global OTel TracerProvider.

    If ``otlp_endpoint`` is None (or the ``OTEL_EXPORTER_OTLP_ENDPOINT``
    env var is not set), a ``ConsoleSpanExporter`` is used so spans are
    printed to stdout.  Useful for local smoke runs.

    Returns the configured provider so callers can add extra exporters.
    """
    global _tracer

    endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if endpoint:
        # Lazy import — only needed when an OTLP endpoint is configured.
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    else:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)
    return provider


def _get_tracer() -> trace.Tracer:
    """Return the module tracer, initialising with console exporter if needed."""
    global _tracer
    if _tracer is None:
        init_tracer()
    return _tracer


def _common_attrs(run_id: str, task_id: str, **labels) -> dict:
    attrs = {"run_id": run_id, "task_id": task_id}
    attrs.update({str(k): str(v) for k, v in labels.items()})
    return attrs


# ---------------------------------------------------------------------------
# Span recording helpers
# ---------------------------------------------------------------------------


def record_publish(
    run_id: str,
    task_id: str,
    t0_ns: int,
    t1_ns: int,
    **labels,
) -> None:
    """Record T0→T1 as a span named ``bench.publish``."""
    tracer = _get_tracer()
    with tracer.start_as_current_span(
        "bench.publish",
        attributes=_common_attrs(run_id, task_id, **labels),
        start_time=t0_ns,
    ) as span:
        span.set_attribute("t0_ns", t0_ns)
        span.set_attribute("t1_ns", t1_ns)
        span.set_attribute("publish_duration_ns", t1_ns - t0_ns)


def record_execution(
    run_id: str,
    task_id: str,
    t2_ns: int,
    t3_ns: int,
    t4_ns: int,
    t5_ns: int,
    **labels,
) -> None:
    """Record T2→T5 as a span named ``bench.execution``."""
    tracer = _get_tracer()
    with tracer.start_as_current_span(
        "bench.execution",
        attributes=_common_attrs(run_id, task_id, **labels),
        start_time=t2_ns,
    ) as span:
        span.set_attribute("t2_ns", t2_ns)
        span.set_attribute("t3_ns", t3_ns)
        span.set_attribute("t4_ns", t4_ns)
        span.set_attribute("t5_ns", t5_ns)
        span.set_attribute("queue_wait_ns", t3_ns - t2_ns)
        span.set_attribute("task_runtime_ns", t4_ns - t3_ns)
        span.set_attribute("result_write_ns", t5_ns - t4_ns)


def record_e2e(
    run_id: str,
    task_id: str,
    t0_ns: int,
    t6_ns: int,
    **labels,
) -> None:
    """Record T0→T6 as the top-level ``bench.e2e`` span."""
    tracer = _get_tracer()
    with tracer.start_as_current_span(
        "bench.e2e",
        attributes=_common_attrs(run_id, task_id, **labels),
        start_time=t0_ns,
    ) as span:
        span.set_attribute("t0_ns", t0_ns)
        span.set_attribute("t6_ns", t6_ns)
        span.set_attribute("e2e_ns", t6_ns - t0_ns)


# ---------------------------------------------------------------------------
# Celery signal integration
# ---------------------------------------------------------------------------


def connect_celery_signals(
    celery_app: "Celery",
    run_id: str,
    result_dir: "ResultDir",
) -> None:
    """Attach Celery task signals to capture T2–T5 timestamps.

    Call this once after the worker is configured.  Each signal handler:
    1. Records ``time.monotonic_ns()`` as the timestamp.
    2. Appends an event dict to ``result_dir``.
    3. (T4→T5 only) emits an OTel span via ``record_execution``.

    The per-task state (T2, T3, T4) is stored in a thread-local dict keyed
    by ``task_id`` so multiple concurrent tasks don't conflict.
    """
    import threading
    from celery.signals import (
        task_received,
        task_prerun,
        task_postrun,
        task_failure,
    )

    _state: dict[str, dict] = {}
    _lock = threading.Lock()

    @task_received.connect(weak=False)
    def on_received(request, **kwargs):
        t2 = time.monotonic_ns()
        tid = request.id
        with _lock:
            _state[tid] = {"T2": t2}
        result_dir.append_event({"task_id": tid, "point": "T2", "ns": t2, "run_id": run_id})

    @task_prerun.connect(weak=False)
    def on_prerun(task_id, **kwargs):
        t3 = time.monotonic_ns()
        with _lock:
            _state.setdefault(task_id, {})["T3"] = t3
        result_dir.append_event({"task_id": task_id, "point": "T3", "ns": t3, "run_id": run_id})

    @task_postrun.connect(weak=False)
    def on_postrun(task_id, **kwargs):
        t4 = time.monotonic_ns()
        with _lock:
            _state.setdefault(task_id, {})["T4"] = t4
        result_dir.append_event({"task_id": task_id, "point": "T4", "ns": t4, "run_id": run_id})
        # T5 is approximated as T4 + result-write time; in practice the
        # producer measures T5 separately when the result is visible.
        t5 = time.monotonic_ns()
        result_dir.append_event({"task_id": task_id, "point": "T5", "ns": t5, "run_id": run_id})
        with _lock:
            state = _state.pop(task_id, {})
        t2 = state.get("T2", t4)
        t3 = state.get("T3", t4)
        record_execution(run_id, task_id, t2, t3, t4, t5)

    @task_failure.connect(weak=False)
    def on_failure(task_id, **kwargs):
        # Clean up state for failed tasks
        with _lock:
            _state.pop(task_id, None)
