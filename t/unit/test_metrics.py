"""Tests for benchlib.metrics."""
from __future__ import annotations

import time

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry import trace as otel_trace

from benchlib.metrics import (
    init_tracer,
    record_publish,
    record_execution,
    record_e2e,
)


@pytest.fixture(scope="module")
def _tracer_exporter():
    """Create a single TracerProvider+InMemorySpanExporter for the whole module."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    otel_trace.set_tracer_provider(provider)
    import benchlib.metrics as m
    m._tracer = otel_trace.get_tracer("benchlib")
    return exporter


@pytest.fixture(autouse=True)
def isolated_tracer(_tracer_exporter):
    """Clear finished spans before each test and return the exporter."""
    _tracer_exporter.clear()
    yield _tracer_exporter


class TestInitTracer:
    def test_init_tracer_no_endpoint(self):
        """Should configure a console exporter and return a TracerProvider."""
        provider = init_tracer(service_name="test", otlp_endpoint=None)
        assert isinstance(provider, TracerProvider)

    def test_init_tracer_sets_module_tracer(self):
        import benchlib.metrics as m
        init_tracer(service_name="test2", otlp_endpoint=None)
        assert m._tracer is not None


class TestRecordPublish:
    def test_creates_span(self, isolated_tracer):
        t0 = time.monotonic_ns()
        t1 = t0 + 1_000_000  # 1 ms
        record_publish("run-1", "task-abc", t0, t1, broker="redis", task="echo")
        spans = isolated_tracer.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "bench.publish"

    def test_span_attributes(self, isolated_tracer):
        t0 = time.monotonic_ns()
        t1 = t0 + 500_000
        record_publish("run-2", "task-xyz", t0, t1, broker="rabbitmq")
        span = isolated_tracer.get_finished_spans()[0]
        attrs = span.attributes
        assert attrs["run_id"] == "run-2"
        assert attrs["task_id"] == "task-xyz"
        assert attrs["broker"] == "rabbitmq"
        assert attrs["t0_ns"] == t0
        assert attrs["t1_ns"] == t1
        assert attrs["publish_duration_ns"] == 500_000


class TestRecordExecution:
    def test_creates_span(self, isolated_tracer):
        t2 = time.monotonic_ns()
        t3, t4, t5 = t2 + 1000, t2 + 2000, t2 + 3000
        record_execution("run-3", "task-e1", t2, t3, t4, t5, pool="prefork")
        spans = isolated_tracer.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "bench.execution"

    def test_span_derived_metrics(self, isolated_tracer):
        t2 = 1000
        t3 = 1500   # queue_wait = 500
        t4 = 2500   # task_runtime = 1000
        t5 = 2700   # result_write = 200
        record_execution("run-4", "task-e2", t2, t3, t4, t5)
        attrs = isolated_tracer.get_finished_spans()[0].attributes
        assert attrs["queue_wait_ns"] == 500
        assert attrs["task_runtime_ns"] == 1000
        assert attrs["result_write_ns"] == 200


class TestRecordE2e:
    def test_creates_span(self, isolated_tracer):
        t0 = time.monotonic_ns()
        t6 = t0 + 10_000_000  # 10 ms
        record_e2e("run-5", "task-f1", t0, t6, backend="redis")
        spans = isolated_tracer.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].name == "bench.e2e"

    def test_e2e_attribute(self, isolated_tracer):
        t0, t6 = 1000, 11000
        record_e2e("run-6", "task-f2", t0, t6)
        attrs = isolated_tracer.get_finished_spans()[0].attributes
        assert attrs["e2e_ns"] == 10000

    def test_span_carries_labels(self, isolated_tracer):
        t0, t6 = 0, 1
        record_e2e("run-7", "task-f3", t0, t6, broker="nats_js", pool="gevent")
        attrs = isolated_tracer.get_finished_spans()[0].attributes
        assert attrs["broker"] == "nats_js"
        assert attrs["pool"] == "gevent"
