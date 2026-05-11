"""Tests for benchlib.producer and benchlib.sla."""
from __future__ import annotations

import pytest

from benchlib.producer import BenchmarkProducer, ProducerResult, _percentiles
from benchlib.results import ResultDir
from benchlib.run_spec import SMOKE_SPEC, RunSpec
from benchlib.sla import evaluate_sla, SLAResult


# ---------------------------------------------------------------------------
# ProducerResult helpers
# ---------------------------------------------------------------------------


class TestPercentiles:
    def test_empty(self):
        p50, p95, p99 = _percentiles([], 50, 95, 99)
        assert p50 == 0.0 and p95 == 0.0 and p99 == 0.0

    def test_single(self):
        (p50,) = _percentiles([5.0], 50)
        assert p50 == 5.0

    def test_sorted_order(self):
        values = [10, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        p50, p95 = _percentiles(values, 50, 95)
        assert p50 <= p95


class TestProducerResult:
    def test_throughput_zero_duration(self):
        r = ProducerResult(succeeded=10, duration_s=0.0)
        assert r.throughput == 0.0

    def test_throughput(self):
        r = ProducerResult(succeeded=100, duration_s=10.0)
        assert r.throughput == 10.0

    def test_to_dict_keys(self):
        r = ProducerResult(submitted=5, succeeded=5)
        d = r.to_dict()
        for key in ("submitted", "succeeded", "failed", "p50_e2e_ms", "p95_e2e_ms", "p99_e2e_ms"):
            assert key in d


# ---------------------------------------------------------------------------
# BenchmarkProducer integration — smoke via memory://
# ---------------------------------------------------------------------------


class TestBenchmarkProducerSmoke:
    """Run a small burst against memory:// (eager mode)."""

    def test_burst_smoke(self, tmp_path):
        spec = SMOKE_SPEC
        rd = ResultDir(spec.run_id, tmp_path)
        with BenchmarkProducer(spec, rd) as producer:
            result = producer.run(count=10)

        assert result.submitted == 10
        assert result.succeeded == 10
        assert result.failed == 0
        assert result.p95_e2e_ms >= 0.0

    def test_events_written(self, tmp_path):
        spec = SMOKE_SPEC
        rd = ResultDir(spec.run_id, tmp_path)
        with BenchmarkProducer(spec, rd) as producer:
            producer.run(count=5)

        # Each task writes 2 events: one for T0+T1, one for T6
        events = rd.read_events()
        assert len(events) == 10  # 5 tasks × 2 events

    def test_events_have_t0_t1_t6(self, tmp_path):
        spec = SMOKE_SPEC
        rd = ResultDir(spec.run_id, tmp_path)
        with BenchmarkProducer(spec, rd) as producer:
            producer.run(count=3)

        events = rd.read_events()
        by_task: dict[str, dict] = {}
        for ev in events:
            tid = ev.get("task_id", "")
            if tid not in by_task:
                by_task[tid] = {}
            by_task[tid].update(ev)

        for tid, ev in by_task.items():
            assert "T0" in ev, f"task {tid} missing T0"
            assert "T1" in ev, f"task {tid} missing T1"
            assert "T6" in ev, f"task {tid} missing T6"

    def test_closed_loop(self, tmp_path):
        spec = SMOKE_SPEC
        rd = ResultDir(spec.run_id, tmp_path)
        with BenchmarkProducer(spec, rd) as producer:
            result = producer._closed_loop(count=5)

        assert result.submitted == 5
        assert result.succeeded == 5

    def test_echo_task_burst(self, tmp_path):
        spec = RunSpec(
            broker="memory",
            broker_profile="default",
            backend="none",
            pool="solo",
            task="echo",
            payload_bytes=64,
            mode="burst",
            replicas=1,
            concurrency=1,
            prefetch=1,
            resource_class="small",
        )
        rd = ResultDir(spec.run_id, tmp_path)
        with BenchmarkProducer(spec, rd) as producer:
            result = producer.run(count=5)

        assert result.succeeded == 5


# ---------------------------------------------------------------------------
# SLA evaluation
# ---------------------------------------------------------------------------


class TestEvaluateSla:
    def _make_result(self, p50=10.0, p95=50.0, p99=100.0, failed=0, submitted=100, duration_s=5.0):
        return ProducerResult(
            submitted=submitted,
            succeeded=submitted - failed,
            failed=failed,
            duration_s=duration_s,
            p50_e2e_ms=p50,
            p95_e2e_ms=p95,
            p99_e2e_ms=p99,
        )

    def test_all_pass(self):
        r = self._make_result()
        sla = evaluate_sla(r, SMOKE_SPEC)
        assert sla.passed
        assert sla.violations == []

    def test_p95_violation(self):
        r = self._make_result(p95=600.0)
        sla = evaluate_sla(r, SMOKE_SPEC, p95_ms=500.0)
        assert not sla.passed
        assert sla.p95_ok is False
        assert any("p95" in v for v in sla.violations)

    def test_p99_violation(self):
        r = self._make_result(p99=1500.0)
        sla = evaluate_sla(r, SMOKE_SPEC, p99_ms=1000.0)
        assert not sla.passed
        assert any("p99" in v for v in sla.violations)

    def test_failure_rate_violation(self):
        r = self._make_result(failed=5, submitted=100)
        sla = evaluate_sla(r, SMOKE_SPEC, max_failure_rate=0.01)
        assert not sla.passed
        assert any("failure_rate" in v for v in sla.violations)

    def test_failure_rate_ok_at_threshold(self):
        r = self._make_result(failed=1, submitted=100)
        sla = evaluate_sla(r, SMOKE_SPEC, max_failure_rate=0.01)
        assert sla.passed

    def test_throughput_violation(self):
        r = self._make_result(duration_s=100.0)
        # submitted=100, succeeded=100 → 1 task/s < 10 task/s
        sla = evaluate_sla(r, SMOKE_SPEC, min_throughput=10.0)
        assert not sla.passed
        assert any("throughput" in v for v in sla.violations)

    def test_summary_pass(self):
        r = self._make_result()
        sla = evaluate_sla(r, SMOKE_SPEC)
        assert sla.summary() == "PASS"

    def test_summary_fail(self):
        r = self._make_result(p95=9999.0)
        sla = evaluate_sla(r, SMOKE_SPEC, p95_ms=100.0)
        assert "FAIL" in sla.summary()
        assert "p95" in sla.summary()
