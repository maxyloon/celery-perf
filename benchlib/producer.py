"""Load generation for benchlib.

The :class:`BenchmarkProducer` submits Celery tasks according to a
:class:`~benchlib.run_spec.RunSpec` and records T0, T1, and T6 timestamps in
``events.jsonl``.  T2–T5 are captured by the Celery signal hooks in
:mod:`benchlib.metrics` when a real worker is running; for memory:// smoke
runs the app is configured with ``task_always_eager = True`` so that tasks
execute in-process.

Modes
-----
- **burst**: submit *count* tasks as fast as possible, then gather results.
- **open_loop**: submit tasks at *rate* tasks/s for *duration* seconds;
  results are gathered after all submissions.
- **closed_loop**: submit one task at a time and wait for the result before
  submitting the next; repeat *count* times.
- **soak**: open-loop for *duration* seconds (intended for long-running tests).

CLI entry points
----------------
- ``celery-perf-smoke`` — run SMOKE_SPEC (100 tasks, memory://, noop, burst)
- ``celery-perf-run``   — generic runner; accepts ``--spec-json`` and
  ``--count`` / ``--duration``
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from benchlib.metrics import init_tracer, record_publish, record_e2e
from benchlib.results import ResultDir
from benchlib.run_spec import RunSpec, SMOKE_SPEC
from benchlib.workload import (
    app as celery_app,
    noop, echo, sleep_task, cpu_task,
    fanout_task, chain_task, retry_once, fail_task,
)

# ---------------------------------------------------------------------------
# Task registry
# ---------------------------------------------------------------------------

_TASK_MAP: dict[str, Any] = {
    "noop": noop,
    "echo": echo,
    "sleep_task": sleep_task,
    "cpu_task": cpu_task,
    "fanout_task": fanout_task,
    "chain_task": chain_task,
    "retry_once": retry_once,
    "fail_task": fail_task,
}


def _resolve_task(name: str):
    """Return the Celery task object for *name*, raise KeyError if unknown."""
    task = _TASK_MAP.get(name)
    if task is None:
        raise KeyError(
            f"Unknown benchmark task: {name!r}. "
            f"Valid names: {sorted(_TASK_MAP)}"
        )
    return task


def _make_args(spec: RunSpec) -> tuple[tuple, dict]:
    """Return ``(args, kwargs)`` appropriate for the task in *spec*."""
    name = spec.task
    pb = spec.payload_bytes
    if name == "noop":
        return (), {}
    if name == "echo":
        payload = "x" * pb if pb > 0 else ""
        return (payload,), {}
    if name == "sleep_task":
        return (), {"ms": 1}  # minimal sleep in smoke/benchmark
    if name == "cpu_task":
        return (), {"n": max(1, pb // 100)}
    if name == "fanout_task":
        return (), {"n": 4}
    if name == "chain_task":
        return (), {"n": 3, "payload": "x" * max(1, pb)}
    if name == "retry_once":
        return (), {}
    if name == "fail_task":
        return (), {}
    return (), {}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class ProducerResult:
    """Summary of a completed benchmark run."""

    submitted: int = 0
    succeeded: int = 0
    failed: int = 0
    duplicates: int = 0
    lost: int = 0
    duration_s: float = 0.0
    p50_e2e_ms: float = 0.0
    p95_e2e_ms: float = 0.0
    p99_e2e_ms: float = 0.0

    # Extra metadata — not included in latency stats
    meta: dict = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        """Succeeded tasks per second."""
        if self.duration_s <= 0:
            return 0.0
        return self.succeeded / self.duration_s

    def to_dict(self) -> dict:
        return {
            "submitted": self.submitted,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "duplicates": self.duplicates,
            "lost": self.lost,
            "duration_s": self.duration_s,
            "p50_e2e_ms": self.p50_e2e_ms,
            "p95_e2e_ms": self.p95_e2e_ms,
            "p99_e2e_ms": self.p99_e2e_ms,
            "throughput": self.throughput,
        }


def _percentiles(values: list[float], *qs: float) -> list[float]:
    """Return percentiles *qs* (0–100) of *values*; returns 0.0 if empty."""
    if not values:
        return [0.0] * len(qs)
    n = len(values)
    sorted_v = sorted(values)
    result = []
    for q in qs:
        idx = max(0, int(q / 100 * n) - 1)
        result.append(sorted_v[min(idx, n - 1)])
    return result


# ---------------------------------------------------------------------------
# BenchmarkProducer
# ---------------------------------------------------------------------------


class BenchmarkProducer:
    """Submits Celery tasks and records timing events.

    Parameters
    ----------
    spec:
        The :class:`~benchlib.run_spec.RunSpec` describing this run.
    result_dir:
        An open :class:`~benchlib.results.ResultDir` for writing events.
    tracer_config:
        Optional dict passed to :func:`~benchlib.metrics.init_tracer`; keys
        are ``service_name`` and ``otlp_endpoint``.
    """

    def __init__(
        self,
        spec: RunSpec,
        result_dir: ResultDir,
        tracer_config: dict | None = None,
    ) -> None:
        self.spec = spec
        self.result_dir = result_dir
        tracer_config = tracer_config or {}
        init_tracer(
            service_name=tracer_config.get("service_name", "benchlib"),
            otlp_endpoint=tracer_config.get("otlp_endpoint"),
        )
        self._task = _resolve_task(spec.task)
        self._args, self._kwargs = _make_args(spec)

        # Use eager mode for in-process brokers so no worker is needed.
        self._eager = "memory" in spec.broker

    def run(self, count: int = 100, duration: float = 0.0) -> ProducerResult:
        """Dispatch to the correct mode and return a :class:`ProducerResult`."""
        mode = self.spec.mode
        if mode == "burst":
            return self._burst(count)
        if mode == "open_loop":
            rate = self.spec.prefetch or 10.0  # tasks/s
            return self._open_loop(rate, duration or 30.0)
        if mode == "closed_loop":
            return self._closed_loop(count)
        if mode == "soak":
            rate = self.spec.prefetch or 10.0
            return self._soak(rate, duration or 300.0)
        raise ValueError(f"Unknown mode: {mode!r}")

    # ------------------------------------------------------------------
    # Context manager helpers
    # ------------------------------------------------------------------

    def __enter__(self):
        if self._eager:
            celery_app.conf.task_always_eager = True
            celery_app.conf.task_eager_propagates = False
        return self

    def __exit__(self, *args):
        if self._eager:
            celery_app.conf.task_always_eager = False
            celery_app.conf.task_eager_propagates = False

    # ------------------------------------------------------------------
    # Internal: single-task submission + retrieval
    # ------------------------------------------------------------------

    def _submit(self) -> tuple[Any, int, int]:
        """Submit one task; return ``(async_result, t0_ns, t1_ns)``."""
        t0 = time.monotonic_ns()
        ar = self._task.apply_async(self._args, self._kwargs)
        t1 = time.monotonic_ns()
        return ar, t0, t1

    def _collect(
        self,
        ar: Any,
        t0: int,
        t1: int,
        run_id: str,
    ) -> tuple[bool, float]:
        """Wait for *ar* to finish; record events; return ``(ok, e2e_ms)``."""
        task_id = ar.id

        # Record publish-side timestamps
        record_publish(run_id, task_id, t0, t1, **self._span_labels())
        self.result_dir.append_event(
            {"run_id": run_id, "task_id": task_id, "T0": t0, "T1": t1}
        )

        # Retrieve result
        ok = True
        t6 = t1  # fallback if get() fails
        try:
            ar.get(timeout=60, propagate=False)
            t6 = time.monotonic_ns()
        except Exception:
            ok = False

        # Record T6
        record_e2e(run_id, task_id, t0, t6, **self._span_labels())
        self.result_dir.append_event(
            {"run_id": run_id, "task_id": task_id, "T6": t6}
        )

        if ar.failed():
            ok = False

        e2e_ms = (t6 - t0) / 1_000_000.0
        return ok, e2e_ms

    def _span_labels(self) -> dict:
        return {
            "broker": self.spec.broker,
            "backend": self.spec.backend,
            "pool": self.spec.pool,
            "task": self.spec.task,
        }

    # ------------------------------------------------------------------
    # Modes
    # ------------------------------------------------------------------

    def _burst(self, count: int) -> ProducerResult:
        """Submit *count* tasks as fast as possible; gather after all submitted."""
        run_id = self.spec.run_id
        start = time.monotonic()

        # Phase 1: submit all tasks
        pending: list[tuple[Any, int, int]] = []
        submitted = 0
        for _ in range(count):
            try:
                ar, t0, t1 = self._submit()
                pending.append((ar, t0, t1))
                submitted += 1
            except Exception as exc:
                print(f"[producer] submit error: {exc}", file=sys.stderr)

        # Phase 2: collect results
        e2e_times: list[float] = []
        succeeded = failed = 0
        for ar, t0, t1 in pending:
            ok, e2e_ms = self._collect(ar, t0, t1, run_id)
            if ok:
                succeeded += 1
                e2e_times.append(e2e_ms)
            else:
                failed += 1

        duration_s = time.monotonic() - start
        p50, p95, p99 = _percentiles(e2e_times, 50, 95, 99)
        return ProducerResult(
            submitted=submitted,
            succeeded=succeeded,
            failed=failed,
            duration_s=duration_s,
            p50_e2e_ms=p50,
            p95_e2e_ms=p95,
            p99_e2e_ms=p99,
        )

    def _closed_loop(self, count: int) -> ProducerResult:
        """Submit one task at a time; wait for result before next submission."""
        run_id = self.spec.run_id
        start = time.monotonic()
        e2e_times: list[float] = []
        succeeded = failed = submitted = 0

        for _ in range(count):
            try:
                ar, t0, t1 = self._submit()
                submitted += 1
            except Exception as exc:
                print(f"[producer] submit error: {exc}", file=sys.stderr)
                continue
            ok, e2e_ms = self._collect(ar, t0, t1, run_id)
            if ok:
                succeeded += 1
                e2e_times.append(e2e_ms)
            else:
                failed += 1

        duration_s = time.monotonic() - start
        p50, p95, p99 = _percentiles(e2e_times, 50, 95, 99)
        return ProducerResult(
            submitted=submitted,
            succeeded=succeeded,
            failed=failed,
            duration_s=duration_s,
            p50_e2e_ms=p50,
            p95_e2e_ms=p95,
            p99_e2e_ms=p99,
        )

    def _open_loop(self, rate: float, duration: float) -> ProducerResult:
        """Submit tasks at *rate* tasks/s for *duration* seconds."""
        run_id = self.spec.run_id
        interval = 1.0 / rate if rate > 0 else 0.0
        deadline = time.monotonic() + duration
        pending: list[tuple[Any, int, int]] = []
        submitted = 0

        while time.monotonic() < deadline:
            loop_start = time.monotonic()
            try:
                ar, t0, t1 = self._submit()
                pending.append((ar, t0, t1))
                submitted += 1
            except Exception as exc:
                print(f"[producer] submit error: {exc}", file=sys.stderr)
            elapsed = time.monotonic() - loop_start
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

        start = time.monotonic() - duration  # approximate
        e2e_times: list[float] = []
        succeeded = failed = 0
        for ar, t0, t1 in pending:
            ok, e2e_ms = self._collect(ar, t0, t1, run_id)
            if ok:
                succeeded += 1
                e2e_times.append(e2e_ms)
            else:
                failed += 1

        p50, p95, p99 = _percentiles(e2e_times, 50, 95, 99)
        return ProducerResult(
            submitted=submitted,
            succeeded=succeeded,
            failed=failed,
            duration_s=duration,
            p50_e2e_ms=p50,
            p95_e2e_ms=p95,
            p99_e2e_ms=p99,
        )

    def _soak(self, rate: float, duration: float) -> ProducerResult:
        """Alias for open-loop; intended for long-running soak tests."""
        return self._open_loop(rate, duration)


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


def smoke_cli() -> None:
    """Run 100 noop tasks against memory:// and print a summary."""
    import argparse

    parser = argparse.ArgumentParser(description="celery-perf smoke test")
    parser.add_argument(
        "--count", type=int, default=100,
        help="Number of tasks (default: 100)"
    )
    parser.add_argument(
        "--out", default=os.getcwd(),
        help="Base directory for results/ output (default: cwd)"
    )
    args = parser.parse_args()

    spec = SMOKE_SPEC
    run_id = spec.run_id
    base_path = Path(args.out)
    rd = ResultDir(run_id, base_path)
    rd.write_run_json(spec, {"cli": "smoke"})

    print(f"[smoke] run_id={run_id}  count={args.count}  broker=memory://")
    with BenchmarkProducer(spec, rd) as producer:
        result = producer.run(count=args.count)

    print(
        f"[smoke] submitted={result.submitted}"
        f"  succeeded={result.succeeded}"
        f"  failed={result.failed}"
        f"  duration={result.duration_s:.3f}s"
        f"  throughput={result.throughput:.1f} tasks/s"
        f"  p50={result.p50_e2e_ms:.2f}ms"
        f"  p95={result.p95_e2e_ms:.2f}ms"
        f"  p99={result.p99_e2e_ms:.2f}ms"
    )
    print(f"[smoke] events written to {rd.run_dir / 'events.jsonl'}")

    if result.failed > 0:
        print(f"[smoke] WARNING: {result.failed} task(s) failed", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


def run_cli() -> None:
    """Generic benchmark runner — reads RunSpec from JSON and runs it."""
    import argparse

    parser = argparse.ArgumentParser(description="celery-perf run")
    parser.add_argument("--spec-json", required=True, help="Path to RunSpec JSON file")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--out", default=os.getcwd())
    parser.add_argument("--otlp-endpoint", default=None)
    args = parser.parse_args()

    with open(args.spec_json) as f:
        spec = RunSpec.from_dict(json.load(f))

    run_id = spec.run_id
    base_path = Path(args.out)
    rd = ResultDir(run_id, base_path)
    rd.write_run_json(spec, {"cli": "run"})

    tracer_config = {"otlp_endpoint": args.otlp_endpoint}
    print(f"[run] run_id={run_id}  mode={spec.mode}  broker={spec.broker}")
    with BenchmarkProducer(spec, rd, tracer_config) as producer:
        result = producer.run(count=args.count, duration=args.duration)

    print(json.dumps(result.to_dict(), indent=2))
    sys.exit(0 if result.failed == 0 else 1)
