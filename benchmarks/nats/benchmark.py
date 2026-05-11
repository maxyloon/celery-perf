#!/usr/bin/env python3
"""Celery NATS benchmark — 4 non-trivial use-cases.

Scenarios
---------
1. Task throughput   — fire N add(i,i) tasks via a group, measure tasks/s end-to-end
2. Task chaining     — chain(add → add → add) × N, measure per-chain latency
3. Fan-out (group)   — group of N process_payload tasks, 3 payload sizes
4. Retry / error     — flaky task with 2 forced retries × N, measure overhead

Worker management
-----------------
A Celery worker is started in a child process (multiprocessing.Process) using
the solo pool.  No external CLI commands are required.  The benchmark waits for
the worker to report ready before sending any tasks.

Usage
-----
    # From the celery repo root:
    python -m examples.nats_celery.benchmark

    # Options:
    python -m examples.nats_celery.benchmark --count 100 --server localhost
    python -m examples.nats_celery.benchmark --no-retry --no-fanout

Requirements
------------
    nats-server running with JetStream enabled:  nats-server -js
    Python packages: celery, kombu (local), nats-py
"""
from __future__ import annotations

import argparse
import logging
import multiprocessing
import statistics
import sys
import time
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Silence nats-py internal read-loop error logs during drain/close teardown.
# ---------------------------------------------------------------------------
logging.getLogger("nats").setLevel(logging.CRITICAL)
logging.getLogger("celery").setLevel(logging.WARNING)
logging.getLogger("kombu").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Task import — must happen after logging setup so workers inherit the level.
# ---------------------------------------------------------------------------
from benchmarks.nats.tasks import add, app, flaky, process_payload  # noqa: E402

# ---------------------------------------------------------------------------
# Worker management
# ---------------------------------------------------------------------------

_WORKER_READY_TIMEOUT = 30  # seconds to wait for the worker to come online


def _worker_entry(ready_event: multiprocessing.Event) -> None:
    """Entry point for the worker child process."""
    # Suppress noisy output
    import logging as _log
    _log.getLogger("celery").setLevel(_log.WARNING)
    _log.getLogger("nats").setLevel(_log.CRITICAL)
    _log.getLogger("kombu").setLevel(_log.WARNING)

    from celery.signals import worker_ready

    @worker_ready.connect
    def on_ready(**kwargs):
        ready_event.set()

    app.worker_main(
        argv=[
            "worker",
            "--pool=solo",
            "--concurrency=1",
            "--loglevel=warning",
            "--without-heartbeat",
            "--without-mingle",
        ]
    )


def start_worker() -> tuple[multiprocessing.Process, multiprocessing.Event]:
    """Spawn a Celery worker in a child process; return (process, ready_event)."""
    ready = multiprocessing.Event()
    proc = multiprocessing.Process(target=_worker_entry, args=(ready,), daemon=True)
    proc.start()
    if not ready.wait(timeout=_WORKER_READY_TIMEOUT):
        proc.terminate()
        raise RuntimeError(
            f"Worker did not become ready within {_WORKER_READY_TIMEOUT}s. "
            "Is the NATS server running with JetStream enabled? (nats-server -js)"
        )
    return proc, ready


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    name: str
    count: int
    elapsed_s: float
    latency_samples: list[float] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        return self.count / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def lat_mean_ms(self) -> float:
        return statistics.mean(self.latency_samples) * 1000 if self.latency_samples else 0.0

    @property
    def lat_p50_ms(self) -> float:
        return statistics.median(self.latency_samples) * 1000 if self.latency_samples else 0.0

    @property
    def lat_p95_ms(self) -> float:
        if not self.latency_samples:
            return 0.0
        s = sorted(self.latency_samples)
        return s[max(0, int(len(s) * 0.95) - 1)] * 1000

    @property
    def lat_min_ms(self) -> float:
        return min(self.latency_samples) * 1000 if self.latency_samples else 0.0


# ---------------------------------------------------------------------------
# Scenario 1: Task throughput
# ---------------------------------------------------------------------------

def bench_throughput(count: int) -> ScenarioResult:
    """Fire *count* add() tasks as a group; time until all results collected."""
    from celery import group as celery_group

    tasks = celery_group(add.s(i, i) for i in range(count))
    t0 = time.perf_counter()
    result = tasks.apply_async()
    result.get(timeout=120)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(name="throughput", count=count, elapsed_s=elapsed)


# ---------------------------------------------------------------------------
# Scenario 2: Task chaining
# ---------------------------------------------------------------------------

def bench_chain(count: int) -> ScenarioResult:
    """chain(add(1,1) | add(2) | add(3)) × count; measure per-chain latency."""
    from celery import chain as celery_chain

    latencies: list[float] = []
    t0 = time.perf_counter()
    for _ in range(count):
        t_chain = time.perf_counter()
        result = celery_chain(add.s(1, 1), add.s(2), add.s(3)).apply_async()
        result.get(timeout=30)
        latencies.append(time.perf_counter() - t_chain)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(
        name="chain",
        count=count,
        elapsed_s=elapsed,
        latency_samples=latencies,
    )


# ---------------------------------------------------------------------------
# Scenario 3: Fan-out (group) by payload size
# ---------------------------------------------------------------------------

FANOUT_PAYLOAD_SIZES: dict[str, int] = {
    "small":  100,
    "medium": 1_024,
    "large":  10 * 1_024,
}


def bench_fanout(count: int, size_name: str) -> ScenarioResult:
    """group(count × process_payload) for a given payload size; measure tasks/s."""
    from celery import group as celery_group

    payload = "x" * FANOUT_PAYLOAD_SIZES[size_name]
    tasks = celery_group(process_payload.s(payload) for _ in range(count))
    t0 = time.perf_counter()
    result = tasks.apply_async()
    result.get(timeout=120)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(
        name=f"fanout/{size_name}",
        count=count,
        elapsed_s=elapsed,
        extra={"payload_bytes": FANOUT_PAYLOAD_SIZES[size_name]},
    )


# ---------------------------------------------------------------------------
# Scenario 4: Retry / error path
# ---------------------------------------------------------------------------

def bench_retry(count: int, fail_count: int = 2) -> ScenarioResult:
    """flaky(fail_count) × count; each task retries *fail_count* times."""
    """flaky(fail_count) × count; each task retries *fail_count* times."""
    latencies: list[float] = []
    t0 = time.perf_counter()
    for _ in range(count):
        t_task = time.perf_counter()
        result = flaky.apply_async(kwargs={"fail_count": fail_count})
        result.get(timeout=30)
        latencies.append(time.perf_counter() - t_task)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(
        name=f"retry(fail_count={fail_count})",
        count=count,
        elapsed_s=elapsed,
        latency_samples=latencies,
        extra={"fail_count": fail_count},
    )


# ---------------------------------------------------------------------------
# Scenario 5: NATS KV backend — task throughput with KV result store
# ---------------------------------------------------------------------------

def bench_kv_backend(count: int, server: str = "localhost", port: int = 4222) -> ScenarioResult:
    """Fire *count* add() tasks using the NATS KV backend for result storage.

    Creates a separate Celery app pointing at nats+kv:// so the default
    ``app`` (which uses rpc://) is untouched.
    """
    from celery import Celery as _Celery, group as celery_group
    from nats.js.api import StorageType

    kv_app = _Celery(
        "nats_bench_kv",
        broker=f"nats://{server}:{port}",
        backend=f"nats+kv://{server}:{port}",
    )
    kv_app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        broker_transport_options={
            "stream_config": {"storage": StorageType.MEMORY},
            "wait_time_seconds": 0.1,
        },
        result_backend_transport_options={
            "nats_kv_bucket": "bench_results",
            "storage": StorageType.MEMORY,
        },
    )

    @kv_app.task(name="bench.add_kv")
    def _add(x, y):
        return x + y

    tasks = celery_group(_add.s(i, i) for i in range(count))
    t0 = time.perf_counter()
    result = tasks.apply_async()
    result.get(timeout=120)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(name="kv_backend/throughput", count=count, elapsed_s=elapsed)


# ---------------------------------------------------------------------------
# Scenario 6: Chord with NATS KV backend
# ---------------------------------------------------------------------------

def bench_chord_kv(count: int, server: str = "localhost", port: int = 4222) -> ScenarioResult:
    """chord(group of add tasks, callback) with the NATS KV result backend."""
    from celery import Celery as _Celery, chord as celery_chord, group as celery_group
    from nats.js.api import StorageType

    kv_app = _Celery(
        "nats_bench_chord_kv",
        broker=f"nats://{server}:{port}",
        backend=f"nats+kv://{server}:{port}",
    )
    kv_app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        broker_transport_options={
            "stream_config": {"storage": StorageType.MEMORY},
            "wait_time_seconds": 0.1,
        },
        result_backend_transport_options={
            "nats_kv_bucket": "chord_results",
            "storage": StorageType.MEMORY,
        },
    )

    @kv_app.task(name="bench.chord_add")
    def _add(x, y):
        return x + y

    @kv_app.task(name="bench.chord_sum")
    def _sum(results):
        return sum(results)

    latencies: list[float] = []
    t0 = time.perf_counter()
    # Run *count* independent single-chord calls to measure end-to-end latency.
    for i in range(count):
        t_chord = time.perf_counter()
        result = celery_chord(
            celery_group(_add.s(j, j) for j in range(5)), _sum.s()
        ).apply_async()
        result.get(timeout=30)
        latencies.append(time.perf_counter() - t_chord)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(
        name="kv_backend/chord",
        count=count,
        elapsed_s=elapsed,
        latency_samples=latencies,
        extra={"chord_size": 5},
    )


# ---------------------------------------------------------------------------
# Scenario 7: Hybrid backend — large-payload routing (>900 KB)
# ---------------------------------------------------------------------------

_HYBRID_SMALL_SIZE = 100          # bytes — should stay in KV
_HYBRID_LARGE_SIZE = 950_000      # bytes — should overflow to Object Store


def bench_hybrid_backend(
    count: int, payload_size: int, server: str = "localhost", port: int = 4222
) -> ScenarioResult:
    """Store large blobs via nats+hybrid backend.

    At *payload_size* > 900 KB the backend transparently stores the result in
    NATS Object Store and stores a pointer in the KV bucket.  This measures the
    round-trip overhead for hybrid routing.
    """
    from celery import Celery as _Celery, group as celery_group
    from nats.js.api import StorageType

    hybrid_app = _Celery(
        "nats_bench_hybrid",
        broker=f"nats://{server}:{port}",
        backend=f"nats+hybrid://{server}:{port}",
    )
    hybrid_app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        broker_transport_options={
            "stream_config": {"storage": StorageType.MEMORY},
            "wait_time_seconds": 0.1,
        },
        result_backend_transport_options={
            "nats_kv_bucket": "hybrid_results",
            "nats_object_bucket": "hybrid_objects",
            "storage": StorageType.MEMORY,
            "nats_hybrid_threshold": 900_000,
        },
    )

    @hybrid_app.task(name="bench.hybrid_echo")
    def _echo(size: int) -> str:
        return "x" * size

    size_label = f"{payload_size // 1024}KB" if payload_size >= 1024 else f"{payload_size}B"
    tasks = celery_group(_echo.s(payload_size) for _ in range(count))
    t0 = time.perf_counter()
    result = tasks.apply_async()
    result.get(timeout=120)
    elapsed = time.perf_counter() - t0
    return ScenarioResult(
        name=f"hybrid_backend/{size_label}",
        count=count,
        elapsed_s=elapsed,
        extra={"payload_bytes": payload_size, "routed_to_obs": payload_size >= 900_000},
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

COL = 30


def _row(label: str, value: str) -> None:
    print(f"    {label:<{COL}}{value}")


def _lat_str(r: ScenarioResult) -> str:
    if not r.latency_samples:
        return "n/a"
    return (
        f"mean={r.lat_mean_ms:.1f} ms  "
        f"p50={r.lat_p50_ms:.1f} ms  "
        f"p95={r.lat_p95_ms:.1f} ms  "
        f"min={r.lat_min_ms:.1f} ms"
    )


def print_report(results: list[ScenarioResult]) -> None:
    divider = "─" * 74
    print()
    print("╔" + "═" * 72 + "╗")
    print("║{:^72}║".format("Celery NATS Benchmark  —  kombu + nats-py"))
    print("╚" + "═" * 72 + "╝")
    print(f"\n  {divider}")
    for r in results:
        print(f"\n  [{r.name}]  ({r.count} tasks)")
        _row("Throughput:", f"{r.throughput:>10,.1f} tasks/s   ({r.elapsed_s:.3f} s total)")
        if r.latency_samples:
            _row("Latency:", _lat_str(r))
        for k, v in r.extra.items():
            _row(f"{k}:", str(v))
    print(f"\n  {divider}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Celery NATS benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--server", default="localhost", metavar="HOST",
                        help="NATS server hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=4222,
                        help="NATS server port (default: 4222)")
    parser.add_argument("--count", type=int, default=100, metavar="N",
                        help="Tasks per scenario (default: 100)")
    parser.add_argument("--chain-count", type=int, default=None, metavar="N",
                        help="Chains for chain scenario (default: --count)")
    parser.add_argument("--retry-count", type=int, default=None, metavar="N",
                        help="Tasks for retry scenario (default: --count // 5)")
    parser.add_argument("--no-chain", action="store_true", help="Skip chain scenario")
    parser.add_argument("--no-fanout", action="store_true", help="Skip fan-out scenario")
    parser.add_argument("--no-retry", action="store_true", help="Skip retry scenario")
    parser.add_argument("--no-kv", action="store_true", help="Skip NATS KV backend scenarios")
    parser.add_argument(
        "--fanout-sizes", nargs="+", choices=list(FANOUT_PAYLOAD_SIZES),
        default=list(FANOUT_PAYLOAD_SIZES),
        help="Payload sizes for fan-out (default: all)",
    )
    args = parser.parse_args()

    # Apply server/port to the Celery app config
    app.conf.broker_url = f"nats://{args.server}:{args.port}"

    chain_count = args.chain_count or args.count
    retry_count = args.retry_count or max(1, args.count // 5)

    print(f"\n  Server  : nats://{args.server}:{args.port}")
    print(f"  Count   : {args.count}  chain={chain_count}  retry={retry_count}")
    print(f"  Fan-out sizes: {', '.join(args.fanout_sizes)}")
    kv_label = "skip" if args.no_kv else "enabled"
    print(f"  KV backend  : {kv_label}")
    print("\n  Starting worker ...", end=" ", flush=True)

    worker_proc, _ready = start_worker()
    print("ready\n")

    results: list[ScenarioResult] = []

    try:
        # --- 1. Throughput ---
        print(f"  [throughput]  {args.count} tasks ...", end=" ", flush=True)
        r = bench_throughput(args.count)
        results.append(r)
        print(f"{r.throughput:,.1f} tasks/s")

        # --- 2. Chain ---
        if not args.no_chain:
            print(f"  [chain]       {chain_count} chains (3 steps each) ...", end=" ", flush=True)
            r = bench_chain(chain_count)
            results.append(r)
            print(f"{r.throughput:,.1f} chains/s  lat(mean)={r.lat_mean_ms:.1f} ms")

        # --- 3. Fan-out ---
        if not args.no_fanout:
            for size_name in args.fanout_sizes:
                print(f"  [fanout/{size_name}]  {args.count} tasks ...", end=" ", flush=True)
                r = bench_fanout(args.count, size_name)
                results.append(r)
                print(f"{r.throughput:,.1f} tasks/s")

        # --- 4. Retry ---
        if not args.no_retry:
            print(f"  [retry]       {retry_count} tasks (2 retries each) ...", end=" ", flush=True)
            r = bench_retry(retry_count, fail_count=2)
            results.append(r)
            print(f"{r.throughput:,.1f} tasks/s  lat(mean)={r.lat_mean_ms:.1f} ms")

        # --- 5. KV backend throughput ---
        if not args.no_kv:
            kv_count = max(1, args.count // 2)
            print(f"  [kv/throughput] {kv_count} tasks ...", end=" ", flush=True)
            r = bench_kv_backend(kv_count, server=args.server, port=args.port)
            results.append(r)
            print(f"{r.throughput:,.1f} tasks/s")

            # --- 6. KV backend chord ---
            chord_count = max(1, args.count // 10)
            print(f"  [kv/chord]    {chord_count} chords (5 tasks each) ...", end=" ", flush=True)
            r = bench_chord_kv(chord_count, server=args.server, port=args.port)
            results.append(r)
            print(f"{r.throughput:,.1f} chords/s  lat(mean)={r.lat_mean_ms:.1f} ms")

            # --- 7. Hybrid backend: small payload (stays in KV) ---
            small_count = max(1, args.count // 5)
            print(f"  [hybrid/small] {small_count} tasks ({_HYBRID_SMALL_SIZE}B) ...", end=" ", flush=True)
            r = bench_hybrid_backend(small_count, _HYBRID_SMALL_SIZE, server=args.server, port=args.port)
            results.append(r)
            print(f"{r.throughput:,.1f} tasks/s")

            # --- 8. Hybrid backend: large payload (routes to Object Store) ---
            large_count = max(1, args.count // 20)
            print(f"  [hybrid/large] {large_count} tasks ({_HYBRID_LARGE_SIZE // 1024}KB) ...", end=" ", flush=True)
            r = bench_hybrid_backend(large_count, _HYBRID_LARGE_SIZE, server=args.server, port=args.port)
            results.append(r)
            print(f"{r.throughput:,.1f} tasks/s")

    finally:
        worker_proc.terminate()
        worker_proc.join(timeout=10)

    print_report(results)


if __name__ == "__main__":
    main()
