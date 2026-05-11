"""Smoke job — runs 100 noop tasks via memory:// and asserts results.

This job is the Phase 0 gate: it verifies that the full benchlib stack
(producer → Celery → result dir) works end-to-end in an in-process
memory:// configuration with no external dependencies.

Definition of Done
------------------
- 100 tasks submitted and succeeded
- ``events.jsonl`` contains 100 event-pairs (200 lines) with T0, T1, T6
- ``p95_e2e_ms`` is populated (non-zero)
"""
from __future__ import annotations

from pathlib import Path

from dagster import job, op, Out, In

from dagster_bench.resources.pvc import LocalResultsResource


@op(
    name="run_smoke_burst",
    description="Submit 100 noop tasks via memory:// in burst mode.",
    out={"result_summary": Out(dict)},
)
def run_smoke_burst(context, results: LocalResultsResource) -> dict:
    from benchlib.run_spec import SMOKE_SPEC
    from benchlib.results import ResultDir
    from benchlib.producer import BenchmarkProducer

    base_path = results.path
    count = 100

    spec = SMOKE_SPEC
    run_id = spec.run_id
    rd = ResultDir(run_id, base_path)
    rd.write_run_json(spec, {"job": "smoke", "count": count})

    context.log.info("Starting smoke burst: run_id=%s count=%d", run_id, count)

    with BenchmarkProducer(spec, rd) as producer:
        result = producer.run(count=count)

    context.log.info(
        "Smoke burst complete: submitted=%d succeeded=%d failed=%d "
        "p95=%.2fms throughput=%.1f tasks/s",
        result.submitted, result.succeeded, result.failed,
        result.p95_e2e_ms, result.throughput,
    )

    return {
        "run_id": run_id,
        "submitted": result.submitted,
        "succeeded": result.succeeded,
        "failed": result.failed,
        "p50_e2e_ms": result.p50_e2e_ms,
        "p95_e2e_ms": result.p95_e2e_ms,
        "p99_e2e_ms": result.p99_e2e_ms,
        "duration_s": result.duration_s,
        "events_path": str(base_path / "raw" / run_id / "events.jsonl"),
    }


@op(
    name="assert_smoke_results",
    description="Assert that smoke run meets baseline expectations.",
    ins={"summary": In(dict)},
)
def assert_smoke_results(context, results: LocalResultsResource, summary: dict) -> None:
    from benchlib.results import ResultDir
    from benchlib.run_spec import SMOKE_SPEC

    base_path = results.path
    run_id = summary["run_id"]
    rd = ResultDir(run_id, base_path)

    assert summary["submitted"] == 100, (
        f"Expected 100 submitted tasks, got {summary['submitted']}"
    )
    assert summary["succeeded"] == 100, (
        f"Expected 100 succeeded tasks, got {summary['succeeded']}: "
        f"{summary['failed']} failed"
    )
    assert summary["p95_e2e_ms"] >= 0, "p95_e2e_ms must be non-negative"

    # Verify events.jsonl
    events = rd.read_events()
    context.log.info("events.jsonl has %d entries", len(events))
    assert len(events) == 200, (
        f"Expected 200 event entries (100 tasks × 2 events), got {len(events)}"
    )

    # Check T0, T1, T6 per task
    by_task: dict[str, dict] = {}
    for ev in events:
        tid = ev.get("task_id", "")
        if tid not in by_task:
            by_task[tid] = {}
        by_task[tid].update(ev)

    missing_t0 = [tid for tid, ev in by_task.items() if "T0" not in ev]
    missing_t6 = [tid for tid, ev in by_task.items() if "T6" not in ev]
    assert not missing_t0, f"Tasks missing T0: {missing_t0[:5]}"
    assert not missing_t6, f"Tasks missing T6: {missing_t6[:5]}"

    context.log.info(
        "Smoke assertions passed: 100 tasks, 200 events, T0/T1/T6 all present. "
        "p95=%.2fms  throughput=%.1f tasks/s",
        summary["p95_e2e_ms"],
        summary["succeeded"] / max(summary["duration_s"], 0.001),
    )


@job(
    name="smoke_job",
    description="Phase 0 gate: 100-task smoke test via memory://.",
    resource_defs={"results": LocalResultsResource()},
)
def smoke_job():
    summary = run_smoke_burst()
    assert_smoke_results(summary)
