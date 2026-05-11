"""SLA evaluation for benchmark runs.

Compares a ``ProducerResult`` against configurable thresholds and returns a
``SLAResult`` indicating pass/fail per metric.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from benchlib.producer import ProducerResult
    from benchlib.run_spec import RunSpec


# Default SLA thresholds (milliseconds)
DEFAULT_P50_MS = 100.0
DEFAULT_P95_MS = 500.0
DEFAULT_P99_MS = 1000.0
DEFAULT_MAX_FAILURE_RATE = 0.01  # 1 %
DEFAULT_MIN_THROUGHPUT = 0.0     # tasks/s — 0 means no lower bound


@dataclass
class SLAResult:
    """Outcome of a single SLA evaluation."""

    passed: bool
    violations: list[str] = field(default_factory=list)

    # Individual metric outcomes
    p50_ok: bool = True
    p95_ok: bool = True
    p99_ok: bool = True
    failure_rate_ok: bool = True
    throughput_ok: bool = True

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        if self.violations:
            return f"{status}: " + "; ".join(self.violations)
        return status


def evaluate_sla(
    result: "ProducerResult",
    spec: "RunSpec",
    *,
    p50_ms: float = DEFAULT_P50_MS,
    p95_ms: float = DEFAULT_P95_MS,
    p99_ms: float = DEFAULT_P99_MS,
    max_failure_rate: float = DEFAULT_MAX_FAILURE_RATE,
    min_throughput: float = DEFAULT_MIN_THROUGHPUT,
) -> SLAResult:
    """Evaluate a :class:`~benchlib.producer.ProducerResult` against SLA thresholds.

    Parameters
    ----------
    result:
        The ``ProducerResult`` from a completed benchmark run.
    spec:
        The ``RunSpec`` that produced the result (reserved for future per-spec
        threshold overrides).
    p50_ms, p95_ms, p99_ms:
        Latency thresholds in milliseconds.
    max_failure_rate:
        Maximum allowed fraction of failed tasks (0.01 == 1 %).
    min_throughput:
        Minimum required tasks/second; 0 disables the check.
    """
    violations: list[str] = []
    sla = SLAResult(passed=True)

    # Latency checks
    if result.p50_e2e_ms > p50_ms:
        sla.p50_ok = False
        violations.append(
            f"p50 {result.p50_e2e_ms:.1f}ms > {p50_ms:.1f}ms"
        )

    if result.p95_e2e_ms > p95_ms:
        sla.p95_ok = False
        violations.append(
            f"p95 {result.p95_e2e_ms:.1f}ms > {p95_ms:.1f}ms"
        )

    if result.p99_e2e_ms > p99_ms:
        sla.p99_ok = False
        violations.append(
            f"p99 {result.p99_e2e_ms:.1f}ms > {p99_ms:.1f}ms"
        )

    # Failure-rate check
    if result.submitted > 0:
        failure_rate = result.failed / result.submitted
        if failure_rate > max_failure_rate:
            sla.failure_rate_ok = False
            violations.append(
                f"failure_rate {failure_rate:.2%} > {max_failure_rate:.2%}"
            )

    # Throughput check
    if min_throughput > 0 and result.duration_s > 0:
        throughput = result.succeeded / result.duration_s
        if throughput < min_throughput:
            sla.throughput_ok = False
            violations.append(
                f"throughput {throughput:.1f} tasks/s < {min_throughput:.1f} tasks/s"
            )

    sla.violations = violations
    sla.passed = len(violations) == 0
    return sla
