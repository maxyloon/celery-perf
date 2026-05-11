"""RunSpec — immutable benchmark case descriptor.

Each RunSpec uniquely identifies one benchmark configuration.  The
``partition_key`` property produces a deterministic, URL-safe identifier
suitable for use as a Dagster partition key or a result directory name.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import datetime, timezone


@dataclasses.dataclass(frozen=True)
class RunSpec:
    # ------------------------------------------------------------------ #
    # What to test                                                         #
    # ------------------------------------------------------------------ #
    broker: str
    """Transport / broker name.

    Valid values: ``"rabbitmq"``, ``"redis"``, ``"nats_js"``,
    ``"nats_core"``, ``"kafka"``, ``"zookeeper"``, ``"mongodb"``,
    ``"sqlalchemy"``, ``"filesystem"``, ``"memory"``.
    """

    broker_profile: str
    """Resource / topology profile for the broker pod.

    Valid values: ``"single_small"``, ``"single_medium"``,
    ``"clustered_medium"``.
    """

    backend: str
    """Result backend name.

    Valid values: ``"redis"``, ``"postgres"``, ``"mysql"``, ``"mongodb"``,
    ``"memcached"``, ``"elasticsearch"``, ``"nats_kv"``, ``"rpc"``,
    ``"none"`` (``ignore_result=True``).
    """

    pool: str
    """Celery worker concurrency pool.

    Valid values: ``"prefork"``, ``"gevent"``, ``"eventlet"``,
    ``"threads"``, ``"solo"``.
    """

    task: str
    """Task name from the benchmark suite.

    Valid values: ``"noop"``, ``"echo"``, ``"sleep"``, ``"cpu"``,
    ``"fanout"``, ``"chain"``, ``"retry_once"``, ``"fail"``.
    """

    payload_bytes: int
    """Approximate payload size in bytes (0 for noop)."""

    # ------------------------------------------------------------------ #
    # How to load it                                                       #
    # ------------------------------------------------------------------ #
    mode: str
    """Load generation mode.

    Valid values: ``"burst"``, ``"open_loop"``, ``"closed_loop"``,
    ``"soak"``.
    """

    replicas: int
    """Number of Celery worker pod replicas."""

    concurrency: int
    """Per-worker concurrency (``--concurrency``)."""

    prefetch: int
    """``worker_prefetch_multiplier`` setting."""

    resource_class: str
    """Worker pod resource class: ``"small"``, ``"medium"``, ``"large"``."""

    # ------------------------------------------------------------------ #
    # Celery settings                                                      #
    # ------------------------------------------------------------------ #
    acks_late: bool = False
    serializer: str = "json"

    # ------------------------------------------------------------------ #
    # Properties                                                           #
    # ------------------------------------------------------------------ #

    @property
    def partition_key(self) -> str:
        """Deterministic, URL-safe key for Dagster partition identity.

        Derived from a SHA-256 digest of the canonical JSON representation
        of all fields.  Truncated to 16 hex characters.
        """
        canonical = json.dumps(dataclasses.asdict(self), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    @property
    def run_id(self) -> str:
        """Human-readable run identifier: ``YYYYMMDDTHHMMSSZ-{hash[:8]}``."""
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{ts}-{self.partition_key[:8]}"

    # ------------------------------------------------------------------ #
    # Serialisation helpers                                                #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        """Return a plain dict representation (all fields)."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RunSpec":
        """Construct a RunSpec from a plain dict (e.g. loaded from JSON)."""
        return cls(**d)


# ---------------------------------------------------------------------------
# Convenience constants
# ---------------------------------------------------------------------------

#: Minimal RunSpec for the smoke harness — uses the in-process ``memory://``
#: transport so no broker infrastructure is needed.
SMOKE_SPEC = RunSpec(
    broker="memory",
    broker_profile="single_small",
    backend="none",
    pool="solo",
    task="noop",
    payload_bytes=0,
    mode="burst",
    replicas=1,
    concurrency=1,
    prefetch=1,
    resource_class="small",
)
