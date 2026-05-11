"""Tests for benchlib.run_spec."""
from __future__ import annotations

import json

import pytest

from benchlib.run_spec import RunSpec, SMOKE_SPEC


class TestRunSpec:
    def test_partition_key_is_deterministic(self):
        spec1 = RunSpec(
            broker="rabbitmq", broker_profile="single_medium", backend="redis",
            pool="prefork", task="echo", payload_bytes=1024, mode="open_loop",
            replicas=4, concurrency=8, prefetch=1, resource_class="medium",
        )
        spec2 = RunSpec(
            broker="rabbitmq", broker_profile="single_medium", backend="redis",
            pool="prefork", task="echo", payload_bytes=1024, mode="open_loop",
            replicas=4, concurrency=8, prefetch=1, resource_class="medium",
        )
        assert spec1.partition_key == spec2.partition_key

    def test_partition_key_changes_when_field_changes(self):
        base = SMOKE_SPEC
        other = RunSpec(**{**base.to_dict(), "broker": "redis"})
        assert base.partition_key != other.partition_key

    def test_partition_key_length(self):
        assert len(SMOKE_SPEC.partition_key) == 16

    def test_run_id_format(self):
        run_id = SMOKE_SPEC.run_id
        # Format: YYYYMMDDTHHMMSSZ-<8 hex chars>
        parts = run_id.split("-")
        assert len(parts) == 2
        ts, h = parts
        assert len(ts) == 16  # YYYYMMDDTHHMMSSz
        assert len(h) == 8

    def test_round_trip(self):
        assert RunSpec.from_dict(SMOKE_SPEC.to_dict()) == SMOKE_SPEC

    def test_round_trip_full_spec(self):
        spec = RunSpec(
            broker="nats_js", broker_profile="single_medium", backend="nats_kv",
            pool="gevent", task="fanout", payload_bytes=10240, mode="soak",
            replicas=8, concurrency=100, prefetch=4, resource_class="large",
            acks_late=True, serializer="msgpack",
        )
        assert RunSpec.from_dict(spec.to_dict()) == spec

    def test_to_dict_is_json_serialisable(self):
        d = SMOKE_SPEC.to_dict()
        # Should not raise
        json.dumps(d)

    def test_smoke_spec_defaults(self):
        assert SMOKE_SPEC.broker == "memory"
        assert SMOKE_SPEC.backend == "none"
        assert SMOKE_SPEC.mode == "burst"
        assert SMOKE_SPEC.acks_late is False
        assert SMOKE_SPEC.serializer == "json"

    def test_frozen(self):
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError, TypeError)):
            SMOKE_SPEC.broker = "redis"  # type: ignore[misc]


import dataclasses  # noqa: E402 (needed for the test above)
