"""Tests for benchlib.results."""
from __future__ import annotations

import json
import threading

import pandas as pd
import pytest

from benchlib.results import ResultDir, read_parquet, write_parquet
from benchlib.run_spec import SMOKE_SPEC


class TestResultDir:
    def test_creates_raw_dir(self, tmp_path):
        rd = ResultDir("test-run-001", tmp_path)
        assert rd.raw_dir.is_dir()

    def test_write_read_run_json(self, tmp_path):
        rd = ResultDir("test-run-001", tmp_path)
        rd.write_run_json(SMOKE_SPEC, extra={"python_version": "3.14.0"})
        data = rd.read_run_json()
        assert data["broker"] == "memory"
        assert data["python_version"] == "3.14.0"

    def test_run_json_is_atomic(self, tmp_path):
        """Verify write goes via a .tmp file (atomic rename)."""
        rd = ResultDir("test-run-002", tmp_path)
        rd.write_run_json(SMOKE_SPEC)
        # .tmp file should be gone after write
        assert not (rd.raw_dir / "run.json.tmp").exists()
        assert (rd.raw_dir / "run.json").exists()

    def test_append_and_read_events(self, tmp_path):
        rd = ResultDir("test-run-003", tmp_path)
        rd.append_event({"task_id": "abc", "point": "T0", "ns": 12345})
        rd.append_event({"task_id": "abc", "point": "T1", "ns": 12346})
        events = rd.read_events()
        assert len(events) == 2
        assert events[0]["point"] == "T0"
        assert events[1]["point"] == "T1"

    def test_read_events_empty(self, tmp_path):
        rd = ResultDir("test-run-004", tmp_path)
        assert rd.read_events() == []

    def test_append_event_thread_safety(self, tmp_path):
        rd = ResultDir("test-run-005", tmp_path)
        barrier = threading.Barrier(10)

        def worker(i):
            barrier.wait()
            rd.append_event({"idx": i})

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        events = rd.read_events()
        assert len(events) == 10
        # All events must be valid JSON (no interleaved writes)
        idxs = sorted(e["idx"] for e in events)
        assert idxs == list(range(10))

    def test_read_events_for_classmethod(self, tmp_path):
        rd = ResultDir("test-run-006", tmp_path)
        rd.append_event({"point": "T0"})
        events = ResultDir.read_events_for("test-run-006", tmp_path)
        assert len(events) == 1

    def test_read_events_for_missing_run(self, tmp_path):
        events = ResultDir.read_events_for("nonexistent", tmp_path)
        assert events == []


class TestParquet:
    def test_write_read_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "run_id": ["r1", "r1"],
            "task_id": ["a", "b"],
            "e2e_ms": [10.5, 20.3],
        })
        write_parquet(df, "r1", tmp_path)
        result = read_parquet("r1", tmp_path)
        assert list(result.columns) == ["run_id", "task_id", "e2e_ms"]
        assert len(result) == 2

    def test_write_parquet_preserves_dtypes(self, tmp_path):
        df = pd.DataFrame({
            "count": pd.array([1, 2, 3], dtype="int64"),
            "latency": pd.array([1.1, 2.2, 3.3], dtype="float64"),
            "label": pd.array(["a", "b", "c"], dtype="object"),
        })
        write_parquet(df, "dtype-test", tmp_path)
        result = read_parquet("dtype-test", tmp_path)
        assert result["count"].dtype == "int64"
        assert result["latency"].dtype == "float64"

    def test_write_parquet_is_atomic(self, tmp_path):
        df = pd.DataFrame({"x": [1]})
        write_parquet(df, "atomic-test", tmp_path)
        assert not (tmp_path / "results" / "processed" / "atomic-test.tmp").exists()
        assert (tmp_path / "results" / "processed" / "atomic-test.parquet").exists()

    def test_write_parquet_returns_path(self, tmp_path):
        df = pd.DataFrame({"x": [1]})
        dest = write_parquet(df, "path-test", tmp_path)
        assert dest.exists()
        assert dest.suffix == ".parquet"
