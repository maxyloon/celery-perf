"""Result I/O helpers for benchlib.

Manages the on-disk layout for benchmark artefacts:

    results/
    ├── raw/{run_id}/
    │   ├── run.json        — RunSpec + environment metadata (written atomically)
    │   └── events.jsonl    — one JSON line per task event (T0–T6 timestamps)
    └── processed/
        └── {run_id}.parquet — aggregated per-run metrics
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from benchlib.run_spec import RunSpec


class ResultDir:
    """Manages the raw result directory for a single run.

    Creates ``{base_path}/results/raw/{run_id}/`` on construction if it
    does not already exist.
    """

    def __init__(self, run_id: str, base_path: Path | str = ".") -> None:
        self.run_id = run_id
        self.base_path = Path(base_path)
        self.raw_dir = self.base_path / "results" / "raw" / run_id
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # run.json                                                             #
    # ------------------------------------------------------------------ #

    def write_run_json(self, run_spec: "RunSpec", extra: dict | None = None) -> None:
        """Write run metadata atomically.

        Combines ``run_spec.to_dict()`` with ``extra`` (e.g. environment
        versions, k8s info) and writes to ``run.json``.
        """
        payload = {**run_spec.to_dict(), **(extra or {})}
        dest = self.raw_dir / "run.json"
        tmp = dest.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str))
        os.replace(tmp, dest)

    def read_run_json(self) -> dict:
        """Read and return the run metadata dict."""
        return json.loads((self.raw_dir / "run.json").read_text())

    # ------------------------------------------------------------------ #
    # events.jsonl                                                         #
    # ------------------------------------------------------------------ #

    def append_event(self, event: dict) -> None:
        """Append one event dict as a JSON line to ``events.jsonl``.

        Thread-safe — multiple producer / worker threads may call this
        concurrently.
        """
        line = json.dumps(event, default=str) + "\n"
        with self._lock:
            with open(self.raw_dir / "events.jsonl", "a") as f:
                f.write(line)

    def read_events(self) -> list[dict]:
        """Read all events from ``events.jsonl``."""
        path = self.raw_dir / "events.jsonl"
        if not path.exists():
            return []
        events = []
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                events.append(json.loads(line))
        return events

    # ------------------------------------------------------------------ #
    # Class-level helpers                                                  #
    # ------------------------------------------------------------------ #

    @classmethod
    def read_events_for(
        cls, run_id: str, base_path: Path | str = "."
    ) -> list[dict]:
        """Read events for an existing run without constructing a new dir."""
        path = Path(base_path) / "results" / "raw" / run_id / "events.jsonl"
        if not path.exists():
            return []
        events = []
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                events.append(json.loads(line))
        return events


# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------


def write_parquet(
    df: pd.DataFrame,
    run_id: str,
    base_path: Path | str = ".",
) -> Path:
    """Write ``df`` to ``results/processed/{run_id}.parquet`` atomically.

    Returns the final parquet path.
    """
    processed_dir = Path(base_path) / "results" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    dest = processed_dir / f"{run_id}.parquet"
    tmp = dest.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, dest)
    return dest


def read_parquet(run_id: str, base_path: Path | str = ".") -> pd.DataFrame:
    """Read and return the parquet file for a given run."""
    path = Path(base_path) / "results" / "processed" / f"{run_id}.parquet"
    return pd.read_parquet(path)
