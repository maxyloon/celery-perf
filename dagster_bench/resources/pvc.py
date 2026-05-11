"""PVC / local results storage resource for Dagster."""
from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableResource


class LocalResultsResource(ConfigurableResource):
    """Wraps the results base directory for benchmark runs.

    In local dev this resolves to ``<repo_root>/results/``.
    In Kubernetes it resolves to the PVC mount path (``/results``).
    """

    base_path: str = os.environ.get(
        "BENCH_RESULTS_PATH",
        str(Path(__file__).parent.parent.parent / "results"),
    )

    @property
    def path(self) -> Path:
        p = Path(self.base_path)
        p.mkdir(parents=True, exist_ok=True)
        return p
