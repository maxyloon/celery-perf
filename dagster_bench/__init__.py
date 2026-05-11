"""Dagster workspace + Definitions for celery-perf.

This module is the Dagster entry-point referenced in ``workspace.yaml``.
It exports a single ``Definitions`` object that includes all assets, jobs,
and resources for the benchmark platform.

Usage (local dev)::

    dagster dev -m dagster_bench

Smoke job::

    dagster job execute -m dagster_bench -j smoke_job
"""
from __future__ import annotations

from dagster import Definitions

from dagster_bench.resources.pvc import LocalResultsResource
from dagster_bench.assets.environment import environment_check_asset
from dagster_bench.jobs.smoke import smoke_job

defs = Definitions(
    assets=[environment_check_asset],
    jobs=[smoke_job],
    resources={
        "results": LocalResultsResource(),
    },
)
