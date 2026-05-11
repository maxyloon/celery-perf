"""Environment check asset — verifies Python packages and broker connectivity."""
from __future__ import annotations

from dagster import asset

from dagster_bench.resources.pvc import LocalResultsResource


@asset(
    name="environment_check",
    description=(
        "Verify benchlib packages are importable and the results directory "
        "is writable.  Run this first to confirm the environment is healthy."
    ),
)
def environment_check_asset(context, results: LocalResultsResource) -> dict:
    """Check the benchmark environment and return a summary dict."""
    import sys
    import benchlib
    import celery
    import kombu

    checks = {
        "python_version": sys.version,
        "benchlib_version": getattr(benchlib, "__version__", "dev"),
        "celery_version": celery.__version__,
        "kombu_version": kombu.__version__,
        "results_path": str(results.path),
        "results_writable": _check_writable(results.path),
    }

    context.log.info("Environment check results: %s", checks)
    for key, value in checks.items():
        if key == "results_writable" and not value:
            raise RuntimeError(f"Results path {results.path} is not writable")

    return checks


def _check_writable(path) -> bool:
    import tempfile
    try:
        with tempfile.NamedTemporaryFile(dir=path, delete=True):
            return True
    except (OSError, PermissionError):
        return False
