"""Tests for benchlib.workload — tasks run via apply() (no worker needed)."""
from __future__ import annotations

import os

import pytest

# Set env before importing workload so the app is configured correctly
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("CELERY_RESULT_BACKEND", "cache+memory://")

from benchlib.workload import (  # noqa: E402
    app,
    noop,
    echo,
    sleep_task,
    cpu_task,
    fanout_task,
    chain_task,
    retry_once,
    fail_task,
)


# Use always_eager so apply_async/delay execute synchronously, no worker needed.
@pytest.fixture(autouse=True)
def eager_mode():
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    yield
    app.conf.task_always_eager = False
    app.conf.task_eager_propagates = False


class TestNoop:
    def test_noop_executes(self):
        # ignore_result=True — should not raise
        noop.apply()

    def test_noop_has_delay(self):
        assert callable(noop.delay)


class TestEcho:
    def test_echo_string(self):
        result = echo.apply(args=("hello",))
        assert result.get() == "hello"

    def test_echo_none(self):
        result = echo.apply(args=(None,))
        assert result.get() is None

    def test_echo_large_string(self):
        payload = "x" * 10000
        result = echo.apply(args=(payload,))
        assert result.get() == payload


class TestSleepTask:
    def test_sleep_task_completes(self):
        result = sleep_task.apply(args=(1,))  # 1 ms
        assert result.get() is None


class TestCpuTask:
    def test_cpu_task_returns_count(self):
        result = cpu_task.apply(args=(5,))
        count = result.get()
        assert isinstance(count, int)
        assert count >= 0


class TestRetryOnce:
    def test_retry_once_succeeds(self):
        # In eager mode Celery raises Retry rather than re-queuing.
        # Simulate both calls: first raises Retry, second succeeds.
        from celery.exceptions import Retry
        # First call — should raise Retry
        with pytest.raises(Retry):
            retry_once.apply()
        # Second call — simulates second attempt (retries==0 check passes because
        # request.retries is reset per apply() call; just verify success path):
        # We patch the retry count to force the success branch.
        result = retry_once.apply(kwargs={}, retries=1)
        assert result.get() == "ok"


class TestFailTask:
    def test_fail_task_raises(self):
        # Disable propagation so the exception is stored, then retrieve it.
        app.conf.task_eager_propagates = False
        try:
            result = fail_task.apply()
            with pytest.raises(Exception):
                result.get(propagate=True)
        finally:
            app.conf.task_eager_propagates = True


class TestImports:
    def test_all_tasks_importable(self):
        for task in [noop, echo, sleep_task, cpu_task, fanout_task, chain_task, retry_once, fail_task]:
            assert hasattr(task, "delay")
            assert hasattr(task, "apply")

    def test_app_broker_url(self):
        assert "memory" in app.conf.broker_url
