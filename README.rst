============
celery-perf
============

Performance benchmarks and profiling tools for `Celery <https://docs.celeryq.dev/>`_.

Benchmarks
==========

NATS JetStream transport
------------------------

Measures broker/backend throughput and latency using a NATS server with JetStream enabled.

**Prerequisites**::

    pip install -e ".[nats]"
    nats-server -js          # NATS server with JetStream

**Run**::

    python -m benchmarks.nats.benchmark

    # Options
    python -m benchmarks.nats.benchmark --count 200 --server localhost --no-kv

**Scenarios**

* **throughput** — group of ``add()`` tasks, measures tasks/s end-to-end
* **chain** — ``chain(add → add → add) × N``, measures per-chain latency
* **fanout/small|medium|large** — ``group`` of ``process_payload()`` at three payload sizes
* **retry** — ``flaky()`` task with 2 forced retries, measures retry overhead
* **kv_backend/throughput** — same as throughput but using ``nats+kv://`` result backend
* **kv_backend/chord** — ``chord(group → sum)`` with KV backend
* **hybrid_backend/small|large** — result routing through ``nats+hybrid://`` backend
