"""Async worker pool — the component the rest of the system is a shell around.

It drains the durable buffer, persists spans, closes traces, and runs the two
detectors. Module map:

    consumer.py   XREADGROUP loop, XAUTOCLAIM reclaim, per-batch orchestration
    writer.py     idempotent bulk span inserts
    finalizer.py  in-flight trace state + deadline-driven finalization
    rollup.py     latency histograms in Redis + periodic percentile flush
    anomaly.py    cost anomaly detection (pure core + IO wrapper)
    drift.py      path drift detection (pure core + IO wrapper)
    main.py       process entrypoint, task supervision, signal handling

Two small runtime helpers live here rather than in a module of their own because
both the consumer side and the finalizer side need them, and importing one leaf
module from the other purely for a four-line helper would make the dependency
graph cyclic.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from aoe import redis_keys

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

# Counters the query API reads at /v1/system/stats. Seeded to zero at startup so
# a freshly-booted worker reports "0 processed" rather than a missing field.
STAT_FIELDS = (
    "spans_processed",
    "spans_duplicate",
    "spans_poison",
    "traces_finalized",
    "traces_finalized_by_timeout",
    "entries_reclaimed",
)


def stage_stats(pipe: Pipeline, **counters: int) -> None:
    """Queue stat increments onto an existing pipeline (no extra round trip)."""
    for field, amount in counters.items():
        if amount:
            pipe.hincrby(redis_keys.WORKER_STATS, field, amount)


async def bump_stats(redis: Redis, **counters: int) -> None:
    pending = {k: v for k, v in counters.items() if v}
    if not pending:
        return
    pipe = redis.pipeline(transaction=False)
    stage_stats(pipe, **pending)
    await pipe.execute()


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Sleep for `seconds`, waking early on shutdown. Returns True if stopping.

    Every periodic loop uses this instead of asyncio.sleep so that SIGTERM does
    not have to wait out a full tick interval.
    """
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True
