"""Worker process entrypoint.

One process runs four kinds of task concurrently:

    consumer   xN   drain the stream, persist spans      (--concurrency)
    reclaimer  x1   re-drive entries stranded in the PEL
    finalizer  x1   close traces whose deadline has passed
    flusher    x1   merge latency histograms into rollups

Only the consumer is scaled inside the process. The other three are
singletons per process because they are coordination points, not throughput
bottlenecks — and running several of them in one process would just add
contention on the same Redis keys for no gain. Horizontal scale is more
processes; the histogram design (DEVIATIONS.md #1) merges correctly across all
of them by construction.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import socket

from aoe import redis_keys
from aoe.config import get_settings
from aoe.db.pool import close_pool, get_pool
from aoe.logging import setup_logging
from aoe.redis_client import close_redis, ensure_consumer_group, get_redis
from aoe.worker import STAT_FIELDS
from aoe.worker.consumer import run_consumer, run_reclaimer
from aoe.worker.finalizer import run_finalizer
from aoe.worker.rollup import run_rollup_flusher


def _consumer_name(index: int) -> str:
    """Stable per-task identity.

    The PEL is keyed by consumer name, so this has to be stable within a process
    lifetime (a restarted worker's stranded entries are picked up by the
    reclaimer, not by name-matching) and unique across processes, or two workers
    would share a pending list and each other's in-flight work.
    """
    return f"{socket.gethostname()}-{os.getpid()}-{index}"


async def run_worker(concurrency: int) -> None:
    settings = get_settings()
    log = setup_logging(settings.log_level, "worker")

    redis = await get_redis()
    pool = await get_pool()
    await ensure_consumer_group(
        redis, redis_keys.STREAM_SPANS, settings.worker_consumer_group
    )

    # Seed counters so /v1/system/stats reports zeros on a fresh worker rather
    # than omitting fields the dashboard then renders as blank.
    await redis.hsetnx(redis_keys.WORKER_STATS, "spans_ingested", 0)
    for field in STAT_FIELDS:
        await redis.hsetnx(redis_keys.WORKER_STATS, field, 0)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    tasks: list[asyncio.Task] = [
        asyncio.create_task(
            run_consumer(redis, pool, settings, stop, _consumer_name(i)),
            name=f"consumer-{i}",
        )
        for i in range(concurrency)
    ]
    tasks += [
        asyncio.create_task(
            run_reclaimer(redis, pool, settings, stop, _consumer_name(0)),
            name="reclaimer",
        ),
        asyncio.create_task(run_finalizer(redis, pool, settings, stop), name="finalizer"),
        asyncio.create_task(
            run_rollup_flusher(redis, pool, settings, stop), name="rollup-flusher"
        ),
    ]

    log.info(
        "worker started",
        extra={
            "fields": {
                "concurrency": concurrency,
                "group": settings.worker_consumer_group,
                "stream": redis_keys.STREAM_SPANS,
            }
        },
    )

    try:
        # If any task dies unexpectedly, stop the rest rather than limping along
        # with, say, no finalizer — a worker that ingests but never closes traces
        # looks healthy and produces nothing.
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                log.error(
                    "worker task failed",
                    extra={"fields": {"task": task.get_name()}},
                    exc_info=task.exception(),
                )
        stop.set()
        if pending:
            # Give in-flight batches a chance to finish and ACK before teardown;
            # anything still running after the grace period gets cancelled.
            _, still_running = await asyncio.wait(pending, timeout=10)
            for task in still_running:
                task.cancel()
            await asyncio.gather(*still_running, return_exceptions=True)
    finally:
        log.info("worker stopping")
        await close_pool()
        await close_redis()


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent observability telemetry worker")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="number of consumer tasks in this process (default: 2)",
    )
    args = parser.parse_args()
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1")

    try:
        asyncio.run(run_worker(args.concurrency))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
