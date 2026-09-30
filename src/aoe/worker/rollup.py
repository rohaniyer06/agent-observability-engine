"""Latency rollups: Redis histograms in, `latency_rollups` rows out.

This is the design doc §4.2 blocker fix. `percentile_cont` over raw span rows is
fine at ten thousand spans and falls over at a million — which is precisely the
moment the load test is trying to produce the project's headline numbers. The
dashboard therefore never reads `spans` for the live view; it reads pre-aggregated
rows written here.

Why a Redis histogram instead of the doc's in-worker t-digest (DEVIATIONS.md #1):
the worker is a pool. A digest in worker A's memory cannot be merged with worker
B's, and `latency_rollups` is keyed (node_name, bucket_start), so two workers
flushing the same minute would fight over one row and silently drop half the
data. Bucket assignment is a pure function of the value, so HINCRBY merges every
worker's observations into one histogram for free, and the flusher then reads
something already correct.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aoe import redis_keys
from aoe.histogram import bucket_index, bucket_upper_bound, percentiles
from aoe.worker import sleep_or_stop

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncio

    import asyncpg
    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

    from aoe.config import Settings
    from aoe.schema import Span

log = logging.getLogger("aoe.worker.rollup")

_QUANTILES = [0.5, 0.95, 0.99]

# Redis has no "set hash field to the max of itself and X". Read-modify-write
# from the worker would lose the larger value under concurrency, and max_us is
# the one column a percentile histogram genuinely cannot reconstruct, so it gets
# four lines of Lua to make the compare-and-set atomic.
_HASH_MAX_LUA = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current == false or tonumber(ARGV[2]) > tonumber(current) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
end
return 1
"""

_UPSERT_ROLLUP = """
INSERT INTO latency_rollups (
    node_name, bucket_start, p50_us, p95_us, p99_us, max_us, count, error_count, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
ON CONFLICT (node_name, bucket_start) DO UPDATE
SET p50_us      = EXCLUDED.p50_us,
    p95_us      = EXCLUDED.p95_us,
    p99_us      = EXCLUDED.p99_us,
    max_us      = EXCLUDED.max_us,
    count       = EXCLUDED.count,
    error_count = EXCLUDED.error_count,
    updated_at  = now()
"""


def bucket_start_epoch(epoch_ms: int, bucket_seconds: int) -> int:
    """Floor an epoch-ms timestamp to its bucket start, returned as epoch SECONDS."""
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be positive")
    return (epoch_ms // 1000 // bucket_seconds) * bucket_seconds


def dirty_member(node_name: str, bucket_epoch_s: int) -> str:
    return f"{node_name}|{bucket_epoch_s}"


def parse_dirty_member(member: str) -> tuple[str, int] | None:
    node_name, sep, bucket = member.rpartition("|")
    if not sep or not node_name or not bucket.isdigit():
        return None
    return node_name, int(bucket)


def stage_latency(pipe: Pipeline, spans: list[Span], settings: Settings) -> None:
    """Queue histogram updates for a batch onto an existing Redis pipeline.

    Spans are bucketed by when they *ended*, not by when the worker got to them,
    so a backlog being drained attributes its latency to the minute it happened
    in rather than smearing it across the recovery window.
    """
    bucket_seconds = settings.rollup_bucket_seconds
    ttl_s = settings.rollup_key_ttl_s

    # One EXPIRE / SADD / max-CAS per (node, bucket) instead of per span: a
    # 500-span batch usually touches four nodes and one minute.
    touched: dict[tuple[str, int], int] = {}

    for span in spans:
        bucket = bucket_start_epoch(span.end_time_ns // 1_000_000, bucket_seconds)
        hist_key = redis_keys.latency_histogram(span.node_name, bucket)
        meta_key = redis_keys.latency_meta(span.node_name, bucket)

        pipe.hincrby(hist_key, str(bucket_index(span.duration_us)), 1)
        pipe.hincrby(meta_key, "count", 1)
        if span.status == "error":
            pipe.hincrby(meta_key, "error_count", 1)

        key = (span.node_name, bucket)
        if span.duration_us > touched.get(key, -1):
            touched[key] = span.duration_us

    for (node_name, bucket), max_us in touched.items():
        hist_key = redis_keys.latency_histogram(node_name, bucket)
        meta_key = redis_keys.latency_meta(node_name, bucket)
        pipe.eval(_HASH_MAX_LUA, 1, meta_key, "max_us", str(max_us))
        pipe.expire(hist_key, ttl_s)
        pipe.expire(meta_key, ttl_s)
        pipe.sadd(redis_keys.HISTOGRAM_DIRTY, dirty_member(node_name, bucket))


async def flush_rollups(redis: Redis, pool: asyncpg.Pool, settings: Settings) -> int:
    """Read every dirty bucket, compute percentiles, upsert. Returns rows written.

    Buckets stay dirty while they are still open so the current minute keeps
    getting refreshed — the dashboard shows a live-updating latest point rather
    than a hole until the minute closes. A bucket is dropped from the dirty set
    only after its final numbers have been written.

    Every worker process runs this loop. That is safe rather than racy: all of
    them read the same already-merged Redis histogram, so concurrent upserts
    write identical values into the row.
    """
    members = await redis.smembers(redis_keys.HISTOGRAM_DIRTY)
    if not members:
        return 0

    parsed: list[tuple[str, str, int]] = []
    unparseable: list[str] = []
    for member in members:
        parts = parse_dirty_member(member)
        if parts is None:
            unparseable.append(member)
            continue
        parsed.append((member, parts[0], parts[1]))
    if unparseable:
        await redis.srem(redis_keys.HISTOGRAM_DIRTY, *unparseable)
    if not parsed:
        return 0

    pipe = redis.pipeline(transaction=False)
    for _, node_name, bucket in parsed:
        pipe.hgetall(redis_keys.latency_histogram(node_name, bucket))
        pipe.hgetall(redis_keys.latency_meta(node_name, bucket))
    results = await pipe.execute()

    # A bucket is settled once it has been closed for longer than one flush
    # interval, i.e. no in-flight batch can still be adding to it.
    settle_s = max(5.0, 2 * settings.rollup_flush_interval_ms / 1000.0)
    now_s = time.time()

    rows: list[tuple] = []
    done: list[str] = []

    for i, (member, node_name, bucket) in enumerate(parsed):
        raw_hist: dict[str, str] = results[2 * i]
        raw_meta: dict[str, str] = results[2 * i + 1]

        if not raw_hist:
            # The histogram TTL'd away (rollup_key_ttl_s is many multiples of the
            # bucket width, so this only happens to buckets long since written).
            done.append(member)
            continue

        counts = {int(k): int(v) for k, v in raw_hist.items()}
        quantiles = percentiles(counts, _QUANTILES)
        count = int(raw_meta.get("count") or sum(counts.values()))
        error_count = int(raw_meta.get("error_count") or 0)
        # Fall back to the top occupied bucket's ceiling if the max CAS never ran.
        max_us = int(raw_meta.get("max_us") or bucket_upper_bound(max(counts)))

        rows.append(
            (
                node_name,
                datetime.fromtimestamp(bucket, tz=UTC),
                quantiles[0.5],
                quantiles[0.95],
                quantiles[0.99],
                max_us,
                count,
                error_count,
            )
        )
        if bucket + settings.rollup_bucket_seconds + settle_s < now_s:
            done.append(member)

    if rows:
        async with pool.acquire() as conn:
            await conn.executemany(_UPSERT_ROLLUP, rows)

    # Only after the write: a crash between the two leaves the bucket dirty and
    # the next flush redoes it, which the upsert makes free.
    if done:
        await redis.srem(redis_keys.HISTOGRAM_DIRTY, *done)

    return len(rows)


async def run_rollup_flusher(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    interval_s = settings.rollup_flush_interval_ms / 1000.0
    while not stop.is_set():
        if await sleep_or_stop(stop, interval_s):
            break
        try:
            written = await flush_rollups(redis, pool, settings)
            if written:
                log.debug("rollup flush", extra={"fields": {"buckets": written}})
        except Exception:
            # A rollup is a derived view; losing one flush costs a refresh, not
            # data, because the histogram stays in Redis and the bucket stays
            # dirty until it is successfully written.
            log.exception("rollup flush failed")
            if await sleep_or_stop(stop, 1.0):
                break
