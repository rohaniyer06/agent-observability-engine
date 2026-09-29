"""Shared async Redis client factory."""

from __future__ import annotations

import asyncio

import redis.asyncio as aioredis

_client: aioredis.Redis | None = None
_lock = asyncio.Lock()


def make_client(url: str, decode_responses: bool = True) -> aioredis.Redis:
    return aioredis.from_url(
        url,
        decode_responses=decode_responses,
        health_check_interval=15,
        socket_keepalive=True,
    )


async def get_redis() -> aioredis.Redis:
    global _client
    if _client is not None:
        return _client
    async with _lock:
        if _client is None:
            from aoe.config import get_settings

            _client = make_client(get_settings().redis_url)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def ensure_consumer_group(redis: aioredis.Redis, stream: str, group: str) -> None:
    """Idempotently create the stream + consumer group.

    MKSTREAM so the group can be created before any producer has run — otherwise
    a worker started first would crash on an empty system.
    """
    try:
        await redis.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
    except aioredis.ResponseError as exc:  # pragma: no cover - depends on server state
        if "BUSYGROUP" not in str(exc):
            raise


async def consumer_group_backlog(redis: aioredis.Redis, stream: str, group: str) -> int:
    """Entries the consumer group has not finished with yet.

    NOT `XLEN`. A stream is an append-only log: acknowledging an entry does not
    remove it, so `XLEN` counts everything ever written (until MAXLEN trimming
    reclaims it) and never falls as the workers catch up. Using it as a backlog
    signal is wrong in both directions — it reports a huge backlog on a perfectly
    drained system, and it would have ingestion shed load with 503 purely because
    the service has been up long enough to have seen that many spans.

    The real backlog is the group's `lag` (entries added but not yet delivered)
    plus its `pending` (delivered but not yet acked).
    """
    try:
        groups = await redis.xinfo_groups(stream)
    except aioredis.ResponseError:
        # Stream or group does not exist yet — nothing has been ingested.
        return 0

    for info in groups:
        if info.get("name") != group:
            continue
        pending = int(info.get("pending") or 0)
        lag = info.get("lag")
        if lag is None:
            # Redis reports a null lag when trimming has made the count
            # unreconcilable. Pending is then the only figure we can still
            # trust; better a conservative under-report than a fabricated one.
            return pending
        return int(lag) + pending
    return 0
