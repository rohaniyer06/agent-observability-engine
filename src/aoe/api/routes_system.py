"""`/v1/system/*`, `/v1/loadtests`, `/health` — the self-observability surface.

An observability tool that cannot show you its own buffer depth and consumer lag
is asking to be trusted on faith. These endpoints exist so the demo can answer
"is the worker keeping up?" without an SSH session.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query, Response
from redis.exceptions import RedisError

from aoe import __version__
from aoe.api import queries
from aoe.api.pagination import clamp_limit
from aoe.apimodels import LoadTestRun, PricingResponse, SystemStats
from aoe.config import get_settings
from aoe.db.pool import get_pool
from aoe.logging import log_fields
from aoe.pricing import get_pricing
from aoe.redis_client import consumer_group_backlog, get_redis
from aoe.redis_keys import STREAM_SPANS, TRACE_DEADLINES, WORKER_STATS
from aoe.schema import HealthResponse

router = APIRouter(tags=["system"])
log = logging.getLogger("aoe.api.system")

_COUNTERS = (
    "spans_ingested",
    "spans_processed",
    "spans_duplicate",
    "traces_finalized",
    "traces_finalized_by_timeout",
    "entries_reclaimed",
)


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _unknown_models(raw: str | None) -> list[str]:
    """Worker-reported unknown model names.

    Tolerant of both encodings because this counter crosses a process boundary:
    a JSON array is the intent, a comma-joined string is the obvious thing a
    HSET of a Python set produces by accident. Either way the answer to "which
    models are we costing at $0?" should not be a 500.
    """
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except ValueError:
        return sorted({part.strip() for part in raw.split(",") if part.strip()})
    if isinstance(parsed, list):
        return sorted({str(m) for m in parsed})
    return []


@router.get("/v1/system/stats", response_model=SystemStats)
async def system_stats() -> SystemStats:
    settings = get_settings()
    group = settings.worker_consumer_group

    stream_depth = 0
    pending = 0
    consumer_count = 0
    consumers: list[dict] = []
    awaiting = 0
    raw_stats: dict[str, str] = {}

    try:
        redis = await get_redis()
        # XLEN and ZCARD are O(1) and safe on missing keys (both answer 0).
        stream_depth = await consumer_group_backlog(redis, STREAM_SPANS, settings.worker_consumer_group)
        awaiting = _int(await redis.zcard(TRACE_DEADLINES))
        raw_stats = await redis.hgetall(WORKER_STATS) or {}

        # Everything below depends on the consumer group existing. It does not
        # until a worker has started, and "the worker isn't up yet" is a normal
        # state for this endpoint to report — not a 500. RedisError covers the
        # NOGROUP / no-such-key ResponseErrors this raises on a cold system.
        try:
            summary = await redis.xpending(STREAM_SPANS, group)
            pending = _int(summary.get("pending") if isinstance(summary, dict) else None)

            for info in await redis.xinfo_groups(STREAM_SPANS):
                if info.get("name") == group:
                    consumer_count = _int(info.get("consumers"))
                    break

            consumers = [
                {
                    "name": c.get("name"),
                    "pending": _int(c.get("pending")),
                    "idle_ms": _int(c.get("idle")),
                }
                for c in await redis.xinfo_consumers(STREAM_SPANS, group)
            ]
            consumer_count = consumer_count or len(consumers)
        except RedisError as exc:
            log_fields(log, logging.DEBUG, "consumer group not available", error=str(exc))
    except RedisError as exc:
        log_fields(log, logging.WARNING, "redis unavailable for /system/stats", error=str(exc))

    counters = {name: _int(raw_stats.get(name)) for name in _COUNTERS}
    return SystemStats(
        stream_depth=stream_depth,
        pending_entries=pending,
        consumer_count=consumer_count,
        consumers=consumers,
        traces_awaiting_finalization=awaiting,
        unknown_models=_unknown_models(raw_stats.get("unknown_models")),
        backpressure_threshold=settings.backpressure_stream_depth,
        **counters,
    )


@router.get("/v1/system/pricing", response_model=PricingResponse)
async def pricing() -> PricingResponse:
    """Publish the cost assumption.

    Design doc §7.7: cost is computed from a static, manually-refreshed pricing
    table. Serving `as_of` and `source` to the dashboard is what turns that from
    a silently wrong number into a stated assumption a reviewer can check.
    """
    return PricingResponse(**get_pricing().as_dict())


@router.get("/v1/loadtests", response_model=list[LoadTestRun])
async def load_tests(limit: int = Query(default=20, ge=1, le=1_000)) -> list[LoadTestRun]:
    settings = get_settings()
    pool = await get_pool()
    return await queries.fetch_load_test_runs(
        pool, limit=clamp_limit(limit, settings.max_page_size)
    )


@router.get("/health", response_model=HealthResponse)
async def health(response: Response) -> HealthResponse:
    """Both dependencies, checked for real.

    A read service that reports healthy while Postgres is down is worse than one
    that reports nothing: it survives the load balancer's check and then 500s
    every request behind it.
    """
    settings = get_settings()
    redis_ok = False
    postgres_ok = False
    stream_depth: int | None = None

    try:
        redis = await get_redis()
        redis_ok = bool(await redis.ping())
        stream_depth = await consumer_group_backlog(redis, STREAM_SPANS, settings.worker_consumer_group)
    except (RedisError, OSError) as exc:
        log_fields(log, logging.WARNING, "health: redis check failed", error=str(exc))

    try:
        pool = await get_pool()
        postgres_ok = await queries.ping(pool)
    except Exception as exc:  # asyncpg raises a wide family here; any of them is "down"
        log_fields(log, logging.WARNING, "health: postgres check failed", error=str(exc))

    healthy = redis_ok and postgres_ok
    if not healthy:
        # 503, so a load balancer or `make stack` health gate actually reacts
        # instead of reading a 200 with "degraded" buried in the body.
        response.status_code = 503
    return HealthResponse(
        status="ok" if healthy else "degraded",
        redis=redis_ok,
        postgres=postgres_ok,
        stream_depth=stream_depth,
        version=__version__,
    )
