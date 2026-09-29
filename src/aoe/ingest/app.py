"""FastAPI ingestion service (design doc §5.2, contract in docs/API.md).

This is the only process the synthetic load generator touches, so everything
here is written for the hot path:

* no `BaseHTTPMiddleware` — Starlette's middleware base wraps every request in an
  anyio task group, which is real per-request cost for checks that are three
  attribute reads;
* the request body is parsed by hand rather than through a `Span | SpanBatch`
  union model, because a union produces a 422 listing both branches' failures and
  a client cannot tell from it which shape it got wrong;
* per-span validation, so one malformed span in a batch of 500 costs you that
  span and not the other 499;
* one Redis round trip per request regardless of batch size.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, ValidationError

from aoe import __version__, redis_keys
from aoe.config import get_settings
from aoe.ingest.backpressure import StreamDepthMonitor
from aoe.ingest.ratelimit import TokenBucketLimiter
from aoe.logging import setup_logging
from aoe.redis_client import close_redis, ensure_consumer_group, get_redis
from aoe.schema import HealthResponse, IngestAccepted, Span

logger = logging.getLogger("ingest")

SERVICE_NAME = "aoe-ingest"
# A fully-invalid batch of 500 would otherwise return a response body larger than
# the request that caused it. The `rejected` count still reports the true total.
MAX_REPORTED_ERRORS = 10
# One span with 40 bad fields should not crowd out the other spans' errors.
MAX_ERRORS_PER_SPAN = 3


class _BodyTooLarge(Exception):
    pass


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging(settings.log_level, "ingest")

    redis = await get_redis()
    app.state.settings = settings
    app.state.redis = redis
    app.state.limiter = TokenBucketLimiter(settings.rate_limit_rps)
    app.state.monitor = StreamDepthMonitor(
        redis,
        stream=redis_keys.STREAM_SPANS,
        threshold=settings.backpressure_stream_depth,
        poll_ms=settings.backpressure_poll_ms,
    )

    try:
        # Created here as well as in the worker so the group exists from the
        # moment the first span lands. Without it, spans XADDed before any worker
        # has ever started are outside every consumer group's delivery window and
        # are silently invisible — a "durable buffer" that quietly drops the
        # first N spans of a fresh deployment.
        await ensure_consumer_group(redis, redis_keys.STREAM_SPANS, settings.worker_consumer_group)
    except Exception as exc:
        # Booting into a degraded state beats crash-looping when Redis is slow to
        # come up under docker compose; /health reports the truth either way.
        logger.error("consumer group setup failed", extra={"fields": {"error": str(exc)}})

    await app.state.monitor.start()
    logger.info(
        "ingest ready",
        extra={
            "fields": {
                "stream": redis_keys.STREAM_SPANS,
                "group": settings.worker_consumer_group,
                "stream_maxlen": settings.stream_maxlen,
                "backpressure_depth": settings.backpressure_stream_depth,
                "rate_limit_rps": settings.rate_limit_rps,
            }
        },
    )

    try:
        yield
    finally:
        await app.state.monitor.stop()
        await close_redis()


app = FastAPI(
    title="AOE Ingestion",
    version=__version__,
    summary="Span ingestion -> Redis Streams durable buffer",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json(
    status_code: int,
    payload: BaseModel | dict[str, Any],
    headers: dict[str, str] | None = None,
) -> Response:
    """Serialise once, ourselves.

    Returning a Response makes FastAPI skip response-model re-validation, which
    on this endpoint is pure overhead — the body was built from a model a line
    earlier.
    """
    body = payload.model_dump_json() if isinstance(payload, BaseModel) else json.dumps(payload)
    return Response(
        content=body,
        status_code=status_code,
        media_type="application/json",
        headers=headers,
    )


async def _read_body(request: Request, limit: int) -> bytes:
    """Read at most `limit` bytes, refusing an oversized body before buffering it."""
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            if int(declared) > limit:
                raise _BodyTooLarge
        except ValueError:
            pass  # malformed header; the streaming guard below still applies

    # Chunked requests declare no length, so the cap is also enforced as the body
    # arrives rather than trusting the header alone.
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise _BodyTooLarge
        chunks.append(chunk)
    return b"".join(chunks)


def _extract_raw_spans(parsed: Any) -> list[Any] | None:
    """Normalise the three accepted body shapes to a list. None = not a span body."""
    if isinstance(parsed, dict):
        if "spans" in parsed:
            spans = parsed["spans"]
            return spans if isinstance(spans, list) else None
        return [parsed]
    # A bare top-level array is not in the documented contract but is the obvious
    # client mistake, and accepting it only ever turns a 400 into a 202.
    if isinstance(parsed, list):
        return parsed
    return None


def _describe(index: int, exc: ValidationError) -> str:
    parts = []
    for err in exc.errors()[:MAX_ERRORS_PER_SPAN]:
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"{loc}: {err['msg']}")
    return f"span[{index}]: " + "; ".join(parts)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/")
async def banner() -> dict[str, str]:
    return {"service": SERVICE_NAME, "version": __version__, "docs": "/docs"}


@app.post("/v1/spans", response_model=IngestAccepted, status_code=202)
async def ingest_spans(request: Request) -> Response:
    settings = request.app.state.settings
    monitor: StreamDepthMonitor = request.app.state.monitor
    limiter: TokenBucketLimiter = request.app.state.limiter

    # Cheapest-first, and all three reject before the body is buffered. 429 and
    # 413 outrank 503 because they are properties of the request itself: a client
    # sending a 4MB body should be told so even while the buffer is backing up.
    if limiter.enabled:
        client = request.client
        if not limiter.allow(client.host if client else "unknown"):
            return _json(
                429,
                {"detail": f"rate limit exceeded ({settings.rate_limit_rps}/s per client)"},
                headers={"Retry-After": "1"},
            )

    if monitor.should_shed():
        return _json(
            503,
            {
                "detail": (
                    f"ingest shedding load: stream depth {monitor.depth} >= threshold "
                    f"{monitor.threshold}. The buffer is draining; retry shortly."
                ),
                "stream_depth": monitor.depth,
                "threshold": monitor.threshold,
                "retry_after_s": settings.backpressure_retry_after_s,
            },
            headers={"Retry-After": str(settings.backpressure_retry_after_s)},
        )

    try:
        body = await _read_body(request, settings.max_body_bytes)
    except _BodyTooLarge:
        return _json(413, {"detail": f"body exceeds {settings.max_body_bytes} bytes"})

    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        return _json(400, {"detail": f"body is not valid JSON: {exc}"})

    raw_spans = _extract_raw_spans(parsed)
    if raw_spans is None:
        return _json(400, {"detail": 'body must be a span object or {"spans": [...]}'})
    if not raw_spans:
        return _json(400, {"detail": "spans must contain at least one span"})
    if len(raw_spans) > settings.max_batch_spans:
        return _json(
            413,
            {
                "detail": f"batch of {len(raw_spans)} exceeds max_batch_spans "
                f"({settings.max_batch_spans})"
            },
        )

    valid: list[Span] = []
    errors: list[str] = []
    for i, raw in enumerate(raw_spans):
        try:
            valid.append(Span.model_validate(raw))
        except ValidationError as exc:
            if len(errors) < MAX_REPORTED_ERRORS:
                errors.append(_describe(i, exc))
    rejected = len(raw_spans) - len(valid)

    if not valid:
        # Nothing usable in the whole body: that is a client bug worth a 4xx,
        # unlike a batch where one span was bad and 499 were fine.
        return _json(
            422,
            {
                "detail": "every span failed validation",
                "accepted": 0,
                "rejected": rejected,
                "stream_depth": monitor.depth,
                "errors": errors,
            },
        )

    try:
        await _enqueue(request.app, valid)
    except Exception as exc:
        # The buffer is the durability guarantee. If we cannot write to it, say so
        # with a retryable status rather than pretending the spans were accepted.
        logger.error("XADD failed", extra={"fields": {"error": str(exc), "spans": len(valid)}})
        return _json(
            503,
            {"detail": f"buffer unavailable: {exc}"},
            headers={"Retry-After": str(settings.backpressure_retry_after_s)},
        )

    return _json(
        202,
        IngestAccepted(
            accepted=len(valid),
            rejected=rejected,
            # The cached reading, not a fresh XLEN: the client sees the same
            # number the shed decision is made on.
            stream_depth=monitor.depth,
            errors=errors,
        ),
    )


async def _enqueue(app: FastAPI, spans: list[Span]) -> None:
    """One round trip: N XADDs plus the ingest counter, pipelined.

    `transaction=False` because these are independent appends — MULTI/EXEC would
    buy atomicity nothing here needs and make Redis hold the whole batch before
    executing any of it.
    """
    settings = app.state.settings
    enqueued_at_ms = int(time.time() * 1000)

    pipe = app.state.redis.pipeline(transaction=False)
    for span in spans:
        pipe.xadd(
            redis_keys.STREAM_SPANS,
            {
                "payload": span.model_dump_json(by_alias=True),
                "enqueued_at_ms": enqueued_at_ms,
            },
            # Design doc §7.3: without a trim the stream grows until Redis OOMs
            # mid-load-test. Approximate trimming lets Redis stop at a node
            # boundary, which is what makes it O(1) instead of O(entries).
            maxlen=settings.stream_maxlen,
            approximate=True,
        )
    # Once per request, not once per span, so a 500-span batch is one HINCRBY.
    pipe.hincrby(redis_keys.WORKER_STATS, "spans_ingested", len(spans))
    await pipe.execute()


@app.get("/health", response_model=HealthResponse)
async def health(request: Request) -> Response:
    monitor: StreamDepthMonitor = request.app.state.monitor
    try:
        # Bounded: an unresponsive Redis must fail the check, not hang it, or a
        # container healthcheck waits forever instead of restarting us.
        await asyncio.wait_for(request.app.state.redis.ping(), timeout=2.0)
        redis_ok = True
    except Exception:
        redis_ok = False

    payload = HealthResponse(
        status="ok" if redis_ok else "degraded",
        redis=redis_ok,
        # Ingestion never touches Postgres; None means "not checked here", which
        # is honest, where False would report an outage this process cannot see.
        postgres=None,
        stream_depth=monitor.depth if redis_ok else None,
        version=__version__,
    )
    return _json(200 if redis_ok else 503, payload)
