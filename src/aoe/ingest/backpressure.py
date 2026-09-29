"""Stream-depth backpressure for the ingestion endpoint (design doc §5.2).

The rule the doc sets is "return 503 with Retry-After rather than blocking".
Blocking is what turns a burst into an outage: held requests occupy the event
loop, client timeouts fire, the client retries, and the arrival rate goes *up*
at exactly the moment the buffer is already behind. Shedding is the only
response that reduces load.

`XLEN` is a round trip, so it is polled on a timer by a background task and read
from memory in the request path. Polling rather than lazily refreshing on first
stale read matters: a lazy refresh puts the latency of a struggling Redis onto
whichever unlucky request triggers it, and that request was going to be accepted
anyway.
"""

from __future__ import annotations

import asyncio
import logging
import time

import redis.asyncio as aioredis

from aoe import redis_keys
from aoe.redis_client import consumer_group_backlog

logger = logging.getLogger("ingest.backpressure")


class StreamDepthMonitor:
    """Caches the buffer depth so `should_shed()` is a memory read."""

    def __init__(
        self,
        redis: aioredis.Redis,
        *,
        stream: str = redis_keys.STREAM_SPANS,
        threshold: int,
        poll_ms: int,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._threshold = threshold
        self._interval_s = max(poll_ms, 1) / 1000.0
        self._depth = 0
        self._updated_at = 0.0
        self._healthy = False
        self._shedding = False
        self._task: asyncio.Task[None] | None = None
        # Mirrored to Redis so a second reader (the query API's /v1/system/stats,
        # another ingest process) can report depth without its own XLEN.
        self._cache_ttl_s = max(1, int(self._interval_s * 4) + 1)

    # -- readings ----------------------------------------------------------

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def threshold(self) -> int:
        return self._threshold

    @property
    def healthy(self) -> bool:
        """True when the last poll reached Redis."""
        return self._healthy

    @property
    def age_ms(self) -> int:
        if self._updated_at == 0.0:
            return -1
        return int((time.monotonic() - self._updated_at) * 1000)

    def should_shed(self) -> bool:
        if self._threshold <= 0:
            return False
        # Deliberately fails open on a stale reading. A Redis blip that stops the
        # poller would otherwise shed 100% of traffic while XADD may still be
        # working; if XADD is genuinely down the write path sheds on its own.
        return self._depth >= self._threshold

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        # One awaited refresh so the very first request decides on a real
        # reading rather than the zero-initialised default.
        await self._refresh()
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="stream-depth-monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            await self._refresh()

    async def _refresh(self) -> None:
        try:
            depth = await consumer_group_backlog(self._redis, self._stream, self._group)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._healthy:
                logger.warning(
                    "stream depth poll failed; serving stale reading",
                    extra={"fields": {"error": str(exc), "last_depth": self._depth}},
                )
            self._healthy = False
            return

        self._depth = depth
        self._updated_at = time.monotonic()
        self._healthy = True
        self._log_transition(depth)

        try:
            await self._redis.set(redis_keys.STREAM_DEPTH_CACHE, depth, ex=self._cache_ttl_s)
        except asyncio.CancelledError:
            raise
        except Exception:  # the mirror is a convenience; never fail the poll for it
            pass

    def _log_transition(self, depth: int) -> None:
        """Log the edges only — a per-request shed log at 1k RPS is its own outage."""
        shedding = self._threshold > 0 and depth >= self._threshold
        if shedding == self._shedding:
            return
        self._shedding = shedding
        logger.warning(
            "backpressure engaged" if shedding else "backpressure released",
            extra={"fields": {"stream_depth": depth, "threshold": self._threshold}},
        )
