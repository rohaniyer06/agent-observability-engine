"""`WS /v1/live` — the dashboard's live feed.

Two decisions worth stating up front.

**One subscription per process, not per socket.** The naive version opens a Redis
pub/sub connection inside the WebSocket handler, so N open dashboard tabs means N
Redis connections all receiving identical bytes. `LiveFeedBroker` subscribes once
and fans out in memory; the Redis cost of the live feed is O(1) in connected
clients.

**Bounded, lossy per-socket queues.** A browser tab on a bad connection, or one
whose JS thread is busy, applies backpressure to the socket. If the broker awaited
that socket, one slow client would stall the feed for every other client and — via
the pub/sub reader task — for the broker itself. So each socket gets a bounded
queue, and when it fills the OLDEST frame is dropped: on a live feed, the newest
frame is the valuable one. Drops are counted, not swallowed.

This asymmetry is the point of DEVIATIONS.md #7 and design doc §5.3.5: a dropped
live-feed frame is a missed animation. Durability lives on the ingest leg, where
losing a span means losing telemetry.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from aoe.api import queries
from aoe.db.pool import get_pool
from aoe.logging import log_fields
from aoe.redis_client import get_redis

router = APIRouter(tags=["live"])
log = logging.getLogger("aoe.api.live")

# ~1.5 minutes of headroom at 2 traces/sec. Deep enough to ride out a GC pause in
# the browser, shallow enough that a wedged tab cannot pin megabytes of frames.
QUEUE_MAXSIZE = 200
# Enough to fill the live panel on first paint so a freshly-opened dashboard is
# not blank until the next trace finalizes.
BACKLOG_SIZE = 20


class _Subscriber:
    """One connected socket's bounded mailbox."""

    __slots__ = ("queue", "dropped")

    def __init__(self, maxsize: int = QUEUE_MAXSIZE) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def offer(self, frame: str) -> None:
        """Non-blocking enqueue. Never awaits, so the broker never blocks."""
        try:
            self.queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass
        # Full: evict the oldest frame rather than dropping the newest. A live
        # feed that falls behind should skip ahead, not replay stale history.
        with contextlib.suppress(asyncio.QueueEmpty):
            self.queue.get_nowait()
        self.dropped += 1
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(frame)


class LiveFeedBroker:
    """Single Redis pub/sub subscription, fanned out to every connected socket."""

    def __init__(self, channel: str) -> None:
        self._channel = channel
        self._subscribers: set[_Subscriber] = set()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self.frames_received = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="live-feed-broker")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._subscribers.clear()

    def subscribe(self) -> _Subscriber:
        sub = _Subscriber()
        self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: _Subscriber) -> None:
        # discard, not remove: a socket that raised mid-teardown must always come
        # out of the registry, and double-unsubscribe must not be an error.
        self._subscribers.discard(sub)
        if sub.dropped:
            log_fields(log, logging.INFO, "live socket closed with drops", dropped=sub.dropped)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def _fanout(self, frame: str) -> None:
        # Iterate a snapshot: offer() cannot mutate the set, but a handler
        # unsubscribing from another task between iterations can.
        for sub in tuple(self._subscribers):
            sub.offer(frame)

    async def _run(self) -> None:
        """Subscribe, forward, and reconnect forever.

        A Redis blip must degrade the live panel, not kill it permanently — if
        this task exited on the first error, every future socket would silently
        receive nothing but its hello frame.
        """
        backoff = 0.5
        while not self._stopping:
            pubsub = None
            try:
                redis = await get_redis()
                pubsub = redis.pubsub(ignore_subscribe_messages=True)
                await pubsub.subscribe(self._channel)
                backoff = 0.5
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    self.frames_received += 1
                    # Forwarded verbatim. The worker already published a
                    # LiveTraceEvent in contract shape; re-parsing and re-encoding
                    # it here would burn CPU on the read service to change nothing.
                    self._fanout(message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_fields(log, logging.WARNING, "live feed subscription lost", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
            finally:
                if pubsub is not None:
                    with contextlib.suppress(Exception):
                        await pubsub.aclose()


async def _socket_writer(websocket: WebSocket, sub: _Subscriber) -> None:
    """The ONLY task that writes to this socket.

    Starlette's WebSocket is not safe for concurrent sends, so pongs are routed
    through the same queue rather than written from the reader task.
    """
    while True:
        frame = await sub.queue.get()
        await websocket.send_text(frame)


async def _client_reader(websocket: WebSocket, sub: _Subscriber) -> None:
    """Read-only channel: keepalive, and disconnect detection.

    The socket carries no client commands by design. It exists to notice the
    close frame — without a pending receive, a browser that went away is only
    discovered on the next send, which for an idle feed may be never.
    """
    while True:
        message = await websocket.receive_text()
        if message.strip().lower() == "ping":
            # Idle-timeout defence: proxies routinely cut a WebSocket that has
            # been silent for 60s, and an observability dashboard is silent
            # whenever the system is healthy.
            sub.offer("pong")


@router.websocket("/v1/live")
async def live_feed(websocket: WebSocket) -> None:
    await websocket.accept()
    broker: LiveFeedBroker = websocket.app.state.live_broker

    backlog: list[dict] = []
    try:
        pool = await get_pool()
        recent = await queries.fetch_traces(pool, limit=BACKLOG_SIZE)
        backlog = [t.model_dump() for t in recent]
    except Exception as exc:
        # An empty backlog is a worse first paint, not a reason to refuse the
        # socket — live frames will still arrive.
        log_fields(log, logging.WARNING, "live backlog unavailable", error=str(exc))

    await websocket.send_text(json.dumps({"type": "hello", "backlog": backlog}))

    sub = broker.subscribe()
    writer = asyncio.create_task(_socket_writer(websocket, sub), name="live-writer")
    reader = asyncio.create_task(_client_reader(websocket, sub), name="live-reader")
    try:
        # Either task finishing means this connection is over: the reader ends on
        # disconnect, the writer ends when a send fails. Whichever it is, the
        # other must be cancelled or it leaks for the lifetime of the process.
        done, pending = await asyncio.wait(
            {writer, reader}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                log_fields(log, logging.INFO, "live socket ended", error=str(exc))
    finally:
        broker.unsubscribe(sub)
        if websocket.client_state is not WebSocketState.DISCONNECTED:
            with contextlib.suppress(Exception):
                await websocket.close()
