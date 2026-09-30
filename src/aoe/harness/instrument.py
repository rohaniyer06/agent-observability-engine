"""Span emission for the harness (design doc §5.1, §4.1).

Two pieces:

* `SpanEmitter` — an async, batching, fire-and-forget client for
  `POST /v1/spans`. A telemetry failure must never fail the agent run; that is
  the entire point of an observability sidecar, and it is the one invariant
  worth writing down twice.
* `TraceRecorder` — per-run bookkeeping. Opens a child span per node, and emits
  the root `invoke_agent` span LAST with `trace_end=True`.

`cost_usd` is deliberately left unset on every span. The worker owns costing so
there is exactly one implementation of it (design doc §7.7, DEVIATIONS.md #9).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx

from aoe.harness.providers import LLMResult, ProviderError
from aoe.logging import log_fields
from aoe.schema import Span

log = logging.getLogger("aoe.harness.instrument")

ROOT_NODE_NAME = "invoke_agent"

DEFAULT_BATCH_SIZE = 50
# How long the flusher will wait for a batch to fill before posting what it has.
# Bounds the telemetry lag without giving up batching under concurrency.
DEFAULT_LINGER_MS = 200
# Bounded queue: if telemetry cannot keep up we drop and count, never block the
# agent. An observability sidecar that applies backpressure to the workload it
# is observing has stopped being a sidecar.
DEFAULT_QUEUE_MAXSIZE = 20_000
DEFAULT_TIMEOUT_S = 5.0
# A single bounded retry after Retry-After. Anything more aggressive is hammering
# a system that just told you it is saturated.
MAX_RETRY_AFTER_S = 5.0


@dataclass
class EmitterStats:
    submitted: int = 0
    emitted: int = 0
    rejected: int = 0
    dropped: int = 0
    batches: int = 0
    backpressure_batches: int = 0
    failed_batches: int = 0
    queue_overflows: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "submitted": self.submitted,
            "emitted": self.emitted,
            "rejected": self.rejected,
            "dropped": self.dropped,
            "batches": self.batches,
            "backpressure_batches": self.backpressure_batches,
            "failed_batches": self.failed_batches,
            "queue_overflows": self.queue_overflows,
        }


class SpanEmitter:
    """Buffers spans and POSTs them as `{"spans": [...]}` batches."""

    def __init__(
        self,
        settings,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        linger_ms: int = DEFAULT_LINGER_MS,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        self._url = f"{settings.ingest_url.rstrip('/')}/v1/spans"
        # The endpoint rejects oversized batches with 413; never build one.
        self._batch_size = max(1, min(batch_size, settings.max_batch_spans))
        self._linger_s = linger_ms / 1000.0
        self._timeout_s = timeout_s
        self._queue: asyncio.Queue[Span | None] = asyncio.Queue(maxsize=queue_maxsize)
        self._client: httpx.AsyncClient | None = None
        self._task: asyncio.Task[None] | None = None
        self.stats = EmitterStats()

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None:
            return
        self._client = httpx.AsyncClient(
            timeout=self._timeout_s,
            # One TCP connection is plenty for a batching emitter and keeps the
            # harness from competing with itself for sockets.
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
        )
        self._task = asyncio.create_task(self._run(), name="aoe-span-emitter")

    async def aclose(self) -> None:
        if self._task is not None:
            await self._queue.put(None)  # sentinel: drain and stop
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> SpanEmitter:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- producer side -----------------------------------------------------

    def submit(self, span: Span) -> None:
        """Hand a span to the emitter. Never blocks, never raises."""
        self.stats.submitted += 1
        try:
            self._queue.put_nowait(span)
        except asyncio.QueueFull:
            self.stats.queue_overflows += 1
            self.stats.dropped += 1
            log_fields(
                log,
                logging.WARNING,
                "span dropped: emitter queue full",
                trace_id=str(span.trace_id),
                node=span.node_name,
            )

    # -- consumer side -----------------------------------------------------

    async def _run(self) -> None:
        stopping = False
        while not stopping:
            first = await self._queue.get()
            if first is None:
                break
            batch = [first]
            # Flush early on trace end so a finished trace does not sit in the
            # buffer waiting for unrelated traffic; otherwise linger for a batch.
            flush_now = first.trace_end
            deadline = asyncio.get_running_loop().time() + self._linger_s
            while not flush_now and len(batch) < self._batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if nxt is None:
                    stopping = True
                    break
                batch.append(nxt)
                flush_now = nxt.trace_end
            await self._post(batch)

        # Drain whatever is left after the sentinel.
        rest: list[Span] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                continue
            rest.append(item)
            if len(rest) >= self._batch_size:
                await self._post(rest)
                rest = []
        if rest:
            await self._post(rest)

    async def _post(self, batch: list[Span]) -> None:
        """Post one batch. Swallows everything — the agent must not care."""
        if not batch or self._client is None:
            return
        payload = {"spans": [s.model_dump(by_alias=True, mode="json") for s in batch]}

        for attempt in (0, 1):
            try:
                resp = await self._client.post(self._url, json=payload)
            except httpx.HTTPError as exc:
                self.stats.failed_batches += 1
                self.stats.dropped += len(batch)
                log_fields(
                    log,
                    logging.WARNING,
                    "span batch dropped: transport error",
                    spans=len(batch),
                    error=str(exc),
                )
                return

            if resp.status_code == 503:
                self.stats.backpressure_batches += 1
                retry_after = _retry_after_seconds(resp)
                if attempt == 0 and retry_after is not None:
                    log_fields(
                        log,
                        logging.INFO,
                        "ingest backpressure, retrying once",
                        spans=len(batch),
                        retry_after_s=retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                self.stats.dropped += len(batch)
                log_fields(
                    log,
                    logging.WARNING,
                    "span batch shed: ingest saturated",
                    spans=len(batch),
                )
                return

            if resp.is_success:
                self.stats.batches += 1
                accepted, rejected, errors = _read_accept_body(resp, len(batch))
                self.stats.emitted += accepted
                self.stats.rejected += rejected
                self.stats.dropped += rejected
                if rejected:
                    log_fields(
                        log,
                        logging.WARNING,
                        "ingest rejected spans in batch",
                        rejected=rejected,
                        errors=errors[:3],
                    )
                return

            self.stats.failed_batches += 1
            self.stats.dropped += len(batch)
            log_fields(
                log,
                logging.WARNING,
                "span batch dropped: ingest returned error",
                status=resp.status_code,
                spans=len(batch),
                body=resp.text[:200],
            )
            return


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, min(float(raw), MAX_RETRY_AFTER_S))
    except ValueError:
        return None


def _read_accept_body(resp: httpx.Response, batch_size: int) -> tuple[int, int, list[str]]:
    try:
        body = resp.json()
    except ValueError:
        return batch_size, 0, []
    if not isinstance(body, dict):
        return batch_size, 0, []
    accepted = int(body.get("accepted", batch_size))
    rejected = int(body.get("rejected", 0))
    errors = [str(e) for e in body.get("errors", []) if e]
    return accepted, rejected, errors


# ---------------------------------------------------------------------------
# Per-run recording
# ---------------------------------------------------------------------------


@dataclass
class NodeSpan:
    """Handle a node body uses to attach usage and attributes to its span."""

    node_name: str
    span_id: uuid.UUID
    attributes: dict[str, Any] = field(default_factory=dict)
    status: str = "ok"
    error_message: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    model_name: str | None = None
    provider_name: str | None = None

    def record(self, result: LLMResult) -> None:
        self.input_tokens += result.input_tokens
        self.output_tokens += result.output_tokens
        self.cache_read_tokens += result.cache_read_tokens
        self.cache_write_tokens += result.cache_write_tokens
        self.model_name = result.model
        self.provider_name = result.provider

    def fail(self, message: str) -> None:
        """Mark the span errored without raising — used where the node recovers.

        A classify timeout that reroutes to escalate is a real error the
        detection layer should see, and also a path the graph continues down.
        """
        self.status = "error"
        self.error_message = message[:2000]


@dataclass
class TraceUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


class TraceRecorder:
    """Owns one trace: its id, its root span, and the path it walked."""

    def __init__(
        self,
        emitter: SpanEmitter,
        *,
        pipeline_name: str,
        model_name: str,
        provider_name: str,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        self.emitter = emitter
        self.pipeline_name = pipeline_name
        self.model_name = model_name
        self.provider_name = provider_name
        self.trace_id = uuid.uuid4()
        self.root_span_id = uuid.uuid4()
        self.path: list[str] = []
        self.usage = TraceUsage()
        self.span_count = 0
        self.error_span_count = 0
        self.root_attributes: dict[str, Any] = dict(attributes or {})
        self._finished = False
        # Wall clock anchors the timestamps; the monotonic clock measures the
        # duration. Using time.time_ns() for both makes an NTP step look like a
        # latency spike.
        self._start_wall_ns = time.time_ns()
        self._start_perf_ns = time.perf_counter_ns()

    @asynccontextmanager
    async def node(self, node_name: str) -> AsyncIterator[NodeSpan]:
        span = NodeSpan(node_name=node_name, span_id=uuid.uuid4())
        self.path.append(node_name)
        start_wall_ns = time.time_ns()
        start_perf_ns = time.perf_counter_ns()
        try:
            yield span
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            span.fail(f"{type(exc).__name__}: {exc}")
            # A failed call still burned tokens; record them or the cost
            # dashboard under-reports exactly the runs that went wrong.
            if isinstance(exc, ProviderError) and exc.usage is not None:
                span.record(exc.usage)
            self._emit_node(span, start_wall_ns, start_perf_ns)
            raise
        else:
            self._emit_node(span, start_wall_ns, start_perf_ns)

    def _emit_node(self, span: NodeSpan, start_wall_ns: int, start_perf_ns: int) -> None:
        end_wall_ns = start_wall_ns + (time.perf_counter_ns() - start_perf_ns)
        self.span_count += 1
        if span.status == "error":
            self.error_span_count += 1
        self.usage.input_tokens += span.input_tokens
        self.usage.output_tokens += span.output_tokens
        self.usage.cache_read_tokens += span.cache_read_tokens
        self.usage.cache_write_tokens += span.cache_write_tokens

        self.emitter.submit(
            Span(
                trace_id=self.trace_id,
                span_id=span.span_id,
                parent_span_id=self.root_span_id,
                operation_name="chat",
                model_name=span.model_name or self.model_name,
                provider_name=span.provider_name or self.provider_name,
                node_name=span.node_name,
                pipeline_name=self.pipeline_name,
                start_time_ns=start_wall_ns,
                end_time_ns=end_wall_ns,
                input_tokens=span.input_tokens,
                output_tokens=span.output_tokens,
                cache_read_tokens=span.cache_read_tokens,
                cache_write_tokens=span.cache_write_tokens,
                status=span.status,
                error_message=span.error_message,
                # cost_usd intentionally unset: the worker owns costing.
                trace_end=False,
                attributes=span.attributes,
            )
        )

    def finish(self, *, status: str = "ok", error_message: str | None = None) -> None:
        """Emit the root span. Last span of the trace, and the end-of-trace signal."""
        if self._finished:
            return
        self._finished = True
        end_wall_ns = self._start_wall_ns + (time.perf_counter_ns() - self._start_perf_ns)

        attributes = dict(self.root_attributes)
        # The harness's own view of the path. The worker derives `traces.path`
        # from the child spans; this is here so a drill-down can compare the two
        # when spans were shed under backpressure.
        attributes["path"] = list(self.path)
        attributes["span_count"] = self.span_count
        attributes["error_span_count"] = self.error_span_count

        self.emitter.submit(
            Span(
                trace_id=self.trace_id,
                span_id=self.root_span_id,
                parent_span_id=None,
                operation_name="invoke_agent",
                model_name=self.model_name,
                provider_name=self.provider_name,
                node_name=ROOT_NODE_NAME,
                pipeline_name=self.pipeline_name,
                start_time_ns=self._start_wall_ns,
                end_time_ns=end_wall_ns,
                input_tokens=0,
                output_tokens=0,
                status=status,
                error_message=error_message[:2000] if error_message else None,
                trace_end=True,
                attributes=attributes,
            )
        )
