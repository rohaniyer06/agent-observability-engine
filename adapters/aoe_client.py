"""Standalone telemetry client. Copy this ONE file into the app you want to observe.

Deliberately has no imports from `aoe` and no dependency on this repository. The
observed application should not have to take on the observability system as a
dependency — it only has to speak HTTP to it. Requires `httpx` (or swap the two
lines in `_post` for `requests`).

Two integration styles:

    1. Context managers — explicit, works with any framework, testable.
    2. `LangGraphCallback` — zero changes to node bodies, but the token-usage
       extraction is LangChain-version sensitive. Verify it before trusting it.

--------------------------------------------------------------------------------
NAME MAPPING — read this before pointing it at a production pipeline
--------------------------------------------------------------------------------
`node_name` and `pipeline_name` land in the database and on the dashboard. If the
pipeline you are instrumenting is proprietary, its node names ARE the internal
taxonomy, and a dashboard screenshot leaks it.

Pass `name_map` to rename at the boundary, so real names never leave the process:

    emitter = TelemetryEmitter(
        pipeline_name="ticket_triage",              # generic label
        name_map={"<real_node>": "extract", ...},   # real -> generic
        on_unmapped="reject",                       # refuse to emit unknown names
    )

`on_unmapped="reject"` is the safe default: a node you forgot to map is dropped
rather than published under its real name. Use "passthrough" only for code you
own outright.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

log = logging.getLogger("aoe.client")

Operation = Literal["chat", "execute_tool", "invoke_agent"]
UnmappedPolicy = Literal["reject", "passthrough"]


@dataclass
class _Span:
    trace_id: str
    span_id: str
    node_name: str
    pipeline_name: str
    operation_name: Operation
    start_time_ns: int
    end_time_ns: int
    parent_span_id: str | None = None
    model_name: str | None = None
    provider_name: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    status: str = "ok"
    error_message: str | None = None
    trace_end: bool = False
    attributes: dict[str, Any] = field(default_factory=dict)

    def wire(self) -> dict[str, Any]:
        """The exact shape POST /v1/spans accepts (OTel GenAI field names)."""
        payload: dict[str, Any] = {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "gen_ai.operation.name": self.operation_name,
            "node_name": self.node_name,
            "pipeline_name": self.pipeline_name,
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns,
            "gen_ai.usage.input_tokens": self.input_tokens,
            "gen_ai.usage.output_tokens": self.output_tokens,
            "gen_ai.usage.cache_read_input_tokens": self.cache_read_tokens,
            "gen_ai.usage.cache_creation_input_tokens": self.cache_write_tokens,
            "status": self.status,
            "trace_end": self.trace_end,
            "attributes": self.attributes,
        }
        if self.model_name:
            payload["gen_ai.request.model"] = self.model_name
        if self.provider_name:
            payload["gen_ai.provider.name"] = self.provider_name
        if self.error_message:
            payload["error_message"] = self.error_message[:2000]
        # cost_usd is deliberately omitted: the worker prices every span from its
        # own pricing table, so there is exactly one costing implementation.
        return payload


class TelemetryEmitter:
    """Buffers spans and ships them on a background thread.

    Two properties matter more than throughput here:

    * It never raises into the caller. An observability sidecar that can take
      down the thing it observes is worse than no observability.
    * It never blocks the agent. Sends happen on a worker thread behind a
      bounded queue; if the queue fills, spans are dropped and counted rather
      than applying backpressure to your production pipeline.
    """

    def __init__(
        self,
        pipeline_name: str,
        ingest_url: str = "http://localhost:8000",
        *,
        name_map: dict[str, str] | Callable[[str], str | None] | None = None,
        on_unmapped: UnmappedPolicy = "reject",
        batch_size: int = 50,
        flush_interval_s: float = 1.0,
        queue_size: int = 10_000,
        timeout_s: float = 5.0,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.url = ingest_url.rstrip("/") + "/v1/spans"
        self._name_map = name_map
        self._on_unmapped = on_unmapped
        self._batch_size = batch_size
        self._flush_interval = flush_interval_s
        self._timeout = timeout_s

        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=queue_size)
        self._client = httpx.Client(timeout=timeout_s)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="aoe-emitter", daemon=True)
        self._thread.start()

        self.dropped = 0
        self.sent = 0
        self.shed = 0        # batches the server rejected with 503 backpressure
        self.unmapped: set[str] = set()

    # -- naming ------------------------------------------------------------

    def resolve_name(self, raw: str) -> str | None:
        """Map a real node name to its published label. None means 'do not emit'."""
        if self._name_map is None:
            return raw
        mapped = self._name_map(raw) if callable(self._name_map) else self._name_map.get(raw)
        if mapped:
            return mapped
        self.unmapped.add(raw)
        return raw if self._on_unmapped == "passthrough" else None

    # -- span construction -------------------------------------------------

    @contextmanager
    def trace(self, **attributes: Any) -> Iterator[TraceContext]:
        """One agent invocation. Emits the root span, with trace_end, on exit.

        The root span is what lets the server close the trace immediately instead
        of waiting out the idle timeout — and `trace_end` is only meaningful if
        it is emitted LAST, which exiting this block guarantees.
        """
        ctx = TraceContext(self, attributes)
        started = time.time_ns()
        try:
            yield ctx
        except BaseException as exc:
            ctx.status = "error"
            ctx.error_message = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self._enqueue(
                _Span(
                    trace_id=ctx.trace_id,
                    span_id=ctx.root_span_id,
                    parent_span_id=None,
                    node_name="invoke_agent",
                    pipeline_name=self.pipeline_name,
                    operation_name="invoke_agent",
                    start_time_ns=started,
                    end_time_ns=time.time_ns(),
                    status=ctx.status,
                    error_message=ctx.error_message,
                    trace_end=True,
                    attributes=ctx.attributes,
                )
            )

    # -- transport ---------------------------------------------------------

    def _enqueue(self, span: _Span) -> None:
        try:
            self._queue.put_nowait(span.wire())
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        batch: list[dict[str, Any]] = []
        last = time.monotonic()
        while not self._stop.is_set() or not self._queue.empty() or batch:
            timeout = max(0.0, self._flush_interval - (time.monotonic() - last))
            try:
                item = self._queue.get(timeout=timeout or 0.01)
                if item is not None:
                    batch.append(item)
            except queue.Empty:
                pass
            due = time.monotonic() - last >= self._flush_interval
            if batch and (len(batch) >= self._batch_size or due or self._stop.is_set()):
                self._post(batch)
                batch = []
                last = time.monotonic()

    def _post(self, batch: list[dict[str, Any]]) -> None:
        try:
            resp = self._client.post(self.url, json={"spans": batch})
            if resp.status_code == 503:
                # The server is shedding load on purpose. Retrying hard into a
                # system that just said "I am saturated" is how you turn its
                # backpressure into an outage. Drop and count.
                self.shed += len(batch)
                return
            if resp.status_code >= 400:
                log.warning("aoe ingest rejected batch: %s %s", resp.status_code, resp.text[:200])
                self.dropped += len(batch)
                return
            self.sent += len(batch)
        except Exception as exc:  # noqa: BLE001 - telemetry must never raise into the app
            self.dropped += len(batch)
            log.warning("aoe ingest unreachable: %s", exc)

    def flush(self, timeout_s: float = 5.0) -> None:
        """Block until queued spans are sent. Call before the process exits."""
        deadline = time.monotonic() + timeout_s
        while not self._queue.empty() and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(min(self._flush_interval + 0.2, max(0.0, deadline - time.monotonic())))

    def shutdown(self, timeout_s: float = 5.0) -> None:
        self.flush(timeout_s)
        self._stop.set()
        self._thread.join(timeout=timeout_s)
        self._client.close()

    def stats(self) -> dict[str, Any]:
        return {
            "sent": self.sent,
            "dropped": self.dropped,
            "shed_503": self.shed,
            "unmapped_nodes": sorted(self.unmapped),
        }


class TraceContext:
    """Handle for one invocation. Open a `node(...)` block per pipeline step."""

    def __init__(self, emitter: TelemetryEmitter, attributes: dict[str, Any]) -> None:
        self._emitter = emitter
        self.trace_id = str(uuid.uuid4())
        self.root_span_id = str(uuid.uuid4())
        self.attributes = attributes
        self.status = "ok"
        self.error_message: str | None = None

    @contextmanager
    def node(
        self,
        name: str,
        *,
        operation: Operation = "chat",
        model: str | None = None,
        provider: str | None = None,
        **attributes: Any,
    ) -> Iterator[NodeSpan]:
        published = self._emitter.resolve_name(name)
        span = NodeSpan(model=model, provider=provider, attributes=attributes)
        started = time.time_ns()
        try:
            yield span
        except BaseException as exc:
            span.status = "error"
            span.error_message = f"{type(exc).__name__}: {exc}"
            self.status = "error"
            raise
        finally:
            if published is not None:  # None => unmapped under a "reject" policy
                self._emitter._enqueue(
                    _Span(
                        trace_id=self.trace_id,
                        span_id=str(uuid.uuid4()),
                        parent_span_id=self.root_span_id,
                        node_name=published,
                        pipeline_name=self._emitter.pipeline_name,
                        operation_name=operation,
                        start_time_ns=started,
                        end_time_ns=time.time_ns(),
                        model_name=span.model,
                        provider_name=span.provider,
                        input_tokens=span.input_tokens,
                        output_tokens=span.output_tokens,
                        cache_read_tokens=span.cache_read_tokens,
                        cache_write_tokens=span.cache_write_tokens,
                        status=span.status,
                        error_message=span.error_message,
                        attributes=span.attributes,
                    )
                )


@dataclass
class NodeSpan:
    """Mutable handle: set token counts inside the block once the call returns."""

    model: str | None = None
    provider: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    status: str = "ok"
    error_message: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def record_anthropic_usage(self, response: Any) -> None:
        """Pull usage off an Anthropic SDK response.

        `usage.input_tokens` is the UNCACHED remainder, not the total — the cache
        fields are additive. Folding them together is the standard way a cost
        dashboard silently under-reports.
        """
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        self.input_tokens = getattr(usage, "input_tokens", 0) or 0
        self.output_tokens = getattr(usage, "output_tokens", 0) or 0
        self.cache_read_tokens = getattr(usage, "cache_read_input_tokens", 0) or 0
        self.cache_write_tokens = getattr(usage, "cache_creation_input_tokens", 0) or 0
        self.model = getattr(response, "model", self.model)
        self.provider = self.provider or "anthropic"

    def record_langchain_usage(self, message: Any) -> None:
        """Pull usage off a LangChain AIMessage (`usage_metadata`, LC >= 0.2)."""
        meta = getattr(message, "usage_metadata", None) or {}
        self.input_tokens = int(meta.get("input_tokens", 0) or 0)
        self.output_tokens = int(meta.get("output_tokens", 0) or 0)
        details = meta.get("input_token_details") or {}
        self.cache_read_tokens = int(details.get("cache_read", 0) or 0)
        self.cache_write_tokens = int(details.get("cache_creation", 0) or 0)
