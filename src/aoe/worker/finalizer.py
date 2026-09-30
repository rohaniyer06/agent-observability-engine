"""Trace finalization: in-flight state in Redis, closed traces in Postgres.

DEVIATIONS.md #2. The design doc (§7.5) says pick one completion signal — an
explicit `trace_end` event or an idle timeout. Both alone leave a hole: timeout
only adds a fixed lag to every trace and can close a slow one early; trace_end
only loses every trace whose emitter crashed, which is exactly the trace an
observability tool exists to show you. So both signals write into ONE deadline
ZSET and there is one finalization path; `traces.finalized_by` records which
signal fired.

The in-flight state is a hash of per-span records rather than a set of running
counters. That costs a few extra hash fields per open trace and buys idempotency:
a redelivered span rewrites its own field with the same bytes, where HINCRBY
would have double-counted its cost and tokens into the trace total. Given that
at-least-once redelivery is the mechanism the durability claim rests on
(DEVIATIONS.md #3), the accumulator has to survive it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aoe import redis_keys
from aoe.schema import LiveTraceEvent
from aoe.worker import bump_stats, drift, sleep_or_stop
from aoe.worker.anomaly import claim_cost_window, observe_cost, persist_anomaly
from aoe.worker.writer import resolve_cost, to_numeric

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncio

    import asyncpg
    from redis.asyncio import Redis
    from redis.asyncio.client import Pipeline

    from aoe.config import Settings
    from aoe.schema import Span

log = logging.getLogger("aoe.worker.finalizer")

# trace_state hash layout.
FIELD_PIPELINE = "pipeline"
FIELD_TRACE_END = "trace_end"
SPAN_FIELD_PREFIX = "s:"
_SPAN_SEP = "|"
# node_name is last so it can contain the separator without ambiguity.
_SPAN_FIELDS = 8

_UPSERT_TRACE = """
INSERT INTO traces (
    trace_id, pipeline_name, started_at, ended_at, total_duration_us, total_cost_usd,
    total_input_tokens, total_output_tokens, span_count, status, path, path_signature,
    finalized_by
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (trace_id) DO UPDATE
SET pipeline_name       = EXCLUDED.pipeline_name,
    started_at          = LEAST(traces.started_at, EXCLUDED.started_at),
    ended_at            = GREATEST(traces.ended_at, EXCLUDED.ended_at),
    total_duration_us   = EXCLUDED.total_duration_us,
    total_cost_usd      = EXCLUDED.total_cost_usd,
    total_input_tokens  = EXCLUDED.total_input_tokens,
    total_output_tokens = EXCLUDED.total_output_tokens,
    span_count          = EXCLUDED.span_count,
    status              = EXCLUDED.status,
    path                = EXCLUDED.path,
    path_signature      = EXCLUDED.path_signature,
    finalized_by        = EXCLUDED.finalized_by
-- Never let a re-finalization shrink a trace. A straggler arriving after the
-- reaper already closed the trace builds a fresh state hash containing only
-- itself; without this guard it would overwrite a 5-span trace with a 1-span
-- one. The straggler is still in `spans`, so drill-down keeps it.
WHERE EXCLUDED.span_count >= traces.span_count
"""


# ---------------------------------------------------------------------------
# In-flight state
# ---------------------------------------------------------------------------


def encode_span_record(span: Span, cost_usd: float) -> str:
    return _SPAN_SEP.join(
        (
            str(span.start_time_ns),
            str(span.end_time_ns),
            f"{cost_usd:.8f}",
            str(span.input_tokens),
            str(span.output_tokens),
            span.status,
            span.node_name,
            span.operation_name,
        )
    )


@dataclass(frozen=True)
class SpanRecord:
    start_ns: int
    end_ns: int
    cost_usd: float
    input_tokens: int
    output_tokens: int
    status: str
    node_name: str
    # Needed to tell the trace envelope apart from the nodes it wraps. Defaulted
    # so a record written before this field existed still decodes.
    operation_name: str = "chat"


def decode_span_record(raw: str) -> SpanRecord | None:
    parts = raw.split(_SPAN_SEP, _SPAN_FIELDS - 1)
    if len(parts) not in (_SPAN_FIELDS - 1, _SPAN_FIELDS):
        return None
    try:
        return SpanRecord(
            start_ns=int(parts[0]),
            end_ns=int(parts[1]),
            cost_usd=float(parts[2]),
            input_tokens=int(parts[3]),
            output_tokens=int(parts[4]),
            status=parts[5],
            node_name=parts[6],
            operation_name=parts[7] if len(parts) > 7 else "chat",
        )
    except ValueError:
        return None


def stage_trace_state(
    pipe: Pipeline,
    spans: list[Span],
    now_ms: int,
    settings: Settings,
) -> None:
    """Queue trace-state updates and finalize deadlines for a batch.

    Deadline rule, both signals into the one ZSET:

    * a normal span pushes the deadline out to now + idle timeout, with ZADD GT
      so an out-of-order batch cannot pull an existing deadline backwards;
    * a `trace_end` span pulls it in to now + a short grace, with ZADD LT so it
      cannot push a nearer deadline out. The grace rather than finalizing on the
      spot is deliberate: the root span is emitted last but its children can
      still be behind it in the same batch, and closing immediately would
      truncate the path.

    A straggler arriving after trace_end re-opens the idle window. That is the
    intended reading of GT: if spans are still landing, the trace is not done.
    """
    ttl_s = max(60, settings.trace_idle_timeout_ms * 10 // 1000)

    grouped: dict[str, list[Span]] = {}
    for span in spans:
        grouped.setdefault(str(span.trace_id), []).append(span)

    for trace_id, group in grouped.items():
        key = redis_keys.trace_state(trace_id)
        mapping: dict[str, str] = {FIELD_PIPELINE: group[0].pipeline_name}
        ended = False
        for span in group:
            mapping[SPAN_FIELD_PREFIX + str(span.span_id)] = encode_span_record(
                span, resolve_cost(span)
            )
            ended = ended or span.trace_end
        if ended:
            mapping[FIELD_TRACE_END] = "1"

        pipe.hset(key, mapping=mapping)
        # Bound the blast radius of an abandoned trace: without this, a harness
        # that dies mid-run leaks one hash per trace forever.
        pipe.expire(key, ttl_s)

        if ended:
            pipe.zadd(
                redis_keys.TRACE_DEADLINES,
                {trace_id: now_ms + settings.trace_end_grace_ms},
                lt=True,
            )
        else:
            pipe.zadd(
                redis_keys.TRACE_DEADLINES,
                {trace_id: now_ms + settings.trace_idle_timeout_ms},
                gt=True,
            )


@dataclass(frozen=True)
class TraceState:
    trace_id: str
    pipeline_name: str
    span_count: int
    started_ns: int
    ended_ns: int
    total_cost_usd: float
    input_tokens: int
    output_tokens: int
    has_error: bool
    trace_end_seen: bool
    path: list[str]

    @property
    def duration_us(self) -> int:
        return (self.ended_ns - self.started_ns) // 1_000

    @property
    def status(self) -> str:
        # An errored trace is an errored trace whether or not it also timed out;
        # 'partial' means "we closed this without being told it was finished".
        if self.has_error:
            return "error"
        return "ok" if self.trace_end_seen else "partial"

    @property
    def finalized_by(self) -> str:
        return "trace_end" if self.trace_end_seen else "timeout"


def fold_state(trace_id: str, raw: dict[str, str]) -> TraceState | None:
    """Reduce a trace's per-span hash records into the row we are about to write.

    Returns None when the hash holds no spans — an expired or already-reaped
    trace whose deadline outlived it.
    """
    records: list[tuple[str, SpanRecord]] = []
    for field, value in raw.items():
        if not field.startswith(SPAN_FIELD_PREFIX):
            continue
        record = decode_span_record(value)
        if record is not None:
            records.append((field[len(SPAN_FIELD_PREFIX) :], record))
    if not records:
        return None

    # Ordered by first observation. The span_id tiebreak only matters for two
    # spans with identical start times, and exists so the path is deterministic
    # rather than dependent on Redis hash iteration order.
    records.sort(key=lambda item: (item[1].start_ns, item[0]))

    path: list[str] = []
    seen: set[str] = set()
    total_cost = 0.0
    input_tokens = 0
    output_tokens = 0
    has_error = False
    started_ns = records[0][1].start_ns
    ended_ns = records[0][1].end_ns

    for _span_id, record in records:
        total_cost += record.cost_usd
        input_tokens += record.input_tokens
        output_tokens += record.output_tokens
        has_error = has_error or record.status == "error"
        started_ns = min(started_ns, record.start_ns)
        ended_ns = max(ended_ns, record.end_ns)
        # The root `invoke_agent` span is the trace envelope, not a step in the
        # pipeline. It still contributes cost, tokens and the outer duration, but
        # putting it in the path would corrupt the signature, guarantee a
        # permanent mismatch against the harness's seeded baseline paths, and
        # draw a phantom node in the flow diagram.
        if record.operation_name == "invoke_agent":
            continue
        # A node visited twice (the harness's escalate retry loop) appears once
        # in the path: the path describes which nodes were traversed, and
        # counting retries as a different shape would report every retry as drift.
        if record.node_name not in seen:
            seen.add(record.node_name)
            path.append(record.node_name)

    return TraceState(
        trace_id=trace_id,
        pipeline_name=raw.get(FIELD_PIPELINE) or "unknown",
        span_count=len(records),
        started_ns=started_ns,
        ended_ns=ended_ns,
        total_cost_usd=total_cost,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        has_error=has_error,
        trace_end_seen=raw.get(FIELD_TRACE_END) == "1",
        path=path,
    )


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------


def _live_event(state: TraceState, event_type: str, detail: dict) -> LiveTraceEvent:
    return LiveTraceEvent(
        type=event_type,
        trace_id=state.trace_id,
        pipeline_name=state.pipeline_name,
        started_at=datetime.fromtimestamp(state.started_ns / 1e9, tz=UTC).isoformat(),
        duration_ms=state.duration_us / 1000.0,
        total_cost_usd=state.total_cost_usd,
        status=state.status,
        path=state.path,
        span_count=state.span_count,
        detail=detail,
    )


async def _finalize_one(
    redis: Redis,
    conn: asyncpg.Connection,
    settings: Settings,
    seeded_cache: drift.SeededPaths,
    state: TraceState,
) -> list[LiveTraceEvent]:
    """Close one trace: row, cost check, drift check — one Postgres transaction."""
    signature = drift.path_signature(state.path)

    # Detector inputs are gathered before the transaction opens: they are Redis
    # round trips, and holding a Postgres transaction open across them would pin
    # a connection for the duration of the network wait.
    cost_verdict = await observe_cost(redis, state.pipeline_name, state.total_cost_usd, settings)
    seeded = await seeded_cache.get(conn, state.pipeline_name)
    drift_verdict = await drift.observe_path(
        redis, state.pipeline_name, signature, seeded, settings
    )

    cost_window_open = False
    if cost_verdict.is_anomaly and cost_verdict.severity:
        cost_window_open = await claim_cost_window(
            redis,
            state.pipeline_name,
            cost_verdict.severity,
            settings.cost_event_cooldown_ms,
            cost_verdict.z_score,
        )

    new_window = False
    if drift_verdict.is_drift:
        new_window = await drift.claim_drift_window(
            redis, state.pipeline_name, signature, settings.drift_event_cooldown_ms
        )

    started_at = datetime.fromtimestamp(state.started_ns / 1e9, tz=UTC)
    ended_at = datetime.fromtimestamp(state.ended_ns / 1e9, tz=UTC)

    drift_row: tuple[int, int] | None = None
    async with conn.transaction():
        await conn.execute(
            _UPSERT_TRACE,
            state.trace_id,
            state.pipeline_name,
            started_at,
            ended_at,
            state.duration_us,
            to_numeric(state.total_cost_usd),
            state.input_tokens,
            state.output_tokens,
            state.span_count,
            state.status,
            state.path,
            signature,
            state.finalized_by,
        )
        if cost_verdict.is_anomaly and cost_window_open:
            await persist_anomaly(
                conn,
                state.trace_id,
                state.pipeline_name,
                state.total_cost_usd,
                cost_verdict,
            )
        await drift.record_path_observation(conn, state.pipeline_name, signature, state.path)
        if drift_verdict.is_drift:
            drift_row = await drift.persist_drift_event(
                conn,
                state.pipeline_name,
                signature,
                state.path,
                state.trace_id,
                drift_verdict,
                new_window=new_window,
            )

    events = [
        _live_event(
            state,
            "trace_finalized",
            {"finalized_by": state.finalized_by, "path_signature": signature},
        )
    ]
    if cost_verdict.is_anomaly:
        events.append(
            _live_event(
                state,
                "cost_anomaly",
                {
                    "severity": cost_verdict.severity,
                    "z_score": round(cost_verdict.z_score, 4),
                    "expected_cost_usd": cost_verdict.expected_cost_usd,
                    "actual_cost_usd": state.total_cost_usd,
                    "deviation_pct": round(cost_verdict.deviation_pct, 2),
                    "sample_size": cost_verdict.sample_size,
                },
            )
        )
    if drift_verdict.is_drift and drift_row is not None:
        events.append(
            _live_event(
                state,
                "path_drift",
                {
                    "path_signature": signature,
                    "is_novel": drift_verdict.is_novel,
                    "occurrence_count": drift_row[1],
                    "event_id": drift_row[0],
                    "baseline_signatures": drift_verdict.baseline_signatures,
                },
            )
        )
    return events


async def finalize_due_traces(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    seeded_cache: drift.SeededPaths,
) -> int:
    """Close every trace whose deadline has passed. Returns how many closed."""
    now_ms = int(time.time() * 1000)
    due = await redis.zrangebyscore(
        redis_keys.TRACE_DEADLINES, 0, now_ms, start=0, num=settings.finalizer_batch_size
    )
    if not due:
        return 0

    # ZREM is the claim, and it runs in a MULTI with the read so two workers
    # cannot both take the same trace: exactly one of them gets a 1 back.
    claim = redis.pipeline(transaction=True)
    for trace_id in due:
        claim.hgetall(redis_keys.trace_state(trace_id))
        claim.zrem(redis_keys.TRACE_DEADLINES, trace_id)
    results = await claim.execute()

    states: list[TraceState] = []
    orphans: list[str] = []
    for i, trace_id in enumerate(due):
        raw, removed = results[2 * i], results[2 * i + 1]
        if not removed:
            continue  # a sibling worker claimed it in the same tick
        state = fold_state(trace_id, raw)
        if state is None:
            orphans.append(trace_id)
            continue
        states.append(state)

    if orphans:
        await redis.delete(*(redis_keys.trace_state(t) for t in orphans))
    if not states:
        return 0

    finalized = 0
    by_timeout = 0
    events: list[LiveTraceEvent] = []
    closed: list[str] = []
    requeued = 0

    async with pool.acquire() as conn:
        for state in states:
            try:
                events.extend(await _finalize_one(redis, conn, settings, seeded_cache, state))
            except Exception:
                # Put the deadline back rather than dropping the trace. The state
                # hash is still there (it is deleted only on success), so the
                # retry sees the same spans. The cost window and path counters
                # were already bumped, which skews one sample rather than losing
                # a trace — the cheaper of the two errors.
                log.exception("trace finalize failed", extra={"fields": {"trace_id": state.trace_id}})
                await redis.zadd(
                    redis_keys.TRACE_DEADLINES,
                    {state.trace_id: now_ms + settings.trace_idle_timeout_ms},
                )
                requeued += 1
                continue
            finalized += 1
            by_timeout += 0 if state.trace_end_seen else 1
            closed.append(state.trace_id)

    pipe = redis.pipeline(transaction=False)
    if closed:
        pipe.delete(*(redis_keys.trace_state(t) for t in closed))
    for event in events:
        # Pub/sub, not a stream: a dropped dashboard frame is a missed animation,
        # not lost telemetry (design doc §5.3.5).
        pipe.publish(redis_keys.PUBSUB_LIVE, event.model_dump_json())
    await pipe.execute()

    await bump_stats(
        redis,
        traces_finalized=finalized,
        traces_finalized_by_timeout=by_timeout,
    )

    if requeued:
        log.warning("traces requeued after failure", extra={"fields": {"count": requeued}})
    return finalized


async def run_finalizer(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    interval_s = settings.finalizer_interval_ms / 1000.0
    seeded_cache = drift.SeededPaths()
    while not stop.is_set():
        if await sleep_or_stop(stop, interval_s):
            break
        try:
            closed = await finalize_due_traces(redis, pool, settings, seeded_cache)
            if closed:
                log.debug("traces finalized", extra={"fields": {"count": closed}})
        except Exception:
            log.exception("finalizer tick failed")
            if await sleep_or_stop(stop, 1.0):
                break
