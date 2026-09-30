"""Stream consumption: XREADGROUP drain, XAUTOCLAIM reclaim, per-batch work.

The ordering inside `process_batch` is the load-bearing part. Postgres first,
Redis state second, XACK last. Ack-last is what makes a crash safe: an entry that
was processed but not acked is redelivered, and every step it replays is
idempotent, so the cost of a crash is duplicated work rather than lost or
double-counted telemetry.

The reverse order — ack first, then write — would be cheaper and is wrong: a
worker that dies in the gap drops those spans permanently, and the "durable
buffer" claim quietly stops being true.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aoe import redis_keys
from aoe.config import Settings
from aoe.schema import Span, StreamedSpan
from aoe.worker import bump_stats, sleep_or_stop, stage_stats
from aoe.worker.finalizer import stage_trace_state
from aoe.worker.rollup import stage_latency
from aoe.worker.writer import write_spans

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg
    from redis.asyncio import Redis

log = logging.getLogger("aoe.worker.consumer")

# XAUTOCLAIM walks the pending-entries list from a cursor. "0-0" restarts the
# walk; the server hands back the cursor to resume from.
PEL_CURSOR_START = "0-0"


@dataclass
class BatchOutcome:
    entries: int = 0
    spans: int = 0
    inserted: int = 0
    duplicates: int = 0
    poison: int = 0


def decode_entry(entry_id: str, fields: dict[str, Any]) -> StreamedSpan | None:
    """Rebuild a StreamedSpan from one stream entry, or None if it is garbage.

    Returning None rather than raising is deliberate. A single malformed entry
    must not be able to wedge the consumer group: the caller acks poison and
    counts it, so a bad producer costs one span and a metric, not the pipeline.
    """
    try:
        payload = fields["payload"]
        enqueued_at_ms = int(fields.get("enqueued_at_ms", 0))
        span = Span.model_validate_json(payload)
    except Exception:
        log.warning("poison entry discarded", extra={"fields": {"entry_id": entry_id}})
        return None
    return StreamedSpan(entry_id=entry_id, enqueued_at_ms=enqueued_at_ms, span=span)


async def process_batch(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    entries: list[tuple[str, dict[str, Any]]],
    *,
    now_ms: int,
) -> BatchOutcome:
    """Persist one batch and ack it. Safe to replay in full."""
    outcome = BatchOutcome(entries=len(entries))
    if not entries:
        return outcome

    decoded: list[StreamedSpan] = []
    for entry_id, fields in entries:
        streamed = decode_entry(entry_id, fields)
        if streamed is None:
            outcome.poison += 1
        else:
            decoded.append(streamed)

    spans: list[Span] = [d.span for d in decoded]
    outcome.spans = len(spans)

    if spans:
        result = await write_spans(pool, spans)
        outcome.inserted = result.inserted
        outcome.duplicates = result.duplicates

        # Only spans the database actually accepted contribute to trace state and
        # to the latency histograms. Both accumulate with HINCRBY, so replaying a
        # reclaimed batch through them would inflate the trace's cost and token
        # totals and add phantom observations to the percentile buckets — an
        # idempotent INSERT alone does not make the derived aggregates idempotent.
        fresh = [s for s in spans if str(s.span_id) in result.inserted_ids]

        if fresh:
            pipe = redis.pipeline(transaction=False)
            stage_trace_state(pipe, fresh, now_ms, settings)
            stage_latency(pipe, fresh, settings)
            stage_stats(
                pipe,
                spans_processed=result.inserted,
                spans_duplicate=result.duplicates,
                spans_poison=outcome.poison,
            )
            await pipe.execute()
        else:
            await bump_stats(
                redis,
                spans_duplicate=result.duplicates,
                spans_poison=outcome.poison,
            )
    elif outcome.poison:
        await bump_stats(redis, spans_poison=outcome.poison)

    # Ack everything, poison included — an entry we will never be able to parse
    # must leave the pending list or the reclaimer picks it up forever.
    await redis.xack(
        redis_keys.STREAM_SPANS,
        settings.worker_consumer_group,
        *[entry_id for entry_id, _ in entries],
    )
    return outcome


async def run_consumer(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    stop: asyncio.Event,
    consumer_name: str,
) -> None:
    """Drain new entries until told to stop."""
    import time

    group = settings.worker_consumer_group
    while not stop.is_set():
        try:
            response = await redis.xreadgroup(
                groupname=group,
                consumername=consumer_name,
                streams={redis_keys.STREAM_SPANS: ">"},
                count=settings.worker_batch_size,
                block=settings.worker_block_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("xreadgroup failed")
            if await sleep_or_stop(stop, 1.0):
                break
            continue

        if not response:
            continue  # block timeout expired with an idle stream

        for _stream, entries in response:
            try:
                outcome = await process_batch(
                    redis, pool, settings, entries, now_ms=int(time.time() * 1000)
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Deliberately no ack. The entries stay pending and either this
                # worker or the reclaimer retries them; dropping them here to
                # keep the loop tidy would be silent data loss.
                log.exception("batch failed, leaving entries pending")
                if await sleep_or_stop(stop, 1.0):
                    return
                continue

            if outcome.spans:
                log.debug(
                    "batch processed",
                    extra={
                        "fields": {
                            "consumer": consumer_name,
                            "spans": outcome.spans,
                            "inserted": outcome.inserted,
                            "duplicates": outcome.duplicates,
                        }
                    },
                )


async def run_reclaimer(
    redis: Redis,
    pool: asyncpg.Pool,
    settings: Settings,
    stop: asyncio.Event,
    consumer_name: str,
) -> None:
    """Re-drive entries stranded in a dead consumer's pending list (§7.4).

    Without this the durability story is a claim rather than a fact: a worker
    that dies mid-batch leaves its entries in the PEL, delivered-but-unacked, and
    nothing ever looks at them again. Those spans are invisible forever.
    """
    import time

    group = settings.worker_consumer_group
    cursor = PEL_CURSOR_START
    interval_s = settings.worker_reclaim_interval_ms / 1000.0

    while not stop.is_set():
        if await sleep_or_stop(stop, interval_s):
            break
        try:
            claimed = await redis.xautoclaim(
                name=redis_keys.STREAM_SPANS,
                groupname=group,
                consumername=consumer_name,
                min_idle_time=settings.worker_reclaim_idle_ms,
                start_id=cursor,
                count=settings.worker_batch_size,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("xautoclaim failed")
            cursor = PEL_CURSOR_START
            continue

        # redis-py returns (next_cursor, entries) or (next_cursor, entries,
        # deleted) depending on server version; only the first two matter here.
        next_cursor = claimed[0] if claimed else PEL_CURSOR_START
        entries = list(claimed[1]) if len(claimed) > 1 else []
        cursor = next_cursor or PEL_CURSOR_START

        if not entries:
            continue

        log.info(
            "reclaimed stranded entries",
            extra={"fields": {"count": len(entries), "consumer": consumer_name}},
        )
        try:
            await process_batch(
                redis, pool, settings, entries, now_ms=int(time.time() * 1000)
            )
            await bump_stats(redis, entries_reclaimed=len(entries))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("reclaimed batch failed, leaving entries pending")
