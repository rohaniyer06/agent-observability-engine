"""Bulk span persistence.

Idempotent by construction. At-least-once delivery is the point of the consumer
group, and it means spans WILL be re-presented — an XAUTOCLAIM reclaim, or a
worker that died between the INSERT and the XACK, both replay work that already
landed. So every insert is `ON CONFLICT (span_id) DO NOTHING`, and the suppressed
rows are counted rather than swallowed: redelivery should be observable at
/v1/system/stats, not invisible (DEVIATIONS.md #3).

One statement per batch, not one per span. The arrays are unnested server-side so
a 500-span batch is a single round trip with a fixed 16 parameters.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from aoe.pricing import get_pricing
from aoe.schema import Span

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

_INSERT_SPANS = """
INSERT INTO spans (
    span_id, trace_id, parent_span_id, node_name, operation_name, model_name,
    provider_name, start_time_ns, end_time_ns, duration_us, input_tokens,
    output_tokens, cost_usd, status, error_message, attributes
)
SELECT span_id, trace_id, parent_span_id, node_name, operation_name, model_name,
       provider_name, start_time_ns, end_time_ns, duration_us, input_tokens,
       output_tokens, cost_usd, status, error_message, attributes::jsonb
FROM unnest(
    $1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::text[],
    $7::text[], $8::bigint[], $9::bigint[], $10::bigint[], $11::int[],
    $12::int[], $13::numeric[], $14::text[], $15::text[], $16::text[]
) AS s(
    span_id, trace_id, parent_span_id, node_name, operation_name, model_name,
    provider_name, start_time_ns, end_time_ns, duration_us, input_tokens,
    output_tokens, cost_usd, status, error_message, attributes
)
ON CONFLICT (span_id) DO NOTHING
RETURNING span_id
"""


@dataclass(frozen=True)
class WriteResult:
    inserted: int
    duplicates: int
    # span_ids the database actually accepted. The caller needs this, not just
    # the count: trace-state accumulation downstream is HINCRBY-based, so
    # replaying a batch would double-count cost and tokens into the trace even
    # though the span rows themselves are idempotent. Filtering to newly-stored
    # spans is what makes the *aggregates* idempotent as well.
    inserted_ids: frozenset[str] = frozenset()


def to_numeric(value: float) -> Decimal:
    """Float -> NUMERIC(12,8).

    asyncpg will not encode a float into a numeric column, and quantizing here
    rather than letting Postgres round means the value stored on the span and the
    value summed into the trace total are the same number.
    """
    return Decimal(f"{float(value):.8f}")


def resolve_cost(span: Span) -> float:
    """Cost for one span, from the emitter if it supplied one, else from pricing.

    Emitters are expected to leave `cost_usd` unset so there is exactly one
    costing implementation in the system (design doc §7.7) — but the field exists
    on the wire schema, so honour it when it is populated.
    """
    if span.cost_usd is not None:
        return float(span.cost_usd)
    return get_pricing().cost_usd(
        span.model_name,
        span.input_tokens,
        span.output_tokens,
        span.cache_read_tokens,
        span.cache_write_tokens,
    )


async def write_spans(pool: asyncpg.Pool, spans: list[Span]) -> WriteResult:
    """Insert a batch, ignoring spans already stored. Duplicates are counted."""
    if not spans:
        return WriteResult(0, 0, frozenset())

    # Collapse in-batch repeats first. ON CONFLICT DO NOTHING tolerates them, but
    # doing it here keeps the duplicate counter honest about how many rows the
    # database actually suppressed.
    unique: dict[str, Span] = {}
    for span in spans:
        unique.setdefault(str(span.span_id), span)
    batch = list(unique.values())

    span_ids: list[str] = []
    trace_ids: list[str] = []
    parent_ids: list[str | None] = []
    node_names: list[str] = []
    operations: list[str] = []
    models: list[str | None] = []
    providers: list[str | None] = []
    starts: list[int] = []
    ends: list[int] = []
    durations: list[int] = []
    input_tokens: list[int] = []
    output_tokens: list[int] = []
    costs: list[Decimal] = []
    statuses: list[str] = []
    errors: list[str | None] = []
    attributes: list[str] = []

    for span in batch:
        span_ids.append(str(span.span_id))
        trace_ids.append(str(span.trace_id))
        parent_ids.append(str(span.parent_span_id) if span.parent_span_id else None)
        node_names.append(span.node_name)
        operations.append(span.operation_name)
        models.append(span.model_name)
        providers.append(span.provider_name)
        starts.append(span.start_time_ns)
        ends.append(span.end_time_ns)
        durations.append(span.duration_us)
        input_tokens.append(span.input_tokens)
        output_tokens.append(span.output_tokens)
        costs.append(to_numeric(resolve_cost(span)))
        statuses.append(span.status)
        errors.append(span.error_message)
        attributes.append(json.dumps(span.attributes))

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _INSERT_SPANS,
            span_ids,
            trace_ids,
            parent_ids,
            node_names,
            operations,
            models,
            providers,
            starts,
            ends,
            durations,
            input_tokens,
            output_tokens,
            costs,
            statuses,
            errors,
            attributes,
        )

    inserted_ids = frozenset(str(r["span_id"]) for r in rows)
    inserted = len(inserted_ids)
    return WriteResult(
        inserted=inserted,
        duplicates=len(spans) - inserted,
        inserted_ids=inserted_ids,
    )
