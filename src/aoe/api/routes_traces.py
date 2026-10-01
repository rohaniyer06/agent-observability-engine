"""`/v1/traces` — the history list and the single-trace drill-down."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query

from aoe.api import queries
from aoe.api.pagination import (
    clamp_limit,
    decode_trace_cursor,
    encode_trace_cursor,
    require_one_of,
    window_start,
)
from aoe.apimodels import TraceDetail, TracePage
from aoe.config import get_settings
from aoe.db.pool import get_pool

router = APIRouter(tags=["traces"])

TRACE_STATUSES = {"ok", "error", "partial"}


@router.get("/v1/traces", response_model=TracePage)
async def list_traces(
    limit: int = Query(default=0, ge=1, le=10_000),
    cursor: str | None = None,
    pipeline: str | None = None,
    status: str | None = None,
    window: str | None = None,
    node: str | None = None,
) -> TracePage:
    settings = get_settings()
    # `limit=0` stands in for "not supplied" so the default lives in config
    # rather than being duplicated in the signature.
    page_size = clamp_limit(limit or settings.default_page_size, settings.max_page_size)

    # Validate everything before acquiring a connection: a bad param should cost
    # a 400 and no database work.
    keyset = decode_trace_cursor(cursor) if cursor else None
    since = window_start(window) if window else None
    status = require_one_of("status", status, TRACE_STATUSES)

    pool = await get_pool()
    # One extra row is the has_more probe: a second COUNT(*) over the same
    # predicate would double the work to answer a boolean.
    rows = await queries.fetch_traces(
        pool,
        limit=page_size + 1,
        pipeline=pipeline,
        status=status,
        since=since,
        node=node,
        cursor=keyset,
    )

    has_more = len(rows) > page_size
    items = rows[:page_size]
    next_cursor = (
        encode_trace_cursor(items[-1].started_at, items[-1].trace_id)
        if has_more and items
        else None
    )
    return TracePage(items=items, next_cursor=next_cursor, has_more=has_more)


@router.get("/v1/traces/{trace_id}", response_model=TraceDetail)
async def get_trace(trace_id: str) -> TraceDetail:
    try:
        tid = uuid.UUID(trace_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="trace_id must be a UUID") from exc

    pool = await get_pool()
    async with pool.acquire() as conn:
        trace = await queries.fetch_trace(conn, tid)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace not found")
        # The one place raw `spans` is read. Bounded to a single trace.
        spans = await queries.fetch_spans(conn, tid)
    return TraceDetail(trace=trace, spans=spans)
