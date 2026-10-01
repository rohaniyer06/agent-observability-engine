"""`/v1/anomalies` and `/v1/drift*` — the "smart layer" read side."""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import APIRouter, Query

from aoe.api import queries
from aoe.api.pagination import (
    clamp_limit,
    decode_anomaly_cursor,
    encode_anomaly_cursor,
    require_one_of,
    window_start,
)
from aoe.apimodels import AnomalyPage, DriftResponse, FlowEdge, FlowResponse
from aoe.config import get_settings
from aoe.db.pool import get_pool

router = APIRouter(tags=["anomalies"])

SEVERITIES = {"warn", "critical"}

# Synthetic source so the Sankey has a single root. Without it the first node of
# every path is a source with no inflow and the diagram renders as N disconnected
# stubs instead of one funnel. Double-underscored so it cannot collide with a
# real node name.
START_NODE = "__start__"


@router.get("/v1/anomalies", response_model=AnomalyPage)
async def list_anomalies(
    limit: int = Query(default=0, ge=0, le=10_000),
    cursor: str | None = None,
    severity: str | None = None,
    window: str | None = None,
) -> AnomalyPage:
    settings = get_settings()
    page_size = clamp_limit(limit or settings.default_page_size, settings.max_page_size)

    # Validate before touching the pool: a bad param costs a 400, not a
    # connection checkout.
    keyset = decode_anomaly_cursor(cursor) if cursor else None
    since = window_start(window) if window else None
    severity = require_one_of("severity", severity, SEVERITIES)

    pool = await get_pool()
    rows = await queries.fetch_anomalies(
        pool,
        limit=page_size + 1,
        severity=severity,
        since=since,
        cursor=keyset,
    )

    has_more = len(rows) > page_size
    items = rows[:page_size]
    next_cursor = (
        encode_anomaly_cursor(items[-1].detected_at, items[-1].id) if has_more and items else None
    )
    return AnomalyPage(items=items, next_cursor=next_cursor, has_more=has_more)


@router.get("/v1/drift", response_model=DriftResponse)
async def drift(
    pipeline: str | None = None,
    limit: int = Query(default=0, ge=0, le=10_000),
) -> DriftResponse:
    settings = get_settings()
    # Single-pipeline system by default; the param exists so it does not have to
    # stay that way.
    name = pipeline or settings.pipeline_name
    page_size = clamp_limit(limit or settings.default_page_size, settings.max_page_size)

    pool = await get_pool()
    async with pool.acquire() as conn:
        # `share` and `is_baseline` are derived in queries.fetch_path_stats so
        # the drift list and the flow diagram cannot disagree about which paths
        # are normal (DEVIATIONS.md #5).
        paths, total = await queries.fetch_path_stats(conn, pipeline=name, limit=page_size)
        events = await queries.fetch_drift_events(conn, pipeline=name, limit=page_size)

    return DriftResponse(pipeline_name=name, total_traces=total, paths=paths, events=events)


def _path_edges(path: list[str]) -> Iterator[tuple[str, str]]:
    """Consecutive node pairs, rooted at the synthetic start.

    A repeated node (the harness's deliberate `escalate` retry loop, design doc
    §5.1) legitimately produces a self-edge. It is emitted as-is: it is real
    behaviour, and collapsing it would hide the retry the drift view exists to
    surface.
    """
    if not path:
        return
    yield START_NODE, path[0]
    for i in range(len(path) - 1):
        yield path[i], path[i + 1]


@router.get("/v1/drift/flow", response_model=FlowResponse)
async def drift_flow(pipeline: str | None = None, window: str = "24h") -> FlowResponse:
    settings = get_settings()
    name = pipeline or settings.pipeline_name
    since = window_start(window)

    pool = await get_pool()
    async with pool.acquire() as conn:
        observed = await queries.fetch_observed_paths(conn, pipeline=name, since=since)
        # No limit: an edge is baseline if it appears in ANY baseline path, so a
        # truncated path list would mislabel edges. Baseline paths are frequent
        # by definition, so this set is small.
        all_paths, _ = await queries.fetch_path_stats(conn, pipeline=name, limit=None)

    baseline_edges = {
        edge for stat in all_paths if stat.is_baseline for edge in _path_edges(stat.path)
    }

    counts: dict[tuple[str, str], int] = {}
    nodes: list[str] = [START_NODE]
    seen: set[str] = {START_NODE}
    # `observed` is ordered by occurrences DESC, so nodes are collected
    # dominant-path-first and the diagram's column order reads as the happy path.
    for path, occurrences in observed:
        for edge in _path_edges(path):
            counts[edge] = counts.get(edge, 0) + occurrences
        for node in path:
            if node not in seen:
                seen.add(node)
                nodes.append(node)

    edges = [
        FlowEdge(source=src, target=dst, value=value, is_baseline=(src, dst) in baseline_edges)
        for (src, dst), value in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return FlowResponse(pipeline_name=name, nodes=nodes, edges=edges)
